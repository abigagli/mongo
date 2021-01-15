#include "mongo/platform/basic.h"

#include "mongo/db/pipeline/accumulator.h"

#include "mongo/db/exec/document_value/value.h"
#include "mongo/db/pipeline/accumulation_statement.h"

namespace mongo {

#define REGISTER_CUSTOM_ACCUMULATOR(key, executor)                                            \
    MONGO_INITIALIZER(addToAccumulatorFactoryMap_##key)(InitializerContext*) {        \
        AccumulationStatement::registerAccumulator("$" #key, (AccumulatorCustom<executor>::parse), boost::none); \
        return Status::OK();                                                          \
    }

template <class EXECUTOR>
class AccumulatorCustom final : public AccumulatorState {
public:
    explicit AccumulatorCustom(const boost::intrusive_ptr<ExpressionContext>& expCtx, Value const& staticArgs)
        : AccumulatorState(expCtx)
        , _staticArgs(staticArgs)
    {
        _accumulatorExecutor.reset(new EXECUTOR (staticArgs));
    }
    void processInternal(const Value& input, bool merging) final
    {
        _accumulatorExecutor->accumulate (input, merging);
    }
    Value getValue(bool toBeMerged) final
    {
        return _accumulatorExecutor->getValue (toBeMerged);
    }
    const char* getOpName() const final
    {
        return EXECUTOR::getOpName();
    }
    void reset() final
    {
        _accumulatorExecutor->resetAccumulator();
    }
    void startNewGroup(Value const& input) 
    {
        _accumulatorExecutor->startNewGroup(input);
    }

    AccumulatorDocumentsNeeded documentsNeeded() const final {
        return AccumulatorDocumentsNeeded::kFirstDocument;
    }
    Document serialize(boost::intrusive_ptr<Expression> initializer,
                                  boost::intrusive_ptr<Expression> argument,
                                  bool explain) const 
    {
        MutableDocument args;
        args.addField("staticArgs", Value(_staticArgs));
        args.addField("initArgs", Value(initializer->serialize(explain)));
        args.addField("accumulateArgs", Value(argument->serialize(explain)));
        return DOC(getOpName() << args.freeze());
    }
    static AccumulationExpression parse(boost::intrusive_ptr<ExpressionContext> expCtx,
                                            BSONElement elem,
                                            VariablesParseState vps) {

        uassert(4544703,
            str::stream() << EXECUTOR::getOpName() << " expects an object as an argument; found: "
                            << typeName(elem.type()),
        elem.type() == BSONType::Object);
        BSONObj obj = elem.embeddedObject();

        boost::intrusive_ptr<Expression> initArgs, accumulateArgs;
        Value staticArgs;

        for (auto&& element : obj) {
            auto name = element.fieldNameStringData();
            if (name == "staticArgs") {
                staticArgs = parseExpression(expCtx, element, vps);
            } else if (name == "initArgs") {
                initArgs = Expression::parseOperand(expCtx, element, vps);
            } else if (name == "accumulateArgs") {
                accumulateArgs = Expression::parseOperand(expCtx, element, vps);
            } else {
                // unexpected field
                uassert(
                    4544706, str::stream() << EXECUTOR::getOpName() << " got an unexpected field: " << name, false);
            }
        }
        if (!initArgs) {
            // initArgs is optional because most custom accumulators don't need the state to depend on
            // the group key.
            initArgs = ExpressionConstant::create(expCtx, Value(0));
        }
        // accumulateArgs is required because it's the only way to communicate a value from the input
        // stream into the accumulator state.
        uassert(4544710, str::stream() << EXECUTOR::getOpName() << " missing required argument 'accumulateArgs'", accumulateArgs);

        auto factory = [expCtx = expCtx,
                        staticArgs = std::move(staticArgs)]() {

            return new AccumulatorCustom(expCtx, staticArgs);
        };
        return {std::move(initArgs), std::move(accumulateArgs), std::move(factory)};
    }
private:
    static Value parseExpression(boost::intrusive_ptr<ExpressionContext> expCtx,
                          BSONElement elem,
                          VariablesParseState vps) {
        boost::intrusive_ptr<Expression> expr = Expression::parseOperand(expCtx, elem, vps);
        expr = expr->optimize();
        ExpressionConstant* ec = dynamic_cast<ExpressionConstant*>(expr.get());
        uassert(4544701,
                str::stream() << EXECUTOR::getOpName() << "staticArgs must be a constant expression",
                ec);
        Value v = ec->getValue();
        return v;
    }
private:
    std::unique_ptr<EXECUTOR> _accumulatorExecutor;
    Value _staticArgs;

};


}