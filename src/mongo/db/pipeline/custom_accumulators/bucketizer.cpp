#include "mongo/platform/basic.h"

#include "mongo/db/pipeline/accumulator_custom.h"

#include "mongo/db/exec/document_value/value.h"

namespace mongo {


class BucketizerImpl
{
public:
    BucketizerImpl (Value const& staticArgs)
    {
        std::cout << "Custom new group created with static = " << staticArgs << "\n";

        uassert(40400,
            str::stream() << "$customsum requires object staticArgs, but " << staticArgs.toString()
                          << " is of type " << typeName(staticArgs.getType()),
            (staticArgs.getType() == BSONType::Object));

        auto doc = staticArgs.getDocument();
        auto numbuckets = doc["buckets"];
        auto step = doc["step"];

        uassert(40400,
            str::stream() << "$customsum requires integral buckets parameter",
            (numbuckets.integral()));


        uassert(40400,
            str::stream() << "$customsum requires numeric parameter step",
            (step.numeric()));

        _numbuckets = numbuckets.coerceToInt();

        uassert(40400,
                str::stream() << "$customsum number of buckets must be >=2",
                    (_numbuckets >= 2));

        _step = step.coerceToDouble();

        std::cout << "buckets: " << _numbuckets << " step: " << _step << "\n";

        resetAccumulator();
    }
    static const char * getOpName()
    {
        return "$customsum";
    }
    Value getValue(bool toBeMerged)
    {
        return Value(_buckets);
    }
    void accumulate(const Value& input, bool merging)
    {
        if (!merging) {
            double val = input.coerceToDouble();
            int bin = val / _step;
            if (bin < 0)
                bin = 0;
            if (bin >= _numbuckets)
                bin = _numbuckets - 1;
            _buckets[bin] = Value(_buckets[bin].coerceToInt() + 1);
        }
        else {
            invariant(input.getType() == Array);
            const std::vector<Value>& vec = input.getArray();

            invariant(vec.size() == _buckets.size());

            unsigned int i = 0;
            for (auto&& val : vec) {
                _buckets[i] = Value(_buckets[i].coerceToInt() + val.coerceToInt());
                i++;
            }

            std::cout << "Merging\n";

        }
    }
    void startNewGroup(Value const& input)
    {
    }
    void resetAccumulator()
    {
        _buckets.assign(_numbuckets, Value(0));
    }

private:
    double _step;
    int _numbuckets;
    std::vector<Value> _buckets;
};


REGISTER_CUSTOM_ACCUMULATOR(bucketizer, BucketizerImpl);

}

