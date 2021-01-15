#include "mongo/platform/basic.h"

#include "mongo/db/pipeline/accumulator_custom.h"

#include "mongo/db/exec/document_value/value.h"

namespace mongo {


class BucketizerImpl
{
public:
    BucketizerImpl (Value const& staticArgs)
    {
        //std::cout << "Custom new group created with static = " << staticArgs << "\n";

        uassert(40400,
            str::stream() << "$bucketizer requires object staticArgs, but " << staticArgs.toString()
                          << " is of type " << typeName(staticArgs.getType()),
            (staticArgs.getType() == BSONType::Object));

        auto doc = staticArgs.getDocument();
        auto numbuckets = doc["buckets"];
        auto step = doc["step"];

        uassert(40400,
            str::stream() << "$bucketizer requires integral buckets parameter",
            (numbuckets.integral()));


        uassert(40400,
            str::stream() << "$bucketizer requires numeric parameter step",
            (step.numeric()));

        _numbuckets = numbuckets.coerceToInt();

        uassert(40400,
                str::stream() << "$bucketizer requires number of buckets >=2",
                    (_numbuckets >= 2));

        _step = step.coerceToDouble();

        //std::cout << "buckets: " << _numbuckets << " step: " << _step << "\n";

        resetAccumulator();

        // Calculate our _fixed_ memory footprint once and for all
        _mem_usage = sizeof(*this) + external_footprint();
    }

    static char const * getOpName()
    {
        return "$bucketizer";
    }

    Value getValue(bool toBeMerged)
    {
        return Value(_buckets);
    }

    int accumulate(const Value& input, bool merging)
    {
        // We don't increase memory footprint while accumulating
        int const additional_memory_used = 0;

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

            //std::cout << "Merging\n";
        }

        return additional_memory_used;
    }

    void startNewGroup(Value const& input)
    {
        // Nothing to do
    }

    void resetAccumulator()
    {
        _buckets.assign(_numbuckets, Value(0));
    }

    int memUsageBytes() const
    {
        // Just use the cached value since this accumulator
        // has a fixed footprint
        return _mem_usage;
    }
private:
    int external_footprint() const
    {
        using wrapped_type_t = int;  // The actual type wrapped inside Value
        return _numbuckets * Value(wrapped_type_t{}).getApproximateSize();
    }

    std::vector<Value> _buckets;
    double _step;
    int _numbuckets;
    int _mem_usage;
};


REGISTER_CUSTOM_ACCUMULATOR(bucketizer, BucketizerImpl);

}

