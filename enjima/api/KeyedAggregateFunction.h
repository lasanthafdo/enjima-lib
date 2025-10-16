//
// Created by m34ferna on 02/03/24.
//

#ifndef ENJIMA_KEYED_AGGREGATE_FUNCTION_H
#define ENJIMA_KEYED_AGGREGATE_FUNCTION_H

#include "AggregateFunction.h"

namespace enjima::api {

    template<typename TInput, typename TOutput>
    class KeyedAggregateFunction : public AggregateFunction<TInput, TOutput> {
    public:
        virtual std::vector<TOutput>& GetResult() = 0;
        virtual ~KeyedAggregateFunction() = default;
    };

}// namespace enjima::api


#endif//ENJIMA_KEYED_AGGREGATE_FUNCTION_H
