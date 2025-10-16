//
// Created by m34ferna on 02/03/24.
//

#ifndef ENJIMA_NON_KEYED_AGGREGATE_FUNCTION_H
#define ENJIMA_NON_KEYED_AGGREGATE_FUNCTION_H

#include "AggregateFunction.h"
namespace enjima::api {

    template<typename TInput, typename TOutput>
    class NonKeyedAggregateFunction : public AggregateFunction<TInput, TOutput> {
    public:
        virtual TOutput GetResult() = 0;
    };

}// namespace enjima::api

#endif//ENJIMA_NON_KEYED_AGGREGATE_FUNCTION_H