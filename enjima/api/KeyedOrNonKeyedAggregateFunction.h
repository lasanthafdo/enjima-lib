//
// Created by m34ferna on 10/10/24.
//

#ifndef ENJIMA_KEYED_OR_NON_KEYED_AGGREGATE_FUNCTION_H
#define ENJIMA_KEYED_OR_NON_KEYED_AGGREGATE_FUNCTION_H

#include "enjima/api/KeyedAggregateFunction.h"
#include "enjima/api/NonKeyedAggregateFunction.h"

#include <type_traits>

namespace enjima::api {
    template<typename T, typename U, typename V, bool keyedAggregateFn>
    concept AggFuncT = (std::is_base_of_v<enjima::api::KeyedAggregateFunction<U, V>, T> && keyedAggregateFn) ||
                       (std::is_base_of_v<enjima::api::NonKeyedAggregateFunction<U, V>, T> && !keyedAggregateFn);

}

#endif//ENJIMA_KEYED_OR_NON_KEYED_AGGREGATE_FUNCTION_H
