//
// Created by m34ferna on 02/03/24.
//

#ifndef ENJIMA_MERGEABLE_KEYED_AGGREGATE_FUNCTION_H
#define ENJIMA_MERGEABLE_KEYED_AGGREGATE_FUNCTION_H

#include "KeyedAggregateFunction.h"

#include <vector>

namespace enjima::api {

    template<typename TInput, typename TOutput>
    class MergeableKeyedAggregateFunction : public KeyedAggregateFunction<TInput, TOutput> {
    public:
        void Reset() {}
        void InitializeActivePane(MergeableKeyedAggregateFunction<TInput, TOutput>& activePaneFunctor) {}
        void Merge(const std::vector<MergeableKeyedAggregateFunction<TInput, TOutput>>& vecToMerge) {}
    };

    template<typename T, typename TInput, typename TOutput>
    concept MergeableKeyedAggFuncT = std::is_same_v<MergeableKeyedAggregateFunction<TInput, TOutput>, T>;
}// namespace enjima::api


#endif//ENJIMA_MERGEABLE_KEYED_AGGREGATE_FUNCTION_H
