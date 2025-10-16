//
// Created by m34ferna on 02/03/24.
//

#ifndef ENJIMA_CO_GROUP_FUNCTION_H
#define ENJIMA_CO_GROUP_FUNCTION_H

#include <type_traits>
#include <vector>
#include <queue>

namespace enjima::api {

    template<typename TLeft, typename TRight, typename TOutput>
    class CoGroupFunction {
    public:
        virtual void operator()(const std::vector<core::Record<TLeft>>& leftInputEvents,
                const std::vector<core::Record<TRight>>& rightInputEvents, std::queue<TOutput>& outputEvents) = 0;
    };

    template<typename T, typename U, typename V, typename X>
    concept CoGroupFuncT = std::is_base_of_v<enjima::api::CoGroupFunction<U, V, X>, T>;
}// namespace enjima::api


#endif//ENJIMA_CO_GROUP_FUNCTION_H
