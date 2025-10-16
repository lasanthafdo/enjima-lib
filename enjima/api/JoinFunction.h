//
// Created by m34ferna on 02/03/24.
//

#ifndef ENJIMA_JOIN_FUNCTION_H
#define ENJIMA_JOIN_FUNCTION_H

#include <type_traits>

namespace enjima::api {

    template<typename TLeft, typename TRight, typename TOutput>
    class JoinFunction {
    public:
        virtual TOutput operator()(const TLeft& leftInputEvent, const TRight& rightInputEvent) = 0;
    };

    template<typename T, typename U, typename V, typename X>
    concept JoinFuncT = std::is_base_of_v<enjima::api::JoinFunction<U, V, X>, T>;
}// namespace enjima::api


#endif//ENJIMA_JOIN_FUNCTION_H
