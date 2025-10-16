//
// Created by m34ferna on 02/03/24.
//

#ifndef ENJIMA_JOIN_PREDICATE_H
#define ENJIMA_JOIN_PREDICATE_H

#include <type_traits>

namespace enjima::api {

    template<typename TLeft, typename TRight>
    class JoinPredicate {
    public:
        virtual bool operator()(const TLeft& leftInputEvent, const TRight& rightInputEvent) = 0;
    };

    template<typename T, typename U, typename V>
    concept JoinPredT = std::is_base_of_v<enjima::api::JoinPredicate<U, V>, T>;

}// namespace enjima::api


#endif//ENJIMA_JOIN_PREDICATE_H
