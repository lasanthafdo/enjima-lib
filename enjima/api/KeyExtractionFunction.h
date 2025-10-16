//
// Created by m34ferna on 02/03/24.
//

#ifndef ENJIMA_KEY_EXTRACTION_FUNCTION_H
#define ENJIMA_KEY_EXTRACTION_FUNCTION_H

#include "JoinPredicate.h"
#include <type_traits>

namespace enjima::api {

    template<typename TInput, typename TKey>
    class KeyExtractionFunction {
    public:
        virtual TKey operator()(const TInput& inputEvent) = 0;
    };

    template<typename T, typename U, typename V>
    concept KeyExtractFuncT = std::is_base_of_v<enjima::api::KeyExtractionFunction<U, V>, T>;
}// namespace enjima::api


#endif//ENJIMA_KEY_EXTRACTION_FUNCTION_H
