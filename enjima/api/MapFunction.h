//
// Created by m34ferna on 02/03/24.
//

#ifndef ENJIMA_MAP_FUNCTION_H
#define ENJIMA_MAP_FUNCTION_H

namespace enjima::api {

    template<typename TInput, typename TOutput>
    class MapFunction {
    public:
        virtual TOutput operator()(const TInput& inputEvent) = 0;
    };

    template<typename T, typename U, typename V>
    concept MapFuncT = std::is_base_of_v<enjima::api::MapFunction<U, V>, T>;
}// namespace enjima::api


#endif//ENJIMA_MAP_FUNCTION_H
