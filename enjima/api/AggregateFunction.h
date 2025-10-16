//
// Created by m34ferna on 02/03/24.
//

#ifndef ENJIMA_AGGREGATE_FUNCTION_H
#define ENJIMA_AGGREGATE_FUNCTION_H

namespace enjima::api {

    template<typename TInput, typename TOutput>
    class AggregateFunction {
    public:
        virtual void operator()(const TInput& inputEvent) = 0;
    };

}// namespace enjima::api


#endif//ENJIMA_AGGREGATE_FUNCTION_H
