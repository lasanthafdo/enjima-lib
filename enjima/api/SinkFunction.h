//
// Created by m34ferna on 02/02/24.
//

#ifndef ENJIMA_USER_FUNCTION_H
#define ENJIMA_USER_FUNCTION_H

namespace enjima::api {

    template<typename TInput>
    class SinkFunction {
    public:
        virtual void Execute(uint64_t timestamp, TInput inputEvent) = 0;
    };

}// namespace enjima::api


#endif//ENJIMA_USER_FUNCTION_H
