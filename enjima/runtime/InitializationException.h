//
// Created by m34ferna on 16/02/24.
//

#ifndef ENJIMA_INITIALIZATION_EXCEPTION_H
#define ENJIMA_INITIALIZATION_EXCEPTION_H

#include <exception>
#include <string>

namespace enjima::runtime {
    class InitializationException : public std::exception {
    public:
        InitializationException() = default;
        explicit InitializationException(std::string message) : message_(std::move(message)) {}

        [[nodiscard]] const char* what() const noexcept override
        {
            return message_.c_str();
        }

    private:
        const std::string message_ = "Initialization exception!";
    };
}// namespace enjima::runtime

#endif//ENJIMA_INITIALIZATION_EXCEPTION_H
