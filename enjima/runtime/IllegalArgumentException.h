//
// Created by m34ferna on 16/02/24.
//

#ifndef ENJIMA_ILLEGAL_ARGUMENT_EXCEPTION_H
#define ENJIMA_ILLEGAL_ARGUMENT_EXCEPTION_H

#include <exception>
#include <string>

namespace enjima::runtime {
    class IllegalArgumentException : public std::exception {
    public:
        IllegalArgumentException() = default;
        explicit IllegalArgumentException(std::string message) : message_(std::move(message)) {}

        [[nodiscard]] const char* what() const noexcept override
        {
            return message_.c_str();
        }

    private:
        const std::string message_ = "Illegal argument exception!";
    };
}// namespace enjima::runtime

#endif//ENJIMA_ILLEGAL_ARGUMENT_EXCEPTION_H
