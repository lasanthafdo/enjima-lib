//
// Created by m34ferna on 16/02/24.
//

#ifndef ENJIMA_ILLEGAL_STATE_EXCEPTION_H
#define ENJIMA_ILLEGAL_STATE_EXCEPTION_H

#include <exception>
#include <string>

namespace enjima::runtime {
    class IllegalStateException : public std::exception {
    public:
        IllegalStateException() = default;
        explicit IllegalStateException(std::string message) : message_(std::move(message)) {}

        [[nodiscard]] const char* what() const noexcept override
        {
            return message_.c_str();
        }

    private:
        const std::string message_ = "Illegal state exception!";
    };
}// namespace enjima::runtime

#endif//ENJIMA_ILLEGAL_STATE_EXCEPTION_H
