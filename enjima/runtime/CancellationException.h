//
// Created by m34ferna on 16/02/24.
//

#ifndef ENJIMA_CANCELLATION_EXCEPTION_H
#define ENJIMA_CANCELLATION_EXCEPTION_H

#include <exception>
#include <string>

namespace enjima::runtime {
    class CancellationException : public std::exception {
    public:
        CancellationException() = default;
        explicit CancellationException(std::string message) : message_(std::move(message)) {}

        [[nodiscard]] const char* what() const noexcept override
        {
            return message_.c_str();
        }

    private:
        const std::string message_ = "Cancellation exception!";
    };
}// namespace enjima::runtime

#endif//ENJIMA_CANCELLATION_EXCEPTION_H
