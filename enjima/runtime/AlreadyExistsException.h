//
// Created by m34ferna on 19/02/24.
//

#ifndef ENJIMA_ALREADY_EXISTS_EXCEPTION_H
#define ENJIMA_ALREADY_EXISTS_EXCEPTION_H

#include <exception>
#include <string>

namespace enjima::runtime {
    class AlreadyExistsException : public std::exception {
    public:
        explicit AlreadyExistsException(std::string message) : message_(std::move(message)) {}

        [[nodiscard]] const char* what() const noexcept override
        {
            return message_.c_str();
        }

    private:
        const std::string message_ = "Illegal state exception!";
    };
}// namespace enjima::runtime

#endif//ENJIMA_ALREADY_EXISTS_EXCEPTION_H
