//
// Created by m34ferna on 16/02/24.
//

#ifndef ENJIMA_JOB_EXCEPTION_H
#define ENJIMA_JOB_EXCEPTION_H

#include <exception>
#include <string>

namespace enjima::runtime {
    class JobValidationException : public std::exception {
    public:
        JobValidationException() = default;
        explicit JobValidationException(std::string message) : message_(std::move(message)) {}

        [[nodiscard]] const char* what() const noexcept override
        {
            return message_.c_str();
        }

    private:
        const std::string message_ = "Unknown streaming job exception!";
    };
}// namespace enjima::runtime

#endif//ENJIMA_JOB_EXCEPTION_H
