//
// Created by m34ferna on 16/02/24.
//

#ifndef ENJIMA_CONFIGURATION_EXCEPTION_H
#define ENJIMA_CONFIGURATION_EXCEPTION_H

#include <exception>
#include <string>

namespace enjima::runtime {
    class ConfigurationException : public std::exception {
    public:
        ConfigurationException() = default;
        explicit ConfigurationException(std::string message) : message_(std::move(message)) {}

        [[nodiscard]] const char* what() const noexcept override
        {
            return message_.c_str();
        }

    private:
        const std::string message_ = "Configuration exception!";
    };
}// namespace enjima::runtime

#endif//ENJIMA_CONFIGURATION_EXCEPTION_H
