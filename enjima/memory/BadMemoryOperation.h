//
// Created by m34ferna on 15/01/24.
//

#ifndef ENJIMA_BAD_MEMORY_INITIALIZATION_H
#define ENJIMA_BAD_MEMORY_INITIALIZATION_H

#include <exception>
#include <string>


namespace enjima::memory {

    class BadMemoryOperation : std::exception {
    public:
        BadMemoryOperation() = default;
        explicit BadMemoryOperation(std::string message) : message_(std::move(message)) {}

        [[nodiscard]] const char* what() const noexcept override
        {
            return message_.c_str();
        }

    private:
        const std::string message_ = "Bad memory operation exception!";
    };

}// namespace enjima::memory


#endif//ENJIMA_BAD_MEMORY_INITIALIZATION_H
