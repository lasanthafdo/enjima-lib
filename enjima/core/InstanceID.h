//
// Created by m34ferna on 09/01/24.
//

#ifndef ENJIMA_OPERATOR_ID_H
#define ENJIMA_OPERATOR_ID_H

#include <concepts>
#include <cstdint>
#include <functional>

namespace enjima::core {

    template<typename T>
        requires std::unsigned_integral<T>
    class InstanceID {
    public:
        explicit InstanceID(T instanceID);
        [[nodiscard]] T GetId() const;
        bool operator==(const InstanceID<T>& other) const;

    private:
        T instanceID_;
    };

}// namespace enjima::core

template class enjima::core::InstanceID<uint64_t>;

#include "InstanceID.tpp"

#endif// ENJIMA_OPERATOR_ID_H
