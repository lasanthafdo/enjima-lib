//
// Created by m34ferna on 09/01/24.
//

#ifndef ENJIMA_COUNTER_H
#define ENJIMA_COUNTER_H

#include <atomic>
#include <concepts>

namespace enjima::metrics {

    template<std::unsigned_integral T>
    class Counter {
    public:
        T GetCount() const;
        void Inc();
        void Inc(T delta);
        const Counter<T> operator++(int);
        Counter<T>& operator++();

    private:
        T count_{0};
    };

    template<>
    class Counter<uint64_t> {
    public:
        [[nodiscard]] uint64_t GetCount() const;
        void Inc();
        void Inc(uint64_t delta);
        void IncRelaxed();
        void IncRelaxed(uint64_t delta);

    private:
        std::atomic<uint64_t> count_{0};
    };
}// namespace enjima::metrics

#include "Counter.tpp"

#endif// ENJIMA_COUNTER_H
