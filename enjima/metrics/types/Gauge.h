//
// Created by m34ferna on 09/01/24.
//

#ifndef ENJIMA_GAUGE_H
#define ENJIMA_GAUGE_H

#include <type_traits>

namespace enjima::metrics {

    template<typename T>
        requires std::is_arithmetic_v<T>
    class Gauge {
    public:
        explicit Gauge(T initialVal);
        virtual T GetVal();
        virtual void UpdateVal(T val);
        virtual ~Gauge() = default;

    private:
        T val_;
    };
}// namespace enjima::metrics

#include "Gauge.tpp"

#endif// ENJIMA_GAUGE_H
