//
// Created by m34ferna on 18/05/24.
//

#ifndef ENJIMA_HISTOGRAM_VALUE_H
#define ENJIMA_HISTOGRAM_VALUE_H

#include <atomic>
#include <concepts>

namespace enjima::metrics {

    template<typename T>
        requires std::integral<T> || std::floating_point<T>
    class HistogramValue {
    public:
        HistogramValue() : value_() {}

        explicit HistogramValue(T val) : value_(val) {}

        HistogramValue(const HistogramValue& other) : value_(other.value_.load(std::memory_order::acquire)) {}

        HistogramValue& operator=(const HistogramValue& other)
        {
            value_.store(other.value_.load(std::memory_order::acquire), std::memory_order::release);
            return *this;
        }

        T GetVal()
        {
            return value_.load(std::memory_order::acquire);
        }

        void SetVal(T val)
        {
            value_.store(val, std::memory_order::release);
        }

    private:
        std::atomic<T> value_;
    };

}// namespace enjima::metrics

#endif//ENJIMA_HISTOGRAM_VALUE_H
