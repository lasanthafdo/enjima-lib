//
// Created by m34ferna on 04/04/24.
//

#ifndef ENJIMA_HISTOGRAM_H
#define ENJIMA_HISTOGRAM_H

#include "HistogramValue.h"

#include <algorithm>
#include <cmath>
#include <concepts>
#include <cstdint>
#include <numeric>
#include <vector>
#include <cassert>

namespace enjima::metrics {

    template<typename T>
        requires std::integral<T> || std::floating_point<T>
    class Histogram {
    public:
        explicit Histogram(uint16_t size);
        void Update(T observation);
        T GetPercentile(double percentile);
        T GetMostRecent();
        double GetAverage();
#if ENJIMA_METRICS_LEVEL >= 3
        void AddAdditionalMetrics(std::vector<uint64_t>* metricVecPtr);
        [[nodiscard]] std::vector<std::vector<uint64_t>*> GetMetricVectorPtrs() const;
        void ClearMetricVectorPtrs();
#endif

    private:
        std::vector<HistogramValue<T>> hist_;
        uint16_t size_;
        uint16_t cursor_{0};

#if ENJIMA_METRICS_LEVEL >= 3
        std::vector<std::vector<uint64_t>*> metricVecPtrs_;
        std::vector<std::vector<uint64_t>*> prevMetricVecPtrs_;
#endif
    };

}// namespace enjima::metrics

#include "Histogram.tpp"

#endif//ENJIMA_HISTOGRAM_H
