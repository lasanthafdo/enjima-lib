//
// Created by m34ferna on 04/04/24.
//

#include "Histogram.h"

namespace enjima::metrics {
    template<typename T>
        requires std::integral<T> || std::floating_point<T>
    Histogram<T>::Histogram(uint16_t size) : size_(size)
    {
        hist_.reserve(size);
        for (auto i = 0; i < size; i++) {
            hist_.emplace_back(0);
        }
    }

    template<typename T>
        requires std::integral<T> || std::floating_point<T>
    void Histogram<T>::Update(T observation)
    {
        hist_[cursor_++].SetVal(observation);
        if (cursor_ == size_) {
            cursor_ = 0;
        }
    }

    template<typename T>
        requires std::integral<T> || std::floating_point<T>
    double Histogram<T>::GetAverage()
    {
        T sum = 0;
        for (auto& elem: hist_) {
            sum += elem.GetVal();
        }
        return static_cast<double>(sum) / static_cast<double>(size_);
    }

    template<typename T>
        requires std::integral<T> || std::floating_point<T>
    T Histogram<T>::GetPercentile(double percentile)
    {
        uint16_t elementIdx = std::ceil(percentile / 100 * size_) - 1;
        assert(elementIdx >= 0 && elementIdx < size_);
        auto tempVec = std::vector<T>{};
        tempVec.reserve(hist_.size());
        for (auto& elem: hist_) {
            tempVec.emplace_back(elem.GetVal());
        }
        std::sort(tempVec.begin(), tempVec.end());
        return tempVec[elementIdx];
    }

    template<typename T>
        requires std::integral<T> || std::floating_point<T>
    T Histogram<T>::GetMostRecent()
    {
        auto mostRecentIndex = (cursor_ + size_ - 1) % size_;
        return hist_[mostRecentIndex].GetVal();
    }

#if ENJIMA_METRICS_LEVEL >= 3
    template<typename T>
        requires std::integral<T> || std::floating_point<T>
    std::vector<std::vector<uint64_t>*> Histogram<T>::GetMetricVectorPtrs() const
    {
        return metricVecPtrs_;
    }

    template<typename T>
        requires std::integral<T> || std::floating_point<T>
    void Histogram<T>::ClearMetricVectorPtrs()
    {
        for (auto metricVecPtr: prevMetricVecPtrs_) {
            delete metricVecPtr;
        }
        prevMetricVecPtrs_.clear();
        prevMetricVecPtrs_.swap(metricVecPtrs_);
    }

    template<typename T>
        requires std::integral<T> || std::floating_point<T>
    void Histogram<T>::AddAdditionalMetrics(std::vector<uint64_t>* metricVecPtr)
    {
        metricVecPtrs_.emplace_back(metricVecPtr);
    }
#endif
}// namespace enjima::metrics
