//
// Created by m34ferna on 27/06/24.
//

#include "DoubleAverageGauge.h"

namespace enjima::metrics {
    double DoubleAverageGauge::GetVal()
    {
        return atomicVal_.load(std::memory_order::acquire) /
               static_cast<double>(count_.load(std::memory_order::acquire));
    }

    void DoubleAverageGauge::UpdateVal(double val)
    {
        atomicVal_.fetch_add(val, std::memory_order::acq_rel);
        count_.fetch_add(1, std::memory_order::acq_rel);
    }

    DoubleAverageGauge::DoubleAverageGauge(double initVal) : Gauge(initVal) {}

}// namespace enjima::metrics
