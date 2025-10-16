//
// Created by m34ferna on 27/06/24.
//

#ifndef ENJIMA_FLOAT_AVERAGE_GAUGE_H
#define ENJIMA_FLOAT_AVERAGE_GAUGE_H

#include "Gauge.h"
#include <atomic>
#include <cstdint>

namespace enjima::metrics {

    class DoubleAverageGauge : public Gauge<double> {
    public:
        explicit DoubleAverageGauge(double initVal);
        double GetVal() override;
        void UpdateVal(double val) override;

    private:
        std::atomic<double> atomicVal_{0};
        std::atomic<uint64_t> count_{0};
    };

}// namespace enjima::metrics


#endif//ENJIMA_FLOAT_AVERAGE_GAUGE_H
