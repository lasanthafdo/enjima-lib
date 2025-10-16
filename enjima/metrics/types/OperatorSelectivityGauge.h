//
// Created by m34ferna on 07/05/24.
//

#ifndef ENJIMA_OPERATOR_SELECTIVITY_GAUGE_H
#define ENJIMA_OPERATOR_SELECTIVITY_GAUGE_H

#include "Counter.h"
#include "Gauge.h"
#include <cstdint>

namespace enjima::metrics {

    class OperatorSelectivityGauge : public Gauge<double> {
    public:
        OperatorSelectivityGauge(const Counter<uint64_t>* inCounter, const Counter<uint64_t>* outCounter,
                uint64_t updateIntervalMs);
        double GetVal() override;

    private:
        const Counter<uint64_t>* inCounter_;
        const Counter<uint64_t>* outCounter_;
        uint64_t lastInCount_{0};
        uint64_t lastOutCount_{0};
        uint64_t lastCalculatedAt_;
        std::atomic<double> lasSelectivity_{1.0};
        uint64_t updateIntervalMs_;
        std::atomic<bool> calcGuard_{false};
    };

}// namespace enjima::metrics


#endif//ENJIMA_OPERATOR_SELECTIVITY_GAUGE_H
