//
// Created by m34ferna on 07/05/24.
//

#ifndef ENJIMA_OPERATOR_COST_GAUGE_H
#define ENJIMA_OPERATOR_COST_GAUGE_H

#include "Counter.h"
#include "Gauge.h"
#include <cstdint>

namespace enjima::metrics {

    class OperatorCostGauge : public Gauge<double> {
    public:
        explicit OperatorCostGauge(uint64_t updateIntervalMs);
        double GetVal() override;
        void UpdateCostMetrics(uint64_t numProcessed, uint64_t scheduledTimeMicros);

        [[nodiscard]] uint64_t GetTotalOperatorCpuTimeMicros() const;
        [[nodiscard]] uint64_t GetNumTimesScheduled() const;

    private:
        uint64_t lastCounterVal_;
        uint64_t lastCalculatedAt_;
        uint64_t lastOpCpuTimeMicros_;
        std::atomic<uint64_t> totOpCpuTimeMicros_{0};
        std::atomic<uint64_t> totProcessed_{0};
        std::atomic<uint64_t> numTimesScheduled{0};
        std::atomic<double> lastCost_{1.0};
        uint64_t updateIntervalMs_;
        std::atomic<bool> calcGuard_{false};
    };

}// namespace enjima::metrics


#endif//ENJIMA_OPERATOR_COST_GAUGE_H
