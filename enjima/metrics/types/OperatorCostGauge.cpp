//
// Created by m34ferna on 07/05/24.
//

#include "OperatorCostGauge.h"
#include "enjima/runtime/RuntimeUtil.h"

namespace enjima::metrics {
    double OperatorCostGauge::GetVal()
    {
        return lastCost_.load(std::memory_order::acquire);
    }

    void OperatorCostGauge::UpdateCostMetrics(uint64_t numProcessed, uint64_t scheduledTimeMicros)
    {
        numTimesScheduled.fetch_add(1, std::memory_order::relaxed);
        totOpCpuTimeMicros_.fetch_add(scheduledTimeMicros, std::memory_order::relaxed);
        totProcessed_.fetch_add(numProcessed, std::memory_order::acq_rel);
        auto expectedGuardVal = false;
        if (calcGuard_.compare_exchange_strong(expectedGuardVal, true, std::memory_order::acq_rel)) {
            auto currentTime = enjima::runtime::GetSystemTimeMillis();
            auto currentCounterVal = totProcessed_.load(std::memory_order::relaxed);
            auto currentCpuTime = totOpCpuTimeMicros_.load(std::memory_order::relaxed);
            auto numProcessedWithinPeriod = currentCounterVal - lastCounterVal_;
            auto timeElapsedMs = currentTime - lastCalculatedAt_;
            auto cpuTimeElapsedMicros = currentCpuTime - lastOpCpuTimeMicros_;
            if (numProcessedWithinPeriod > 0 && timeElapsedMs >= updateIntervalMs_ && cpuTimeElapsedMicros > 0) {
                lastCost_.store(static_cast<double>(cpuTimeElapsedMicros) /
                                        (static_cast<double>(numProcessedWithinPeriod)),
                        std::memory_order::release);
                lastCalculatedAt_ = currentTime;
                lastCounterVal_ = currentCounterVal;
                lastOpCpuTimeMicros_ = currentCpuTime;
            }
            calcGuard_.store(false, std::memory_order::release);
        }
    }

    uint64_t OperatorCostGauge::GetTotalOperatorCpuTimeMicros() const
    {
        return totOpCpuTimeMicros_.load(std::memory_order::acquire);
    }

    uint64_t OperatorCostGauge::GetNumTimesScheduled() const
    {
        return numTimesScheduled.load(std::memory_order::acquire);
    }

    OperatorCostGauge::OperatorCostGauge(uint64_t updateIntervalMs)
        : Gauge<double>(1.0), lastCalculatedAt_(enjima::runtime::GetSystemTimeMillis()),
          updateIntervalMs_(updateIntervalMs)
    {
        lastCounterVal_ = totProcessed_.load(std::memory_order::acquire);
        lastOpCpuTimeMicros_ = totOpCpuTimeMicros_.load(std::memory_order::acquire);
    }
}// namespace enjima::metrics
