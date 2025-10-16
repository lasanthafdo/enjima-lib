//
// Created by m34ferna on 05/06/24.
//

#include "CpuGauge.h"
#include "enjima/runtime/RuntimeUtil.h"
#include <thread>

namespace enjima::metrics {
    CpuGauge::CpuGauge()
        : Gauge<float>(0.0f), numProcessors_(std::thread::hardware_concurrency()),
          lastCpuTime_(enjima::runtime::GetCurrentProcessCPUTimeMicros()),
          lastSteadyClockTime_(enjima::runtime::GetSteadyClockMicros())
    {
    }

    float CpuGauge::GetVal()
    {
        auto expectedUpdateStatus = false;
        if (calcGuard.compare_exchange_strong(expectedUpdateStatus, true, std::memory_order::acq_rel)) {
            auto currentSteadyClockTime = enjima::runtime::GetSteadyClockMicros();
            auto steadyClockTimeMicros = currentSteadyClockTime - lastSteadyClockTime_;
            if (steadyClockTimeMicros > updateIntervalMicros_) {
                auto currentCpuTime = enjima::runtime::GetRUsageCurrentProcessCPUTimeMicros();
                auto cpuTimeMicros = currentCpuTime - lastCpuTime_;
                lastCpuUsage_.store((static_cast<float>(cpuTimeMicros) / static_cast<float>(steadyClockTimeMicros)) /
                                            static_cast<float>(numProcessors_),
                        std::memory_order::release);
                lastCpuTime_ = currentCpuTime;
                lastSteadyClockTime_ = currentSteadyClockTime;
            }
            calcGuard.store(false, std::memory_order::release);
        }
        return lastCpuUsage_.load(std::memory_order::acquire);
    }

    void CpuGauge::SetUpdateIntervalMicros(uint64_t updateIntervalMicros)
    {
        updateIntervalMicros_ = updateIntervalMicros;
    }
}// namespace enjima::metrics
