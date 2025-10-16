//
// Created by m34ferna on 05/06/24.
//

#ifndef ENJIMA_CPU_GAUGE_H
#define ENJIMA_CPU_GAUGE_H

#include "Gauge.h"

#include <atomic>
#include <cstdint>

namespace enjima::metrics {

    class CpuGauge : public Gauge<float> {
    public:
        CpuGauge();
        float GetVal() override;
        void SetUpdateIntervalMicros(uint64_t updateIntervalMicros);

    private:
        const unsigned int numProcessors_;
        uint64_t lastCpuTime_;
        uint64_t lastSteadyClockTime_;
        std::atomic<float> lastCpuUsage_{0.0f};
        std::atomic<bool> calcGuard{false};
        uint64_t updateIntervalMicros_{10'000};
    };

}// namespace enjima::metrics

#endif//ENJIMA_CPU_GAUGE_H
