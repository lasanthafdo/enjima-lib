//
// Created by m34ferna on 18/06/24.
//

#include "NonPreemptiveMode.h"
#include "enjima/runtime/RuntimeUtil.h"

namespace enjima::runtime {
    NonPreemptiveMode::NonPreemptiveMode(MetricsMapT* metricsMapPtr,
            std::atomic<uint64_t>* nextSchedulingUpdateAtMicrosPtr)
        : metricsMapPtr_(metricsMapPtr), nextSchedulingUpdateAtMicrosPtr_(nextSchedulingUpdateAtMicrosPtr)
    {
    }

    long NonPreemptiveMode::CalculateNumEventsToProcess(operators::StreamingOperator*& opPtr) const
    {
        auto metricsTuple = metricsMapPtr_->at(opPtr);
        auto cost = get<2>(metricsTuple)->GetVal();
        auto currentTimeMicros = runtime::GetSteadyClockMicros();
        auto nextSchedulingTimeMicros = nextSchedulingUpdateAtMicrosPtr_->load(std::memory_order::acquire);
        auto remainingTimeMicros =
                currentTimeMicros < nextSchedulingTimeMicros ? nextSchedulingTimeMicros - currentTimeMicros : 0;
        auto numEventsToProcess = std::lround(static_cast<double>(remainingTimeMicros) / cost);
        assert(numEventsToProcess >= 0);
        return std::max(numEventsToProcess, 1000L);
    }
}// namespace enjima::runtime
