//
// Created by m34ferna on 21/07/24.
//

#include "SchedulingPolicy.h"
#include "enjima/operators/StreamingOperator.h"
#include "enjima/runtime/RuntimeUtil.h"
#include "enjima/runtime/scheduling/OperatorContext.h"

namespace enjima::runtime {
    SchedulingPolicy::SchedulingPolicy(uint64_t eventThreshold, uint64_t idleThresholdMs)
        : kEventThreshold_(eventThreshold), kIdleThresholdMs_(idleThresholdMs)
    {
    }

    bool SchedulingPolicy::IsEligibleForScheduling(uint64_t numPendingEvents, const OperatorContext* opCtxtPtr,
            uint8_t lastOperatorStatus) const
    {
        auto lastScheduledAtMs = opCtxtPtr->GetLastScheduledInMillis();
        return static_cast<bool>(lastOperatorStatus & operators::StreamingOperator::kCanOutput) &&
               (numPendingEvents > kEventThreshold_ ||
                       (numPendingEvents > 0 &&
                               (runtime::GetSystemTimeMillis() - lastScheduledAtMs) > kIdleThresholdMs_));
    }
}// namespace enjima::runtime