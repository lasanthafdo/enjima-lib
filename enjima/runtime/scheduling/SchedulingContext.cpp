//
// Created by m34ferna on 24/05/24.
//

#include "SchedulingContext.h"
#include "OperatorContext.h"
#include "SchedulingDecisionContext.h"

namespace enjima::runtime {
    SchedulingContext::SchedulingContext(OperatorContext* opCtxtPtr, SchedulingDecisionContext* decisionCtxtPtr)
        : opCtxtPtr_(opCtxtPtr), decisionCtxtPtr_(decisionCtxtPtr)
    {
    }

    float SchedulingContext::GetPriority() const
    {
        return priority_.load(std::memory_order::acquire);
    }

    OperatorContext* SchedulingContext::GetOpCtxtPtr() const
    {
        return opCtxtPtr_;
    }

    void SchedulingContext::SetPriority(float priority)
    {
        priority_.store(priority, std::memory_order::release);
    }

    bool SchedulingContext::TryAcquire()
    {
        auto expectedVal = kReleased_;
        return internalStatus_.compare_exchange_strong(expectedVal, kAcquired_, std::memory_order::acq_rel);
    }

    bool SchedulingContext::TryRelease()
    {
        auto expectedVal = kAcquired_;
        return internalStatus_.compare_exchange_strong(expectedVal, kReleased_, std::memory_order::acq_rel);
    }

    bool SchedulingContext::IsAvailable() const
    {
        return internalStatus_.load(std::memory_order::acquire) == kReleased_;
    }

    void SchedulingContext::Deactivate()
    {
        internalStatus_.store(kInactive_, std::memory_order::release);
    }

    bool SchedulingContext::IsActive() const
    {
        return internalStatus_.load(std::memory_order::acquire) != kInactive_;
    }

    SchedulingDecisionContext* SchedulingContext::GetDecisionCtxtPtr() const
    {
        return decisionCtxtPtr_;
    }

}// namespace enjima::runtime
