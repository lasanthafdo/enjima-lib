//
// Created by m34ferna on 17/05/24.
//

#include "OperatorContext.h"

namespace enjima::runtime {
    OperatorContext::OperatorContext(operators::StreamingOperator* opPtr, uint8_t opStatus)
        : operatorPtr_(opPtr), runtimeStatus_(opStatus)
    {
    }

    bool OperatorContext::TrySetStatus(uint8_t& expectedStatus, uint8_t newStatus)
    {
        return runtimeStatus_.compare_exchange_strong(expectedStatus, newStatus, std::memory_order::acq_rel);
    }

    bool OperatorContext::TrySetStatusReadyToCancel()
    {
        auto expectedStatus = kCancellationInProgress;
        if (runtimeStatus_.compare_exchange_strong(expectedStatus, kReadyToCancel, std::memory_order::acq_rel)) {
            waitingToCancel_.store(false, std::memory_order::release);
            return true;
        }
        return false;
    }

    bool OperatorContext::TrySetStatusScheduled()
    {
        auto expectedStatus = kRunnable;
        if (runtimeStatus_.compare_exchange_strong(expectedStatus, kScheduled, std::memory_order::acq_rel)) {
            lastScheduledMs_.store(GetSystemTimeMillis(), std::memory_order::release);
            lastOperatorStatus_.store(operators::StreamingOperator::kRunThreshold, std::memory_order::release);
#if ENJIMA_METRICS_LEVEL >= 3
            scheduleGapCounterPtr_->Inc();
            scheduleGapTimeCounterPtr_->Inc(
                    GetSystemTimeMicros() - lastDeScheduledMs_.load(std::memory_order::acquire));
#endif
            return true;
        }
        return false;
    }

    bool OperatorContext::TrySetStatusDeScheduled()
    {
        auto expectedStatus = kScheduled;
        if (runtimeStatus_.compare_exchange_strong(expectedStatus, kRunnable, std::memory_order::acq_rel)) {
#if ENJIMA_METRICS_LEVEL >= 3
            lastDeScheduledMs_.store(GetSystemTimeMicros(), std::memory_order::release);
#endif
            return true;
        }
        return false;
    }

    void OperatorContext::SetRuntimeStatus(uint8_t newStatus)
    {
        runtimeStatus_.store(newStatus, std::memory_order::release);
    }

    operators::StreamingOperator* OperatorContext::GetOperatorPtr() const
    {
        return operatorPtr_;
    }

    bool OperatorContext::IsWaitingToCancel() const
    {
        return waitingToCancel_.load(std::memory_order::acquire);
    }

    void OperatorContext::SetWaitingToCancel()
    {
        waitingToCancel_.store(true, std::memory_order::release);
    }

    uint64_t OperatorContext::GetLastScheduledInMillis() const
    {
        return lastScheduledMs_.load(std::memory_order::acquire);
    }

    bool OperatorContext::IsWaitingToBePickedByScheduler() const
    {
        return waitingToBePickedByScheduler_.load(std::memory_order::acquire);
    }

    void OperatorContext::MarkPickedByScheduler()
    {
        waitingToBePickedByScheduler_.store(false, std::memory_order::release);
    }

    bool OperatorContext::IsBackPressured() const
    {
        return !(lastOperatorStatus_.load(std::memory_order::acquire) & operators::StreamingOperator::kCanOutput);
    }

    bool OperatorContext::IsOutOfInput() const
    {
        return !(lastOperatorStatus_.load(std::memory_order::acquire) & operators::StreamingOperator::kHasInput);
    }

    void OperatorContext::SetLastOperatorStatus(uint8_t lastOperatorStatus)
    {
        lastOperatorStatus_.store(lastOperatorStatus, std::memory_order::release);
    }

    uint8_t OperatorContext::GetLastOperatorStatus() const
    {
        return lastOperatorStatus_.load(std::memory_order::acquire);
    }

    AdaptivePipelineContext* OperatorContext::GetPipelineCtxtPtr() const
    {
        return pipelineCtxtPtr_;
    }

    void OperatorContext::SetPipelineCtxtPtr(AdaptivePipelineContext* pipelineCtxtPtr)
    {
        pipelineCtxtPtr_ = pipelineCtxtPtr;
    }

#if ENJIMA_METRICS_LEVEL >= 3
    void OperatorContext::SetScheduleGapCounters(metrics::Counter<uint64_t>* scheduleGapCounterPtr,
            metrics::Counter<uint64_t>* scheduleGapTimeCounterPtr)
    {
        scheduleGapCounterPtr_ = scheduleGapCounterPtr;
        scheduleGapTimeCounterPtr_ = scheduleGapTimeCounterPtr;
        lastDeScheduledMs_.store(runtime::GetSystemTimeMicros(), std::memory_order::release);
    }
#endif
}// namespace enjima::runtime
