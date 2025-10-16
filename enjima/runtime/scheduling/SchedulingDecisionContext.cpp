//
// Created by m34ferna on 27/05/24.
//

#include "SchedulingDecisionContext.h"

namespace enjima::runtime {
    thread_local uint64_t SchedulingDecisionContext::cpuTimeAtStartOfWork_ = 0;
    thread_local uint64_t SchedulingDecisionContext::cpuTimeAtEndOfWork_ = 0;
    thread_local uint64_t SchedulingDecisionContext::toBeProcessedAtEndOfSchedule_ = 0;
    thread_local uint64_t SchedulingDecisionContext::numProcessedAtStartOfWork_ = 0;
    thread_local uint64_t SchedulingDecisionContext::numProcessedAtEndOfWork_ = 0;
    thread_local SchedulingDecisionContext::ScheduleStatus SchedulingDecisionContext::scheduleStatus_ =
            SchedulingDecisionContext::ScheduleStatus::kIneligible;
    thread_local uint8_t SchedulingDecisionContext::lastOperatorStatus_ = operators::StreamingOperator::kBlocked;

    SchedulingDecisionContext::SchedulingDecisionContext(operators::StreamingOperator* opPtr,
            metrics::Counter<uint64_t>* processedCounter)
        : operatorPtr_(opPtr), processedCounter_(processedCounter)
    {
    }


    uint64_t SchedulingDecisionContext::GetScheduledCpuTime()
    {
        assert(cpuTimeAtEndOfWork_ >= cpuTimeAtStartOfWork_);
        return cpuTimeAtEndOfWork_ - cpuTimeAtStartOfWork_;
    }

    void SchedulingDecisionContext::MarkStartOfWork()
    {
        if (scheduleStatus_ == ScheduleStatus::kNormal) {
            numProcessedAtStartOfWork_ = processedCounter_->GetCount();
            cpuTimeAtStartOfWork_ = GetCurrentThreadCPUTimeMicros();
        }
    }

    void SchedulingDecisionContext::MarkEndOfWork(uint8_t lastOperatorStatus)
    {
        if (scheduleStatus_ == ScheduleStatus::kNormal) {
            cpuTimeAtEndOfWork_ = GetCurrentThreadCPUTimeMicros();
            numProcessedAtEndOfWork_ = processedCounter_->GetCount();
        }
        lastOperatorStatus_ = lastOperatorStatus;
    }

    void SchedulingDecisionContext::MarkEndOfSchedule()
    {
        endOfSchedule_.store(true, std::memory_order::release);
    }

    operators::StreamingOperator* SchedulingDecisionContext::GetOperatorPtr() const
    {
        return operatorPtr_;
    }

    SchedulingDecisionContext::ScheduleStatus SchedulingDecisionContext::GetScheduleStatus()
    {
        return scheduleStatus_;
    }

    uint64_t SchedulingDecisionContext::GetNumProcessed()
    {
        return numProcessedAtEndOfWork_ - numProcessedAtStartOfWork_;
    }

    bool SchedulingDecisionContext::IsEndOfSchedule() const
    {
        return endOfSchedule_.load(std::memory_order::acquire) ||
               processedCounter_->GetCount() >= toBeProcessedAtEndOfSchedule_;
    }

    uint8_t SchedulingDecisionContext::GetLastOperatorStatus()
    {
        return lastOperatorStatus_;
    }

    void SchedulingDecisionContext::SetDecision(SchedulingDecisionContext::ScheduleStatus status, uint64_t stopAtCount)
    {
        scheduleStatus_ = status;
        toBeProcessedAtEndOfSchedule_ = stopAtCount;
        endOfSchedule_.store(false, std::memory_order::release);
    }

    void SchedulingDecisionContext::SetDecisionToIneligible()
    {
        scheduleStatus_ = ScheduleStatus::kIneligible;
    }

}// namespace enjima::runtime
