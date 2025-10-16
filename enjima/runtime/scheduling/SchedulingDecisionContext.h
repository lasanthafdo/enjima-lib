//
// Created by m34ferna on 27/05/24.
//

#ifndef ENJIMA_SCHEDULING_DECISION_CONTEXT_H
#define ENJIMA_SCHEDULING_DECISION_CONTEXT_H

#include "SchedulingContext.h"
#include "enjima/operators/OperatorsInternals.fwd.h"
#include "enjima/runtime/RuntimeUtil.h"

#include <vector>

namespace enjima::runtime {
    class alignas(hardware_constructive_interference_size) SchedulingDecisionContext {
    public:
        enum class ScheduleStatus { kNormal, kToBeCancelled, kIneligible };

        [[nodiscard]] static uint64_t GetScheduledCpuTime();
        [[nodiscard]] static uint64_t GetNumProcessed();
        [[nodiscard]] static ScheduleStatus GetScheduleStatus();
        [[nodiscard]] static uint8_t GetLastOperatorStatus();
        static void SetDecisionToIneligible();

        constexpr SchedulingDecisionContext() = default;
        SchedulingDecisionContext(operators::StreamingOperator* opPtr, metrics::Counter<uint64_t>* processedCounter);

        void MarkStartOfWork();
        void MarkEndOfWork(uint8_t lastOperatorStatus);
        void MarkEndOfSchedule();
        void SetDecision(ScheduleStatus status, uint64_t stopAtCount);
        [[nodiscard]] operators::StreamingOperator* GetOperatorPtr() const;
        [[nodiscard]] bool IsEndOfSchedule() const;

    private:
        // The following variables can be thread local because they are set and read from during a single scheduled run.
        // This reduces thread synchronization required. Take care not to use them outside of that context!
        thread_local static uint64_t cpuTimeAtStartOfWork_;
        thread_local static uint64_t cpuTimeAtEndOfWork_;
        thread_local static uint64_t toBeProcessedAtEndOfSchedule_;
        thread_local static uint64_t numProcessedAtStartOfWork_;
        thread_local static uint64_t numProcessedAtEndOfWork_;
        thread_local static ScheduleStatus scheduleStatus_;
        thread_local static uint8_t lastOperatorStatus_;

        operators::StreamingOperator* const operatorPtr_{nullptr};
        metrics::Counter<uint64_t>* const processedCounter_{nullptr};
        std::atomic<bool> endOfSchedule_{true};
    };
}// namespace enjima::runtime


#endif//ENJIMA_SCHEDULING_DECISION_CONTEXT_H
