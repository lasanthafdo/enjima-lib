//
// Created by m34ferna on 17/05/24.
//

#ifndef ENJIMA_OPERATOR_CONTEXT_H
#define ENJIMA_OPERATOR_CONTEXT_H

#include "Scheduler.h"
#include "enjima/operators/StreamingOperator.h"

namespace enjima::runtime {

    class alignas(hardware_constructive_interference_size) OperatorContext {
    public:
        inline static const uint8_t kScheduled = 1;
        inline static const uint8_t kRunnable = 2;
        inline static const uint8_t kCancellationInProgress = 4;
        inline static const uint8_t kReadyToCancel = 8;
        inline static const uint8_t kInactive = 16;

        explicit OperatorContext(operators::StreamingOperator* opPtr, uint8_t opStatus);

        [[nodiscard]] operators::StreamingOperator* GetOperatorPtr() const;
        [[nodiscard]] AdaptivePipelineContext* GetPipelineCtxtPtr() const;
        void SetPipelineCtxtPtr(AdaptivePipelineContext* pipelineCtxtPtr);
        bool TrySetStatus(uint8_t& expectedStatus, uint8_t newStatus);
        bool TrySetStatusReadyToCancel();
        bool TrySetStatusScheduled();
        bool TrySetStatusDeScheduled();
        void SetRuntimeStatus(uint8_t newStatus);
        [[nodiscard]] bool IsWaitingToCancel() const;
        void SetWaitingToCancel();
        [[nodiscard]] bool IsWaitingToBePickedByScheduler() const;
        void MarkPickedByScheduler();
        [[nodiscard]] uint64_t GetLastScheduledInMillis() const;

        void SetLastOperatorStatus(uint8_t lastOperatorStatus);
        [[nodiscard]] uint8_t GetLastOperatorStatus() const;
        [[nodiscard]] bool IsBackPressured() const;
        [[nodiscard]] bool IsOutOfInput() const;

#if ENJIMA_METRICS_LEVEL >= 3
        void SetScheduleGapCounters(metrics::Counter<uint64_t>* scheduleGapCounterPtr,
                metrics::Counter<uint64_t>* scheduleGapTimeCounterPtr);
#endif

    private:
        operators::StreamingOperator* const operatorPtr_;
        AdaptivePipelineContext* pipelineCtxtPtr_{nullptr};
        std::atomic<bool> waitingToBePickedByScheduler_{true};
        std::atomic<bool> waitingToCancel_{false};
        std::atomic<uint8_t> runtimeStatus_;
        std::atomic<uint8_t> lastOperatorStatus_{operators::StreamingOperator::kBlocked};
        std::atomic<uint64_t> lastScheduledMs_{0};

#if ENJIMA_METRICS_LEVEL >= 3
        std::atomic<uint64_t> lastDeScheduledMs_{0};
        metrics::Counter<uint64_t>* scheduleGapCounterPtr_{nullptr};
        metrics::Counter<uint64_t>* scheduleGapTimeCounterPtr_{nullptr};
#endif
    };

}// namespace enjima::runtime


#endif//ENJIMA_OPERATOR_CONTEXT_H
