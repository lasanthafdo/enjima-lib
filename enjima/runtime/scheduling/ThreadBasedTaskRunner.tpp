//
// Created by m34ferna on 10/06/24.
//

#include "enjima/runtime/RuntimeUtil.h"
namespace enjima::runtime {
    using StreamingOpT = operators::StreamingOperator;
    using ProcModeT = StreamingTask::ProcessingMode;

    template<StreamingTask::ProcessingMode ProcMode>
    ThreadBasedTaskRunner<ProcMode>::ThreadBasedTaskRunner(operators::StreamingOperator* streamingOperatorPtr,
            ThreadBasedTask* tbTaskPtr, std::atomic<bool>* taskRunningPtr, std::atomic<bool>* downstreamWaitingPtr,
            metrics::Profiler* profilerPtr)
        : pStreamingOperator_(streamingOperatorPtr), pThreadBasedTask_(tbTaskPtr), pTaskRunning_(taskRunningPtr),
          pDownstreamWaiting_(downstreamWaitingPtr)
    {
        if (pStreamingOperator_->IsSourceOperator()) {
            processedCounter_ = profilerPtr->GetOutCounter(pStreamingOperator_->GetOperatorName());
        }
        else {
            processedCounter_ = profilerPtr->GetInCounter(pStreamingOperator_->GetOperatorName());
        }
        costGauge_ = profilerPtr->GetOperatorCostGauge(pStreamingOperator_->GetOperatorName());
    }

    template<StreamingTask::ProcessingMode ProcMode>
    void ThreadBasedTaskRunner<ProcMode>::Run()
    {
        std::string procModeIdStr = ProcMode == ProcModeT::kBlockBasedSingle ? "block based single" : "unknown";
        spdlog::info("Using {} processing mode for worker with operator id : {}", procModeIdStr,
                pStreamingOperator_->GetOperatorId());
        uint8_t operatorStatus;
        while (pTaskRunning_->load(std::memory_order::acquire)) {
            MarkStartOfWork();
            operatorStatus = pStreamingOperator_->ProcessBlock();
            MarkEndOfWork();
            if (pDownstreamWaiting_->load(std::memory_order::acquire) &&
                    static_cast<bool>(operatorStatus & StreamingOpT::kHasOutput)) {
                pThreadBasedTask_->NotifyOutputAvailability();
            }
            if (!static_cast<bool>(operatorStatus & StreamingOpT::kHasInput)) {
                pThreadBasedTask_->WaitForUpstreamOutput(pStreamingOperator_->GetBlockedUpstreamChannelId());
            }
            if (!static_cast<bool>(operatorStatus & StreamingOpT::kCanOutput)) {
                std::this_thread::yield();
            }
        }
        if (!pStreamingOperator_->IsSourceOperator()) {
            // Process any remaining data once
            operatorStatus = pStreamingOperator_->ProcessBlock();
            while (static_cast<bool>(operatorStatus & StreamingOpT::kHasInput)) {
                operatorStatus = pStreamingOperator_->ProcessBlock();
                if (pDownstreamWaiting_->load(std::memory_order::acquire) &&
                        static_cast<bool>(operatorStatus & StreamingOpT::kHasOutput)) {
                    pThreadBasedTask_->NotifyOutputAvailability();
                }
            }
        }
    }

    template<>
    void ThreadBasedTaskRunner<ProcModeT::kBlockBasedBatch>::Run()
    {
        std::string procModeIdStr = "block based batch";
        spdlog::info("Using {} processing mode for worker with operator id : {}", procModeIdStr,
                pStreamingOperator_->GetOperatorId());
        uint8_t operatorStatus;
        while (pTaskRunning_->load(std::memory_order::acquire)) {
            MarkStartOfWork();
            operatorStatus = pStreamingOperator_->ProcessBlockInBatches();
            MarkEndOfWork();
            if (pDownstreamWaiting_->load(std::memory_order::acquire) &&
                    static_cast<bool>(operatorStatus & StreamingOpT::kHasOutput)) {
                pThreadBasedTask_->NotifyOutputAvailability();
            }
            if (!static_cast<bool>(operatorStatus & StreamingOpT::kHasInput)) {
                pThreadBasedTask_->WaitForUpstreamOutput(pStreamingOperator_->GetBlockedUpstreamChannelId());
            }
            if (!static_cast<bool>(operatorStatus & StreamingOpT::kCanOutput)) {
                std::this_thread::yield();
            }
        }
        if (!pStreamingOperator_->IsSourceOperator()) {
            // Process any remaining data once
            operatorStatus = pStreamingOperator_->ProcessBlockInBatches();
            while (static_cast<bool>(operatorStatus & StreamingOpT::kHasInput)) {
                operatorStatus = pStreamingOperator_->ProcessBlockInBatches();
                if (pDownstreamWaiting_->load(std::memory_order::acquire) &&
                        static_cast<bool>(operatorStatus & StreamingOpT::kHasOutput)) {
                    pThreadBasedTask_->NotifyOutputAvailability();
                }
            }
        }
    }

    template<>
    void ThreadBasedTaskRunner<ProcModeT::kQueueBasedSingle>::Run()
    {
        std::string procModeIdStr = "queue based single";
        spdlog::info("Using {} processing mode for worker with operator id : {}", procModeIdStr,
                pStreamingOperator_->GetOperatorId());
        uint8_t operatorStatus;
        while (pTaskRunning_->load(std::memory_order::acquire)) {
            MarkStartOfWork();
            operatorStatus = pStreamingOperator_->ProcessQueue();
            MarkEndOfWork();
            if (pDownstreamWaiting_->load(std::memory_order::acquire) &&
                    static_cast<bool>(operatorStatus & StreamingOpT::kHasOutput)) {
                pThreadBasedTask_->NotifyOutputAvailability();
            }
            if (!static_cast<bool>(operatorStatus & StreamingOpT::kHasInput)) {
                pThreadBasedTask_->WaitForUpstreamOutput(pStreamingOperator_->GetBlockedUpstreamChannelId());
            }
            if (!static_cast<bool>(operatorStatus & StreamingOpT::kCanOutput)) {
                std::this_thread::yield();
            }
        }
        if (!pStreamingOperator_->IsSourceOperator()) {
            // Process any remaining data once
            operatorStatus = pStreamingOperator_->ProcessQueue();
            while (static_cast<bool>(operatorStatus & StreamingOpT::kHasInput)) {
                operatorStatus = pStreamingOperator_->ProcessQueue();
                if (pDownstreamWaiting_->load(std::memory_order::acquire) &&
                        static_cast<bool>(operatorStatus & StreamingOpT::kHasOutput)) {
                    pThreadBasedTask_->NotifyOutputAvailability();
                }
            }
        }
    }

    template<StreamingTask::ProcessingMode ProcMode>
    void ThreadBasedTaskRunner<ProcMode>::MarkStartOfWork()
    {
        numProcessedAtStartOfWork_ = processedCounter_->GetCount();
        cpuTimeAtStartOfWork_ = runtime::GetCurrentThreadCPUTimeMicros();
    }

    template<StreamingTask::ProcessingMode ProcMode>
    void ThreadBasedTaskRunner<ProcMode>::MarkEndOfWork()
    {
        auto numProcessed = processedCounter_->GetCount() - numProcessedAtStartOfWork_;
        auto scheduledTimeMicros = runtime::GetCurrentThreadCPUTimeMicros() - cpuTimeAtStartOfWork_;
        costGauge_->UpdateCostMetrics(numProcessed, scheduledTimeMicros);
    }

}// namespace enjima::runtime
