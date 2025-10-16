//
// Created by m34ferna on 17/05/24.
//

#include "SchedulingDecisionContext.h"
#include "enjima/runtime/RuntimeUtil.h"

namespace enjima::runtime {
    using StreamingOpT = operators::StreamingOperator;
    using ProcModeT = StreamingTask::ProcessingMode;

    template<ProcModeT ProcMode>
    thread_local operators::StreamingOperator* StateBasedTaskRunner<ProcMode>::pNextOp_ = nullptr;

    template<ProcModeT ProcMode>
    StateBasedTaskRunner<ProcMode>::StateBasedTaskRunner(uint32_t taskId, Scheduler* schedulerPtr,
            std::atomic<bool>* taskRunningPtr)
        : taskId_(taskId), pScheduler_(schedulerPtr), pTaskRunning_(taskRunningPtr)
    {
    }

    template<ProcModeT ProcMode>
    void StateBasedTaskRunner<ProcMode>::Run()
    {
        spdlog::info("Using {} processing mode for worker with task id : {}", GetProcessingMode(), taskId_);
        uint8_t opStatus = StreamingOpT::kBlocked;
        while (pTaskRunning_->load(std::memory_order::acquire)) {
            auto pSchedResult = pScheduler_->GetNextOperator();
            pNextOp_ = pSchedResult->GetOperatorPtr();
            // TODO Possible optimization : Only return from ProcessBlock method when the specified number of events have been processed
            switch (pSchedResult->GetScheduleStatus()) {
                case SchedulingDecisionContext::ScheduleStatus::kNormal:
                    pSchedResult->MarkStartOfWork();
                    opStatus = DoProcess();
                    while (opStatus >= StreamingOpT::kRunThreshold && !pSchedResult->IsEndOfSchedule()) {
                        opStatus = DoProcess();
                    }
                    pSchedResult->MarkEndOfWork(opStatus);
                    break;
                case SchedulingDecisionContext::ScheduleStatus::kToBeCancelled:
                    pSchedResult->MarkStartOfWork();
                    opStatus = DoProcess();
                    // TODO Need to revisit the logic here. Probable cause for hanging at cancellation!
                    while (static_cast<bool>(opStatus & StreamingOpT::kHasInput) &&
                            pTaskRunning_->load(std::memory_order::acquire)) {
                        opStatus = DoProcess();
                    }
                    pSchedResult->MarkEndOfWork(opStatus);
                    break;
                case SchedulingDecisionContext::ScheduleStatus::kIneligible:
                default:
                    break;
            }
        }
    }

    template<StreamingTask::ProcessingMode ProcMode>
    uint8_t StateBasedTaskRunner<ProcMode>::DoProcess()
    {
        return pNextOp_->ProcessBlock();
    }

    template<>
    uint8_t StateBasedTaskRunner<ProcModeT::kBlockBasedBatch>::DoProcess()
    {
        return pNextOp_->ProcessBlockInBatches();
    }

    template<>
    uint8_t StateBasedTaskRunner<ProcModeT::kQueueBasedSingle>::DoProcess()
    {
        return pNextOp_->ProcessQueue();
    }

    template<StreamingTask::ProcessingMode ProcMode>
    std::string StateBasedTaskRunner<ProcMode>::GetProcessingMode() const
    {
        std::string procModeIdStr = ProcMode == ProcModeT::kBlockBasedSingle ? "block based single" : "unknown";
        return procModeIdStr;
    }

    template<>
    std::string StateBasedTaskRunner<ProcModeT::kBlockBasedBatch>::GetProcessingMode() const
    {
        return std::string("block based batch");
    }

    template<>
    std::string StateBasedTaskRunner<ProcModeT::kQueueBasedSingle>::GetProcessingMode() const
    {
        return std::string("queue based single");
    }

}// namespace enjima::runtime