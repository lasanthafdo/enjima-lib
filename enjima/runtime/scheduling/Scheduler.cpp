//
// Created by m34ferna on 02/02/24.
//

#include "Scheduler.h"
#include "enjima/core/StreamingPipeline.h"
#include "enjima/runtime/IllegalStateException.h"

namespace enjima::runtime {

    void Scheduler::Init()
    {
        schedulerRunning_.store(true, std::memory_order::release);
    }

    void Scheduler::RegisterPipeline(core::StreamingPipeline* pStreamPipeline)
    {
        std::lock_guard<std::mutex> vecLockGuard{containerMutex_};
        runnablePipelines_.emplace_back(pStreamPipeline);
        for (const auto& opPtr: pStreamPipeline->GetOperatorsInTopologicalOrder()) {
            runnableOperators_.emplace_back(opPtr);
        }
        if (schedQEmpty_.load(std::memory_order::acquire)) {
            auto expected = true;
            schedQEmpty_.compare_exchange_strong(expected, false, std::memory_order::acq_rel);
        }
    }

    void Scheduler::DeRegisterPipeline(core::StreamingPipeline* pStreamPipeline)
    {
        std::lock_guard<std::mutex> vecLockGuard{containerMutex_};
        for (const auto* pStreamOp: pStreamPipeline->GetOperatorsInTopologicalOrder()) {
            auto numErased = std::erase(runnableOperators_, pStreamOp);
            if (numErased != 1) {
                throw IllegalStateException(std::string("[Scheduler] There was more or less than 1 streaming operator "
                                                        "registered with same pointer for ID ")
                                .append(std::to_string(pStreamOp->GetOperatorId())));
            }
        }
        std::erase(runnablePipelines_, pStreamPipeline);
        if (runnableOperators_.empty()) {
            auto expected = false;
            schedQEmpty_.compare_exchange_strong(expected, true, std::memory_order::acq_rel);
        }
    }

    bool Scheduler::ContainsRunnableOperators() const
    {
        return !schedQEmpty_.load(std::memory_order::acquire);
    }

    void Scheduler::Shutdown()
    {
        schedulerRunning_.store(false, std::memory_order::release);
    }

    StreamingTask::ProcessingMode Scheduler::GetProcessingMode() const
    {
        return processingMode_;
    }
}// namespace enjima::runtime
