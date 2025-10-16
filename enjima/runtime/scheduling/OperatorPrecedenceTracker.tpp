//
// Created by m34ferna on 24/05/24.
//

#include "enjima/runtime/CancellationException.h"
namespace enjima::runtime {
    template<typename T>
    OperatorPrecedenceTracker<T>::OperatorPrecedenceTracker()
    {
        constexpr size_t opSchedDecCtxtSize =
                sizeof(OperatorContext) + sizeof(SchedulingDecisionContext) + sizeof(SchedulingContext);
        schedMemoryAreaPtr_ = aligned_alloc(64, Scheduler::kMaxSchedulerQueueSize * opSchedDecCtxtSize);
        schedMemoryCursor_ = schedMemoryAreaPtr_;
    }

    template<typename T>
    OperatorPrecedenceTracker<T>::~OperatorPrecedenceTracker()
    {
        free(schedMemoryAreaPtr_);
    }

    template<typename T>
    void enjima::runtime::OperatorPrecedenceTracker<T>::TrackPipeline(core::StreamingPipeline* pStreamingPipeline)
    {
        // These methods are called only when de-/registering operators. So it is OK to have a lock here
        std::lock_guard<std::mutex> lockGuard(modificationMutex_);
        TrackPipelineWithoutLocking(pStreamingPipeline);
    }

    template<typename T>
    void OperatorPrecedenceTracker<T>::TrackPipelineWithoutLocking(core::StreamingPipeline* pStreamingPipeline)
    {
        currentPipelines_.emplace_back(pStreamingPipeline);
        for (const auto& pStreamOp: pStreamingPipeline->GetOperatorsInTopologicalOrder()) {
            auto* opCtxtPtr = new (schedMemoryCursor_) OperatorContext{pStreamOp, OperatorContext::kInactive};
            schedMemoryCursor_ = static_cast<OperatorContext*>(schedMemoryCursor_) + 1;
            auto schedCtxtPtr = new (schedMemoryCursor_) SchedulingContext{opCtxtPtr, nullptr};
            schedMemoryCursor_ = static_cast<SchedulingContext*>(schedMemoryCursor_) + 1;
            currentOpCtxtPtrs_.emplace_back(opCtxtPtr);
            schedCtxtMap_.emplace(opCtxtPtr, schedCtxtPtr);
            std::vector<operators::StreamingOperator*> downstreamOps =
                    pStreamingPipeline->GetAllDownstreamOps(pStreamOp);
            downstreamOpsByOpPtrMap_.emplace(pStreamOp, downstreamOps);
            opPipelineMap_.emplace(opCtxtPtr, pStreamingPipeline);
        }
    }

    template<typename T>
    bool enjima::runtime::OperatorPrecedenceTracker<T>::UnTrackPipeline(core::StreamingPipeline* pStreamingPipeline)
    {
        // These methods are called only when de-/registering operators. So it is OK to have a lock here
        std::lock_guard<std::mutex> lockGuard(modificationMutex_);
        return UnTrackPipelineWithoutLocking(pStreamingPipeline);
    }

    template<typename T>
    bool enjima::runtime::OperatorPrecedenceTracker<T>::UnTrackPipelineWithoutLocking(
            core::StreamingPipeline* pStreamingPipeline)
    {
        for (const auto& pStreamOp: pStreamingPipeline->GetOperatorsInTopologicalOrder()) {
            auto resultIter = std::find_if(currentOpCtxtPtrs_.begin(), currentOpCtxtPtrs_.end(),
                    [&pStreamOp](
                            const OperatorContext* opCtxtPtr) { return opCtxtPtr->GetOperatorPtr() == pStreamOp; });
            if (resultIter != currentOpCtxtPtrs_.end()) {
                auto* opCtxtPtr = *resultIter;
                opCtxtPtr->SetRuntimeStatus(OperatorContext::kInactive);
                opPipelineMap_.erase(opCtxtPtr);
                downstreamOpsByOpPtrMap_.erase(pStreamOp);
                currentOpCtxtPtrs_.erase(resultIter);
            }
        }
        if (std::erase(currentPipelines_, pStreamingPipeline) > 0) {
            return true;
        }
        return false;
    }

    template<typename T>
    bool enjima::runtime::OperatorPrecedenceTracker<T>::ActivateOperator(
            operators::StreamingOperator* pStreamingOperator)
    {
        auto resultIter = std::find_if(currentOpCtxtPtrs_.begin(), currentOpCtxtPtrs_.end(),
                [&pStreamingOperator](const OperatorContext* opCtxtPtr) {
                    return opCtxtPtr->GetOperatorPtr() == pStreamingOperator;
                });
        bool success = false;
        if (resultIter != currentOpCtxtPtrs_.end()) {
            auto* opCtxtPtr = *resultIter;
            auto expectedStatusInactive = OperatorContext::kInactive;
            success = opCtxtPtr->TrySetStatus(expectedStatusInactive, OperatorContext::kRunnable);
        }
        return success;
    }

    template<typename T>
    void enjima::runtime::OperatorPrecedenceTracker<T>::DeactivateOperator(
            operators::StreamingOperator* pStreamingOperator, std::chrono::milliseconds waitMs)
    {
        auto resultIter = std::find_if(currentOpCtxtPtrs_.begin(), currentOpCtxtPtrs_.end(),
                [&pStreamingOperator](const OperatorContext* opCtxtPtr) {
                    return opCtxtPtr->GetOperatorPtr() == pStreamingOperator;
                });
        if (resultIter != currentOpCtxtPtrs_.end()) {
            auto* opCtxtPtr = *resultIter;
            opCtxtPtr->SetWaitingToCancel();
            auto expectedStatusReady = OperatorContext::kReadyToCancel;
            auto startTime = std::chrono::system_clock::now();
            while (!opCtxtPtr->TrySetStatus(expectedStatusReady, OperatorContext::kInactive)) {
                auto currentTime = std::chrono::system_clock::now();
                if (waitMs.count() > 0 &&
                        std::chrono::duration_cast<std::chrono::milliseconds>(currentTime - startTime) > waitMs) {
                    spdlog::warn("Could not deactivate operator {} correctly. Skipping deactivation!",
                            pStreamingOperator->GetOperatorName());
                    throw CancellationException{std::string("Could not deactivate operator ")
                                    .append(pStreamingOperator->GetOperatorName())
                                    .append(" correctly!")};
                }
                expectedStatusReady = OperatorContext::kReadyToCancel;
            }
        }
    }
}// namespace enjima::runtime