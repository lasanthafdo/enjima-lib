//
// Created by m34ferna on 07/06/24.
//

#include "ThroughputOptimizedPriorityCalculator.h"
#include "enjima/core/StreamingPipeline.h"
#include "enjima/runtime/IllegalArgumentException.h"
#include "enjima/runtime/scheduling/OperatorContext.h"

namespace enjima::runtime {

    ThroughputOptimizedPriorityCalculator::ThroughputOptimizedPriorityCalculator(MetricsMapT* metricsMapPtr)
        : MetricsBasedPriorityCalculator(metricsMapPtr, 0, 0)
    {
    }

    float ThroughputOptimizedPriorityCalculator::CalculatePriority(const OperatorContext* opCtxtPtr)
    {
        auto opPtr = opCtxtPtr->GetOperatorPtr();
        if (backPressuredOpsMap_.at(opPtr) || outOfInputOpsMap_.at(opPtr)) {
            return 0.0f;
        }
        auto& schedCycleCounter = GetUpdatedPipelineSchedulingCycle(opPtr);
        auto opCyclesLeft = schedCycleCounter.load(std::memory_order::acquire) -
                            schedCycleForOp_.at(opPtr).load(std::memory_order::acquire);
        auto priority = static_cast<float>((numDownstreamOpsMap_.at(opPtr) + 1) * opCyclesLeft);
        return priority;
    }

    std::atomic<unsigned long>& ThroughputOptimizedPriorityCalculator::GetUpdatedPipelineSchedulingCycle(
            const operators::StreamingOperator* opPtr)
    {
        const auto* currentPipeline = pipelineByOpPtrMap_.at(opPtr);
        auto& schedCycleCounter = schedCycleForPipeline_.at(currentPipeline);
        auto currentSchedCycle = schedCycleCounter.load(std::memory_order::acquire);
        auto pipelineCycleComplete = std::all_of(schedCycleForOp_.cbegin(), schedCycleForOp_.cend(),
                [&currentSchedCycle, &currentPipeline, this](auto& opCounterPair) {
                    auto currentOpPtr = opCounterPair.first;
                    auto& schedCycleCounter = opCounterPair.second;
                    return (pipelineByOpPtrMap_.contains(currentOpPtr) &&
                                   pipelineByOpPtrMap_.at(currentOpPtr) != currentPipeline) ||
                           (schedCycleCounter.load(std::memory_order::acquire) == currentSchedCycle);
                });
        if (pipelineCycleComplete) {
            schedCycleCounter.fetch_add(1, std::memory_order::acq_rel);
        }
        return schedCycleCounter;
    }

    void ThroughputOptimizedPriorityCalculator::InitializeMetricsForPipeline(
            core::StreamingPipeline* pStreamingPipeline,
            [[maybe_unused]] const std::vector<OperatorContext*>& opCtxtPtrsVec)
    {
        schedCycleForPipeline_.emplace(pStreamingPipeline, 1);
        for (const auto& opPtr: pStreamingPipeline->GetOperatorsInTopologicalOrder()) {
            auto numDownstreamOps = pStreamingPipeline->GetAllDownstreamOps(opPtr).size();
            numDownstreamOpsMap_.emplace(opPtr, numDownstreamOps);
            downstreamOpVecsMap_.emplace(opPtr, pStreamingPipeline->GetDownstreamOperators(opPtr->GetOperatorId()));
            upstreamOpVecsMap_.emplace(opPtr, pStreamingPipeline->GetUpstreamOperators(opPtr->GetOperatorId()));
            backPressuredOpsMap_.emplace(opPtr, false);
            outOfInputOpsMap_.emplace(opPtr, false);
            schedCycleForOp_.emplace(opPtr, 0);
            pipelineByOpPtrMap_.emplace(opPtr, pStreamingPipeline);
        }
    }

    void ThroughputOptimizedPriorityCalculator::UpdateState() {}

    void ThroughputOptimizedPriorityCalculator::NotifyScheduleCompletion(const OperatorContext* opCtxtPtr)
    {
        const auto* opPtr = opCtxtPtr->GetOperatorPtr();
        if (opCtxtPtr->IsBackPressured()) {
            assert(!opPtr->IsSinkOperator());
            backPressuredOpsMap_[opPtr] = true;
            const auto downstreamOpVec = downstreamOpVecsMap_[opPtr];
            for (const auto* downstreamOpPtr: downstreamOpVec) {
                outOfInputOpsMap_[downstreamOpPtr] = false;
            }
        }
        if (opCtxtPtr->IsOutOfInput() && !opPtr->IsSourceOperator()) {
            outOfInputOpsMap_[opPtr] = true;
            const auto upstreamOpVec = upstreamOpVecsMap_[opPtr];
            for (const auto* upstreamOpPtr: upstreamOpVec) {
                backPressuredOpsMap_[upstreamOpPtr] = false;
            }
        }
        schedCycleForOp_.at(opPtr).fetch_add(1, std::memory_order::acq_rel);
    }

    void ThroughputOptimizedPriorityCalculator::DeactivateMetricsForPipeline(
            core::StreamingPipeline* pStreamingPipeline)
    {
        if (pStreamingPipeline == nullptr) {
            throw IllegalArgumentException{"Pipeline passed for metrics deactivation is null!"};
        }
    }

    bool ThroughputOptimizedPriorityCalculator::IsEligibleForScheduling(uint64_t numPendingEvents,
            const OperatorContext* opCtxtPtr, uint8_t lastOperatorStatus) const
    {
        auto lastScheduledAtMs = opCtxtPtr->GetLastScheduledInMillis();
        if (lastScheduledAtMs == 0 ||
                (numPendingEvents == 0 && lastOperatorStatus > operators::StreamingOperator::kBlocked) ||
                numPendingEvents > 0) {
            return true;
        }
        return false;
    }
}// namespace enjima::runtime
