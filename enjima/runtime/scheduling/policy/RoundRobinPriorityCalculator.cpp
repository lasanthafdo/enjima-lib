//
// Created by m34ferna on 07/06/24.
//

#include "RoundRobinPriorityCalculator.h"
#include "enjima/core/StreamingPipeline.h"
#include "enjima/runtime/IllegalArgumentException.h"
#include "enjima/runtime/scheduling/OperatorContext.h"

namespace enjima::runtime {

    RoundRobinPriorityCalculator::RoundRobinPriorityCalculator(MetricsMapT* metricsMapPtr)
        : MetricsBasedPriorityCalculator(metricsMapPtr, 0, 0)
    {
    }

    float RoundRobinPriorityCalculator::CalculatePriority(const OperatorContext* opCtxtPtr)
    {
        auto opPtr = opCtxtPtr->GetOperatorPtr();
        auto currentOpPtrPipeline = pipelineByOpPtrMap_.at(opPtr);
        if (pipelineActivatedMap_.at(currentOpPtrPipeline)) {
            auto currentSchedCycle = GetUpdatedPipelineSchedulingCycle();
            auto opCyclesLeft = currentSchedCycle - schedCycleForOp_.at(opPtr).load(std::memory_order::acquire);
            // We can add 1 to the opCyclesLeft so that there is a bit of leeway, and workers are allowed to schedule one round ahead.
            // This prevents some workers starving while other workers complete the round for straggler operators. However,
            // for strict round-robin, we do not incorporate this optimization
            // First part of term implies :
            //              opCycles <= 0 => priority > 0.0f
            //              opCycles > 0 => priority > opCyclesLeft
            // where opCycles is an integral type
            auto priority = std::max(static_cast<float>(opCyclesLeft), 0.0f) +
                            topologicalOrderPriorityMap_.at(opPtr) /
                                    static_cast<float>(numActiveOperators_.load(std::memory_order::acquire));
            return priority;
        }
        return 0.0f;
    }

    int64_t RoundRobinPriorityCalculator::GetUpdatedPipelineSchedulingCycle()
    {
        auto currentSchedCycle = currentSchedCycle_.load(std::memory_order::acquire);
        auto pipelineCycleComplete = std::all_of(schedCycleForOp_.cbegin(), schedCycleForOp_.cend(),
                [&currentSchedCycle, this](auto& opCounterPair) {
                    auto currentOpPtr = opCounterPair.first;
                    auto currentOpPtrPipeline = pipelineByOpPtrMap_.at(currentOpPtr);
                    if (pipelineActivatedMap_.at(currentOpPtrPipeline)) {
                        auto& schedCycleForOp = opCounterPair.second;
                        return schedCycleForOp.load(std::memory_order::acquire) >= currentSchedCycle;
                    }
                    return true;
                });
        if (pipelineCycleComplete) {
            currentSchedCycle_.fetch_add(1, std::memory_order::acq_rel);
        }
        return currentSchedCycle_.load(std::memory_order::acquire);
    }

    void RoundRobinPriorityCalculator::InitializeMetricsForPipeline(core::StreamingPipeline* pStreamingPipeline,
            [[maybe_unused]] const std::vector<OperatorContext*>& opCtxtPtrsVec)
    {
        auto [entry, success] = pipelineActivatedMap_.emplace(pStreamingPipeline, false);
        auto currentSchedCycle = currentSchedCycle_.load(std::memory_order::acquire);
        for (const auto& opPtr: pStreamingPipeline->GetOperatorsInTopologicalOrder()) {
            pipelineByOpPtrMap_.emplace(opPtr, pStreamingPipeline);
            schedCycleForOp_.emplace(opPtr, currentSchedCycle);
        }
        const auto dagDepth = pStreamingPipeline->GetDagDepth();
        for (const auto& [opID, dagLevel]: pStreamingPipeline->GetOperatorsLevelsMap()) {
            auto opPtr = pStreamingPipeline->GetOperatorForID(opID);
            auto priority = static_cast<float>(1 + dagDepth - dagLevel);
            topologicalOrderPriorityMap_.emplace(opPtr, priority);
        }
        const auto numOperators = pStreamingPipeline->GetOperatorsInTopologicalOrder().size();
        numActiveOperators_.fetch_add(numOperators, std::memory_order::acq_rel);
        entry->second.store(true, std::memory_order::release);
    }

    void RoundRobinPriorityCalculator::UpdateState() {}

    void RoundRobinPriorityCalculator::NotifyScheduleCompletion(const OperatorContext* opCtxtPtr)
    {
        const auto* opPtr = opCtxtPtr->GetOperatorPtr();
        schedCycleForOp_.at(opPtr).fetch_add(1, std::memory_order::acq_rel);
    }

    void RoundRobinPriorityCalculator::DeactivateMetricsForPipeline(core::StreamingPipeline* pStreamingPipeline)
    {
        if (pStreamingPipeline == nullptr) {
            throw IllegalArgumentException{"Pipeline passed for metrics deactivation is null!"};
        }
        pipelineActivatedMap_.at(pStreamingPipeline).store(false, std::memory_order::release);
        const auto numOperators = pStreamingPipeline->GetOperatorsInTopologicalOrder().size();
        numActiveOperators_.fetch_sub(numOperators, std::memory_order::acq_rel);
    }

    bool RoundRobinPriorityCalculator::IsEligibleForScheduling([[maybe_unused]] uint64_t numPendingEvents,
            [[maybe_unused]] const OperatorContext* opCtxtPtr, [[maybe_unused]] uint8_t lastOperatorStatus) const
    {
        return true;
    }
}// namespace enjima::runtime
