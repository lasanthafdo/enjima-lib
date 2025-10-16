//
// Created by m34ferna on 07/06/24.
//

#include "MinCostPriorityCalculator.h"
#include "enjima/core/StreamingPipeline.h"
#include "enjima/runtime/IllegalArgumentException.h"
#include "enjima/runtime/IllegalStateException.h"
#include "enjima/runtime/scheduling/OperatorContext.h"

#include <map>
#include <stack>

namespace enjima::runtime {

    MinCostPriorityCalculator::MinCostPriorityCalculator(MetricsMapT* metricsMapPtr)
        : MetricsBasedPriorityCalculator(metricsMapPtr, 0, 0)
    {
    }

    float MinCostPriorityCalculator::CalculatePriority(const OperatorContext* opCtxtPtr)
    {
        auto opPtr = opCtxtPtr->GetOperatorPtr();
        auto currentOpPtrPipeline = pipelineByOpPtrMap_.at(opPtr);
        if (pipelineActivatedMap_.at(currentOpPtrPipeline)) {
            auto currentSchedCycle = GetUpdatedPipelineSchedulingCycle();
            auto opCyclesLeft = currentSchedCycle - schedCycleForOp_.at(opPtr).load(std::memory_order::acquire);
            // We can add 1 to the opCyclesLeft so that there is a bit of leeway, and workers are allowed to schedule one round ahead.
            // This prevents some workers starving while other workers complete the round for straggler operators. However,
            // for strict round-robin, we do not incorporate this optimization
            /*
            // The following code can be used if operators that get more cycles need to be penalized proportionately.
            if (static_cast<float>(opCyclesLeft) < minObservedOpCyclesLeft) {
                // For this condition to be satisfied, opCycles must be negative. So multiply by 1 to make it positive.
                // This allows us to avoid sign conversion when biasing
                minObservedOpCyclesLeft = static_cast<float>(opCyclesLeft * -1);
            }
            auto biasedWaitScore = opCyclesLeft <= 0 ? static_cast<float>(opCyclesLeft) / minObservedOpCyclesLeft + 1.0f
                                                     : static_cast<float>(opCyclesLeft + 1);
            auto priority = biasedWaitScore +
                            postOrderPriorityMap_.at(opPtr) /
                                    static_cast<float>(numActiveOperators_.load(std::memory_order::acquire) + 1);
            */
            auto priority = std::max(static_cast<float>(opCyclesLeft), 0.0f) +
                            postOrderPriorityMap_.at(opPtr) / (kPriorityRange_ + 1.0f);

            return priority;
        }
        return 0.0f;
    }

    int64_t MinCostPriorityCalculator::GetUpdatedPipelineSchedulingCycle()
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

    void MinCostPriorityCalculator::SetOperatorPriorityForPostOrderTraversal(
            const core::StreamingPipeline* pStreamingPipeline,
            const std::vector<operators::StreamingOperator*>::size_type numOperators)
    {
        auto sinkOpRoot = pStreamingPipeline->GetSinkOperators().at(0);
        std::stack<operators::StreamingOperator*> tempStack;
        std::stack<operators::StreamingOperator*> traversalStack;
        std::map<operators::StreamingOperator*, uint64_t> sharedOperatorMap;
        auto currentOpPriority = static_cast<float>(numOperators) + 1.0f;
        auto relPriorityDivisor = static_cast<float>(numOperators) / kPriorityRange_;

        tempStack.push(sinkOpRoot);
        while (!tempStack.empty()) {
            auto currentOpPtr = tempStack.top();
            tempStack.pop();
            traversalStack.push(currentOpPtr);
            auto upstreamOps = pStreamingPipeline->GetUpstreamOperators(currentOpPtr->GetOperatorId());
            for (const auto& upstreamOpPtr: upstreamOps) {
                auto downStreamOfUpstream = pStreamingPipeline->GetDownstreamOperators(upstreamOpPtr->GetOperatorId());
                auto downStreamOfUpstreamSize = downStreamOfUpstream.size();
                if (downStreamOfUpstreamSize == 1) {
                    tempStack.push(upstreamOpPtr);
                }
                else if (downStreamOfUpstreamSize == 0) {
                    throw IllegalStateException{"Unsupported pipeline for min-cost"};
                }
                else {
                    auto sharedOp = sharedOperatorMap.find(upstreamOpPtr);
                    if (sharedOp == sharedOperatorMap.end()) {
                        sharedOp = sharedOperatorMap.emplace(upstreamOpPtr, downStreamOfUpstreamSize).first;
                    }
                    --sharedOp->second;
                    if (sharedOp->second == 0) {
                        tempStack.push(upstreamOpPtr);
                    }
                }
            }
        }
        while (!traversalStack.empty()) {
            auto currentOpPtr = traversalStack.top();
            traversalStack.pop();
            auto relPriority = (currentOpPriority - 1.0f) / relPriorityDivisor;
            auto emplaceResult = postOrderPriorityMap_.emplace(currentOpPtr, relPriority);
            if (!emplaceResult.second) {
                throw IllegalStateException{"Post-order traversal for operator already in map!"};
            }
            currentOpPriority -= 1.0f;
        }
    }

    void MinCostPriorityCalculator::InitializeMetricsForPipeline(core::StreamingPipeline* pStreamingPipeline,
            [[maybe_unused]] const std::vector<OperatorContext*>& opCtxtPtrsVec)
    {
        auto [entry, success] = pipelineActivatedMap_.emplace(pStreamingPipeline, false);
        if (pStreamingPipeline->GetSinkOperators().size() != 1) {
            throw IllegalArgumentException{"Min cost algorithm only supports pipelines with 1 sink!"};
        }
        const auto numOperators = pStreamingPipeline->GetOperatorsInTopologicalOrder().size();
        SetOperatorPriorityForPostOrderTraversal(pStreamingPipeline, numOperators);
        auto currentSchedCycle = currentSchedCycle_.load(std::memory_order::acquire);
        for (const auto& opPtr: pStreamingPipeline->GetOperatorsInTopologicalOrder()) {
            pipelineByOpPtrMap_.emplace(opPtr, pStreamingPipeline);
            schedCycleForOp_.emplace(opPtr, currentSchedCycle);
        }
        numActiveOperators_.fetch_add(numOperators, std::memory_order::acq_rel);
        entry->second.store(true, std::memory_order::release);
    }

    void MinCostPriorityCalculator::DeactivateMetricsForPipeline(core::StreamingPipeline* pStreamingPipeline)
    {
        if (pStreamingPipeline == nullptr) {
            throw IllegalArgumentException{"Pipeline passed for metrics deactivation is null!"};
        }
        pipelineActivatedMap_.at(pStreamingPipeline).store(false, std::memory_order::release);
        const auto numOperators = pStreamingPipeline->GetOperatorsInTopologicalOrder().size();
        numActiveOperators_.fetch_sub(numOperators, std::memory_order::acq_rel);
    }

    void MinCostPriorityCalculator::UpdateState() {}

    void MinCostPriorityCalculator::NotifyScheduleCompletion(const OperatorContext* opCtxtPtr)
    {
        const auto* opPtr = opCtxtPtr->GetOperatorPtr();
        schedCycleForOp_.at(opPtr).fetch_add(1, std::memory_order::acq_rel);
    }

    bool MinCostPriorityCalculator::IsEligibleForScheduling([[maybe_unused]] uint64_t numPendingEvents,
            [[maybe_unused]] const OperatorContext* opCtxtPtr, [[maybe_unused]] uint8_t lastOperatorStatus) const
    {
        return true;
    }
}// namespace enjima::runtime
