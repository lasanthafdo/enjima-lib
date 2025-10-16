//
// Created by m34ferna on 07/06/24.
//

#include "HighestRatePriorityCalculator.h"
#include "enjima/core/StreamingPipeline.h"
#include "enjima/runtime/scheduling/OperatorContext.h"

#include <ranges>

namespace enjima::runtime {

    HighestRatePriorityCalculator::HighestRatePriorityCalculator(MetricsMapT* metricsMapPtr)
        : MetricsBasedPriorityCalculator(metricsMapPtr, 0, 0)
    {
    }

    float HighestRatePriorityCalculator::CalculatePriority(const OperatorContext* opCtxtPtr)
    {
        auto opPtr = opCtxtPtr->GetOperatorPtr();
        auto priority = globalOutputRateMap_.at(opPtr).load(std::memory_order::acquire);
        return priority;
    }

    void HighestRatePriorityCalculator::InitializeMetricsForPipeline(core::StreamingPipeline* pStreamingPipeline,
            [[maybe_unused]] const std::vector<OperatorContext*>& opCtxtPtrsVec)
    {
        trackedPipelines_.emplace_back(pStreamingPipeline);
        for (const auto& opPtr: pStreamingPipeline->GetOperatorsInTopologicalOrder()) {
            globalSelectivityMap_.emplace(opPtr, 0.0f);
            globalAvgCostMap_.emplace(opPtr, 0.0f);
            globalOutputRateMap_.emplace(opPtr, 0.0f);
        }
    }

    void HighestRatePriorityCalculator::DeactivateMetricsForPipeline(core::StreamingPipeline* pStreamingPipeline)
    {
        std::erase(trackedPipelines_, pStreamingPipeline);
    }

    void HighestRatePriorityCalculator::UpdateState()
    {
        for (const auto* pipelinePtr: trackedPipelines_) {
            auto opPtrVec = pipelinePtr->GetOperatorsInTopologicalOrder();
            for (auto* opPtr: std::ranges::views::reverse(opPtrVec)) {
                auto currOpSelectivity = static_cast<float>(get<3>(metricsMapPtr_->at(opPtr))->GetVal());
                auto costGauge = get<2>(metricsMapPtr_->at(opPtr));
                auto currOpCost = static_cast<float>(costGauge->GetVal());
                auto downstreamOpVec = pipelinePtr->GetDownstreamOperators(opPtr->GetOperatorId());
                float globalSelectivity = currOpSelectivity;
                float globalAvgCost = currOpCost;
                if (!downstreamOpVec.empty()) {
                    // Note we approximate the join operator calculations without taking additional HT values into account.
                    if (downstreamOpVec.size() == 1) {
                        auto downstreamOp = downstreamOpVec.front();
                        globalSelectivity = currOpSelectivity * globalSelectivityMap_.at(downstreamOp);
                        globalAvgCost = currOpCost + currOpSelectivity * globalAvgCostMap_.at(downstreamOp);
                    }
                    else {
                        // Since global selectivity is undefined for a shared operator, we define it as min of paths for operator sharing
                        for (auto i = downstreamOpVec.size(); i > 0; i--) {
                            globalSelectivity = std::min(globalSelectivity,
                                    currOpSelectivity * globalSelectivityMap_.at(downstreamOpVec[i - 1]));
                        }
                        for (auto i = downstreamOpVec.size(); i > 0; i--) {
                            globalAvgCost += currOpSelectivity * globalAvgCostMap_.at(downstreamOpVec[i - 1]);
                        }
                    }
                }
                globalSelectivityMap_[opPtr] = globalSelectivity;
                globalAvgCostMap_[opPtr] = globalAvgCost;
                globalOutputRateMap_.at(opPtr).store(globalSelectivity / globalAvgCost, std::memory_order::release);
            }
        }
    }

    bool HighestRatePriorityCalculator::IsEligibleForScheduling(uint64_t numPendingEvents,
            [[maybe_unused]] const OperatorContext* opCtxtPtr, [[maybe_unused]] uint8_t lastOperatorStatus) const
    {
        return numPendingEvents > 0;
    }
}// namespace enjima::runtime
