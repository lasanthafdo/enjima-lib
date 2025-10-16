//
// Created by m34ferna on 07/06/24.
//

#include "MinLatencyPriorityCalculator.h"
#include "enjima/core/StreamingPipeline.h"
#include "enjima/runtime/scheduling/OperatorContext.h"

#include <ranges>

namespace enjima::runtime {

    MinLatencyPriorityCalculator::MinLatencyPriorityCalculator(MetricsMapT* metricsMapPtr)
        : MetricsBasedPriorityCalculator(metricsMapPtr, 0, 0)
    {
    }

    float MinLatencyPriorityCalculator::CalculatePriority(const OperatorContext* opCtxtPtr)
    {
        auto opPtr = opCtxtPtr->GetOperatorPtr();
        return 1 / outputCostMap_.at(opPtr).load(std::memory_order::acquire);
    }

    void MinLatencyPriorityCalculator::InitializeMetricsForPipeline(core::StreamingPipeline* pStreamingPipeline,
            [[maybe_unused]] const std::vector<OperatorContext*>& opCtxtPtrsVec)
    {
        trackedPipelines_.emplace_back(pStreamingPipeline);
        for (const auto& opPtr: pStreamingPipeline->GetOperatorsInTopologicalOrder()) {
            outputSelectivityMap_.emplace(opPtr, 0.0f);
            cumulativeOutputCostMap_.emplace(opPtr, 0.0f);
            outputCostMap_.emplace(opPtr, 0.0f);
        }
    }

    void MinLatencyPriorityCalculator::DeactivateMetricsForPipeline(core::StreamingPipeline* pStreamingPipeline)
    {
        std::erase(trackedPipelines_, pStreamingPipeline);
    }

    void MinLatencyPriorityCalculator::UpdateState()
    {
        for (const auto* pipelinePtr: trackedPipelines_) {
            auto opPtrVec = pipelinePtr->GetOperatorsInTopologicalOrder();
            for (auto* opPtr: std::ranges::views::reverse(opPtrVec)) {
                auto outputSelectivity = static_cast<float>(get<3>(metricsMapPtr_->at(opPtr))->GetVal());
                auto downstreamOpVec = pipelinePtr->GetDownstreamOperators(opPtr->GetOperatorId());
                if (!downstreamOpVec.empty()) {
                    for (auto i = downstreamOpVec.size(); i > 0; i--) {
                        outputSelectivity *= outputSelectivityMap_.at(downstreamOpVec[i - 1]);
                    }
                }
                outputSelectivityMap_[opPtr] = outputSelectivity;
                auto costGauge = get<2>(metricsMapPtr_->at(opPtr));
                auto outputCost = static_cast<float>(costGauge->GetVal()) / outputSelectivity;
                auto downstreamCumulativeOutputCost = 0.0f;
                for (auto i = downstreamOpVec.size(); i > 0; i--) {
                    downstreamCumulativeOutputCost += cumulativeOutputCostMap_.at(downstreamOpVec[i - 1]);
                }
                outputCost += downstreamCumulativeOutputCost;
                outputCostMap_.at(opPtr).store(outputCost, std::memory_order::release);
                cumulativeOutputCostMap_[opPtr] = outputCost + downstreamCumulativeOutputCost;
            }
        }
    }

    bool MinLatencyPriorityCalculator::IsEligibleForScheduling(uint64_t numPendingEvents,
            [[maybe_unused]] const OperatorContext* opCtxtPtr, [[maybe_unused]] uint8_t lastOperatorStatus) const
    {
        return numPendingEvents > 0;
    }
}// namespace enjima::runtime
