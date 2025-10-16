//
// Created by m34ferna on 07/06/24.
//

#include "SimpleThroughputPriorityCalculator.h"
#include "enjima/core/StreamingPipeline.h"
#include "enjima/runtime/IllegalArgumentException.h"
#include "enjima/runtime/scheduling/OperatorContext.h"

namespace enjima::runtime {
    SimpleThroughputPriorityCalculator::SimpleThroughputPriorityCalculator()
        : MetricsBasedPriorityCalculator(nullptr, 0, 0)
    {
    }

    float SimpleThroughputPriorityCalculator::CalculatePriority(const OperatorContext* opCtxtPtr)
    {
        auto opPtr = opCtxtPtr->GetOperatorPtr();
        auto priority = static_cast<float>(numDownstreamOpsMap_.at(opPtr) + 1);
        return priority;
    }

    void SimpleThroughputPriorityCalculator::InitializeMetricsForPipeline(core::StreamingPipeline* pStreamingPipeline,
            [[maybe_unused]] const std::vector<OperatorContext*>& opCtxtPtrsVec)
    {
        for (const auto& opPtr: pStreamingPipeline->GetOperatorsInTopologicalOrder()) {
            auto numDownstreamOps = pStreamingPipeline->GetAllDownstreamOps(opPtr).size();
            numDownstreamOpsMap_.emplace(opPtr, numDownstreamOps);
        }
    }

    void SimpleThroughputPriorityCalculator::UpdateState() {}

    void SimpleThroughputPriorityCalculator::DeactivateMetricsForPipeline(core::StreamingPipeline* pStreamingPipeline)
    {
        if (pStreamingPipeline == nullptr) {
            throw IllegalArgumentException{"Pipeline passed for metrics deactivation is null!"};
        }
    }

}// namespace enjima::runtime
