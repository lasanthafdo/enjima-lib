//
// Created by m34ferna on 07/06/24.
//

#include "InputSizeBasedPriorityCalculator.h"
#include "enjima/runtime/IllegalArgumentException.h"
#include "enjima/runtime/scheduling/OperatorContext.h"

namespace enjima::runtime {
    InputSizeBasedPriorityCalculator::InputSizeBasedPriorityCalculator(MetricsMapT* metricsMapPtr)
        : MetricsBasedPriorityCalculator(metricsMapPtr, 0, 0)
    {
    }

    float InputSizeBasedPriorityCalculator::CalculatePriority(const OperatorContext* opCtxtPtr)
    {
        auto opPtr = opCtxtPtr->GetOperatorPtr();
        if (opPtr->IsSourceOperator()) {
            return kMaxPriorityFloat;
        }
        auto metricsTuple = metricsMapPtr_->at(opPtr);
        auto eventGauge = get<0>(metricsTuple);
        return static_cast<float>(eventGauge->GetVal());
    }

    void InputSizeBasedPriorityCalculator::UpdateState() {}

    void InputSizeBasedPriorityCalculator::InitializeMetricsForPipeline(core::StreamingPipeline* pStreamingPipeline,
            [[maybe_unused]] const std::vector<OperatorContext*>& opCtxtPtrsVec)
    {
        if (pStreamingPipeline == nullptr) {
            throw IllegalArgumentException{"Pipeline passed for metrics initialization is null!"};
        }
    }

    void InputSizeBasedPriorityCalculator::DeactivateMetricsForPipeline(core::StreamingPipeline* pStreamingPipeline)
    {
        if (pStreamingPipeline == nullptr) {
            throw IllegalArgumentException{"Pipeline passed for metrics deactivation is null!"};
        }
    }

}// namespace enjima::runtime
