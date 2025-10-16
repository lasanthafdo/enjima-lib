//
// Created by m34ferna on 07/06/24.
//

#ifndef ENJIMA_INPUT_SIZE_BASED_PRIORITY_CALCULATOR_H
#define ENJIMA_INPUT_SIZE_BASED_PRIORITY_CALCULATOR_H

#include "MetricsBasedPriorityCalculator.h"
#include "enjima/core/CoreInternals.fwd.h"

namespace enjima::runtime {

    class InputSizeBasedPriorityCalculator : public MetricsBasedPriorityCalculator {
    public:
        explicit InputSizeBasedPriorityCalculator(MetricsMapT* metricsMapPtr);
        float CalculatePriority(const OperatorContext* opCtxtPtr) override;
        void UpdateState() override;
        void InitializeMetricsForPipeline(core::StreamingPipeline* pStreamingPipeline,
                const std::vector<OperatorContext*>& opCtxtPtrsVec) override;
        void DeactivateMetricsForPipeline(core::StreamingPipeline* pStreamingPipeline) override;
    };

}// namespace enjima::runtime


#endif//ENJIMA_INPUT_SIZE_BASED_PRIORITY_CALCULATOR_H
