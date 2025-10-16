//
// Created by m34ferna on 07/06/24.
//

#ifndef ENJIMA_SP_LATENCY_OPTIMIZED_PRIORITY_CALCULATOR_H
#define ENJIMA_SP_LATENCY_OPTIMIZED_PRIORITY_CALCULATOR_H

#include "MetricsBasedPriorityCalculator.h"

namespace enjima::runtime {

    class SPLatencyOptimizedPriorityCalculator : public MetricsBasedPriorityCalculator {
    public:
        SPLatencyOptimizedPriorityCalculator(MetricsMapT* metricsMapPtr, uint64_t eventThreshold,
                uint64_t idleThresholdMs);
        float CalculatePriority(const OperatorContext* opCtxtPtr) override;
        void UpdateState() override;
        void InitializeMetricsForPipeline(core::StreamingPipeline* pStreamingPipeline,
                const std::vector<OperatorContext*>& opCtxtPtrsVec) override;
        void DeactivateMetricsForPipeline(core::StreamingPipeline* pStreamingPipeline) override;

    private:
        std::vector<core::StreamingPipeline*> trackedPipelines_;
        ConcurrentUnorderedMapTBB<const operators::StreamingOperator*, float> outputSelectivityMap_;
        ConcurrentUnorderedMapTBB<const operators::StreamingOperator*, float> cumulativeOutputCostMap_;
        ConcurrentUnorderedMapTBB<const operators::StreamingOperator*, float> outputCostMap_;
    };

}// namespace enjima::runtime

#endif//ENJIMA_SP_LATENCY_OPTIMIZED_PRIORITY_CALCULATOR_H
