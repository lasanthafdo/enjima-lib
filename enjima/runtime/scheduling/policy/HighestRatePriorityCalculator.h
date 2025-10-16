//
// Created by m34ferna on 07/06/24.
//

#ifndef ENJIMA_HIGHEST_RATE_PRIORITY_CALCULATOR_H
#define ENJIMA_HIGHEST_RATE_PRIORITY_CALCULATOR_H

#include "MetricsBasedPriorityCalculator.h"

namespace enjima::runtime {

    class HighestRatePriorityCalculator : public MetricsBasedPriorityCalculator {
    public:
        explicit HighestRatePriorityCalculator(MetricsMapT* metricsMapPtr);
        float CalculatePriority(const OperatorContext* opCtxtPtr) override;
        void UpdateState() override;
        void InitializeMetricsForPipeline(core::StreamingPipeline* pStreamingPipeline,
                const std::vector<OperatorContext*>& opCtxtPtrsVec) override;
        void DeactivateMetricsForPipeline(core::StreamingPipeline* pStreamingPipeline) override;
        bool IsEligibleForScheduling(uint64_t numPendingEvents, const OperatorContext* opCtxtPtr,
                uint8_t lastOperatorStatus) const override;

    private:
        std::vector<core::StreamingPipeline*> trackedPipelines_;
        ConcurrentUnorderedMapTBB<const operators::StreamingOperator*, float> globalSelectivityMap_;
        ConcurrentUnorderedMapTBB<const operators::StreamingOperator*, float> globalAvgCostMap_;
        ConcurrentUnorderedMapTBB<const operators::StreamingOperator*, std::atomic<float>> globalOutputRateMap_;
    };

}// namespace enjima::runtime

#endif//ENJIMA_HIGHEST_RATE_PRIORITY_CALCULATOR_H
