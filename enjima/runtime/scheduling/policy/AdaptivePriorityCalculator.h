//
// Created by m34ferna on 07/06/24.
//

#ifndef ENJIMA_ADAPTIVE_PRIORITY_CALCULATOR_H
#define ENJIMA_ADAPTIVE_PRIORITY_CALCULATOR_H

#include "AdaptivePipelineContext.h"
#include "MetricsBasedPriorityCalculator.h"
#include "SchedulingPolicy.h"

namespace enjima::runtime {

    class AdaptivePriorityCalculator : public MetricsBasedPriorityCalculator {
    public:
        AdaptivePriorityCalculator(MetricsMapT* metricsMapPtr, metrics::Profiler* profilerPtr, uint64_t eventThreshold,
                uint64_t idleThresholdMs);
        ~AdaptivePriorityCalculator() override = default;

        float CalculatePriority(const OperatorContext* opCtxtPtr) override;
        void UpdateState() override;
        void InitializeMetricsForPipeline(core::StreamingPipeline* pStreamingPipeline,
                const std::vector<OperatorContext*>& opCtxtPtrsVec) override;
        void DeactivateMetricsForPipeline(core::StreamingPipeline* pStreamingPipeline) override;
        bool IsEligibleForScheduling(uint64_t numPendingEvents, const OperatorContext* opCtxtPtr,
                uint8_t lastOperatorStatus) const override;

    private:
        const uint32_t numStateBasedWorkers_;

        std::vector<core::StreamingPipeline*> trackedPipelines_;
        ConcurrentUnorderedMapTBB<const operators::StreamingOperator*, float> outputSelectivityMap_;
        ConcurrentUnorderedMapTBB<const operators::StreamingOperator*, float> cumulativeOutputCostMap_;
#ifdef ENJIMA_USE_NORMALIZED_OUTPUT_COST
        UnorderedHashMapST<const operators::StreamingOperator*, float> outputCostMap_;
        ConcurrentUnorderedMapTBB<const operators::StreamingOperator*, std::atomic<float>> normalizedOutputCostMap_;
#else
        ConcurrentUnorderedMapTBB<const operators::StreamingOperator*, std::atomic<float>> outputCostMap_;
#endif

        ConcurrentUnorderedMapTBB<const operators::StreamingOperator*, size_t> numDownstreamOpsMap_;
        UnorderedHashMapST<const core::StreamingPipeline*, AdaptivePipelineContext> adaptivePipelineCtxtMap_;
        metrics::Profiler* profilerPtr_;
        uint64_t lastParamUpdateAtMs_;
        uint64_t adaptPeriodMs_{1'000};
        uint64_t maxEventThreshold_{100'000};
        uint64_t maxIdleThreshold_{100};
        float eventThresholdMaxStepSize_{1'000.0};
        float idleThresholdMaxStepSize_{10.0};
        std::atomic<uint32_t> numActiveOperators_{0};

#ifdef ENJIMA_PRINT_SCHED_INFO
        uint64_t lastPrintedAtMs_{0};
#endif
    };

}// namespace enjima::runtime


#endif//ENJIMA_ADAPTIVE_PRIORITY_CALCULATOR_H
