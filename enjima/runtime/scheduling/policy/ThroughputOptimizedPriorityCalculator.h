//
// Created by m34ferna on 07/06/24.
//

#ifndef ENJIMA_THROUGHPUT_OPTIMIZED_PRIORITY_CALCULATOR_H
#define ENJIMA_THROUGHPUT_OPTIMIZED_PRIORITY_CALCULATOR_H

#include "MetricsBasedPriorityCalculator.h"
#include "enjima/runtime/scheduling/policy/SchedulingPolicy.h"

namespace enjima::runtime {

    class ThroughputOptimizedPriorityCalculator : public MetricsBasedPriorityCalculator {
    public:
        explicit ThroughputOptimizedPriorityCalculator(MetricsMapT* metricsMapPtr);
        float CalculatePriority(const OperatorContext* opCtxtPtr) override;
        void UpdateState() override;
        void InitializeMetricsForPipeline(core::StreamingPipeline* pStreamingPipeline,
                const std::vector<OperatorContext*>& opCtxtPtrsVec) override;
        void DeactivateMetricsForPipeline(core::StreamingPipeline* pStreamingPipeline) override;
        void NotifyScheduleCompletion(const OperatorContext* opCtxtPtr);
        [[nodiscard]] bool IsEligibleForScheduling(uint64_t numPendingEvents, const OperatorContext* opCtxtPtr,
                uint8_t lastOperatorStatus) const override;

    private:
        std::atomic<unsigned long>& GetUpdatedPipelineSchedulingCycle(const operators::StreamingOperator* opPtr);

        ConcurrentUnorderedMapTBB<const operators::StreamingOperator*, size_t> numDownstreamOpsMap_;
        ConcurrentUnorderedMapTBB<const operators::StreamingOperator*, core::StreamingPipeline*> pipelineByOpPtrMap_;
        ConcurrentUnorderedMapTBB<const operators::StreamingOperator*, std::atomic<uint64_t>> schedCycleForOp_;
        ConcurrentUnorderedMapTBB<const core::StreamingPipeline*, std::atomic<uint64_t>> schedCycleForPipeline_;
        ConcurrentUnorderedMapTBB<const operators::StreamingOperator*, bool> backPressuredOpsMap_;
        ConcurrentUnorderedMapTBB<const operators::StreamingOperator*, bool> outOfInputOpsMap_;
        ConcurrentUnorderedMapTBB<const operators::StreamingOperator*, const std::vector<operators::StreamingOperator*>>
                downstreamOpVecsMap_;
        ConcurrentUnorderedMapTBB<const operators::StreamingOperator*, const std::vector<operators::StreamingOperator*>>
                upstreamOpVecsMap_;
    };

}// namespace enjima::runtime


#endif//ENJIMA_THROUGHPUT_OPTIMIZED_PRIORITY_CALCULATOR_H
