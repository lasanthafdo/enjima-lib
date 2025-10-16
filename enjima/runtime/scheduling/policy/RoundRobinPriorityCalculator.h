//
// Created by m34ferna on 07/06/24.
//

#ifndef ENJIMA_ROUND_ROBIN_PRIORITY_CALCULATOR_H
#define ENJIMA_ROUND_ROBIN_PRIORITY_CALCULATOR_H

#include "MetricsBasedPriorityCalculator.h"
#include "enjima/runtime/scheduling/policy/SchedulingPolicy.h"

namespace enjima::runtime {

    class RoundRobinPriorityCalculator : public MetricsBasedPriorityCalculator {
    public:
        explicit RoundRobinPriorityCalculator(MetricsMapT* metricsMapPtr);
        float CalculatePriority(const OperatorContext* opCtxtPtr) override;
        void UpdateState() override;
        void InitializeMetricsForPipeline(core::StreamingPipeline* pStreamingPipeline,
                const std::vector<OperatorContext*>& opCtxtPtrsVec) override;
        void DeactivateMetricsForPipeline(core::StreamingPipeline* pStreamingPipeline) override;
        void NotifyScheduleCompletion(const OperatorContext* opCtxtPtr);
        [[nodiscard]] bool IsEligibleForScheduling(uint64_t numPendingEvents, const OperatorContext* opCtxtPtr,
                uint8_t lastOperatorStatus) const override;

    private:
        int64_t GetUpdatedPipelineSchedulingCycle();

        ConcurrentUnorderedMapTBB<const operators::StreamingOperator*, core::StreamingPipeline*> pipelineByOpPtrMap_;
        ConcurrentUnorderedMapTBB<const operators::StreamingOperator*, std::atomic<int64_t>> schedCycleForOp_;
        std::atomic<int64_t> currentSchedCycle_;
        ConcurrentUnorderedMapTBB<const core::StreamingPipeline*, std::atomic<bool>> pipelineActivatedMap_;
        ConcurrentUnorderedMapTBB<const operators::StreamingOperator*, std::atomic<float>> topologicalOrderPriorityMap_;
        std::atomic<uint64_t> numActiveOperators_{0};
    };

}// namespace enjima::runtime


#endif//ENJIMA_ROUND_ROBIN_PRIORITY_CALCULATOR_H
