//
// Created by m34ferna on 07/06/24.
//

#ifndef ENJIMA_MIN_COST_PRIORITY_CALCULATOR_H
#define ENJIMA_MIN_COST_PRIORITY_CALCULATOR_H

#include "MetricsBasedPriorityCalculator.h"

namespace enjima::runtime {

    class MinCostPriorityCalculator : public MetricsBasedPriorityCalculator {
    public:
        explicit MinCostPriorityCalculator(MetricsMapT* metricsMapPtr);
        float CalculatePriority(const OperatorContext* opCtxtPtr) override;
        void UpdateState() override;
        void InitializeMetricsForPipeline(core::StreamingPipeline* pStreamingPipeline,
                const std::vector<OperatorContext*>& opCtxtPtrsVec) override;
        void DeactivateMetricsForPipeline(core::StreamingPipeline* pStreamingPipeline) override;
        void NotifyScheduleCompletion(const OperatorContext* opCtxtPtr);
        bool IsEligibleForScheduling(uint64_t numPendingEvents, const OperatorContext* opCtxtPtr,
                uint8_t lastOperatorStatus) const override;

    private:
        int64_t GetUpdatedPipelineSchedulingCycle();
        void SetOperatorPriorityForPostOrderTraversal(const core::StreamingPipeline* pStreamingPipeline,
                std::vector<operators::StreamingOperator*>::size_type numOperators);

        ConcurrentUnorderedMapTBB<const operators::StreamingOperator*, core::StreamingPipeline*> pipelineByOpPtrMap_;
        ConcurrentUnorderedMapTBB<const operators::StreamingOperator*, std::atomic<int64_t>> schedCycleForOp_;
        std::atomic<int64_t> currentSchedCycle_;
        ConcurrentUnorderedMapTBB<const core::StreamingPipeline*, std::atomic<bool>> pipelineActivatedMap_;
        ConcurrentUnorderedMapTBB<const operators::StreamingOperator*, std::atomic<float>> postOrderPriorityMap_;
        std::atomic<uint64_t> numActiveOperators_{0};
        constexpr static float kPriorityRange_ = 10.0f;
    };

}// namespace enjima::runtime

#endif//ENJIMA_MIN_COST_PRIORITY_CALCULATOR_H
