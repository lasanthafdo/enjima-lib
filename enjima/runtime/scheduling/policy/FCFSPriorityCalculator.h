//
// Created by m34ferna on 07/06/24.
//

#ifndef ENJIMA_FCFS_PRIORITY_CALCULATOR_H
#define ENJIMA_FCFS_PRIORITY_CALCULATOR_H

#include "MetricsBasedPriorityCalculator.h"
#include "enjima/runtime/scheduling/policy/SchedulingPolicy.h"

namespace enjima::runtime {

    class FCFSPriorityCalculator : public MetricsBasedPriorityCalculator {
    public:
        explicit FCFSPriorityCalculator(MetricsMapT* metricsMapPtr);
        float CalculatePriority(const OperatorContext* opCtxtPtr) override;
        void UpdateState() override;
        void InitializeMetricsForPipeline(core::StreamingPipeline* pStreamingPipeline,
                const std::vector<OperatorContext*>& opCtxtPtrsVec) override;
        void DeactivateMetricsForPipeline(core::StreamingPipeline* pStreamingPipeline) override;
        void NotifyScheduleCompletion(const OperatorContext* opCtxtPtr);
        [[nodiscard]] bool IsEligibleForScheduling(uint64_t numPendingEvents, const OperatorContext* opCtxtPtr,
                uint8_t lastOperatorStatus) const override;

    private:
        std::atomic<uint64_t> nextUpperThresholdMicros_;
        ConcurrentUnorderedMapTBB<const operators::StreamingOperator*, core::StreamingPipeline*> pipelineByOpPtrMap_;
        ConcurrentUnorderedMapTBB<const operators::StreamingOperator*, std::atomic<uint64_t>> lastEligibleAt_;
        ConcurrentUnorderedMapTBB<const core::StreamingPipeline*, std::atomic<bool>> pipelineActivatedMap_;
        ConcurrentUnorderedMapTBB<const operators::StreamingOperator*, const std::vector<operators::StreamingOperator*>>
                downstreamOpVecsMap_;
    };

}// namespace enjima::runtime


#endif//ENJIMA_FCFS_PRIORITY_CALCULATOR_H
