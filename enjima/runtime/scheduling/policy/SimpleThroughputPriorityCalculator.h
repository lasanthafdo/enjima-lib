//
// Created by m34ferna on 07/06/24.
//

#ifndef ENJIMA_SIMPLE_THROUGHPUT_PRIORITY_CALCULATOR_H
#define ENJIMA_SIMPLE_THROUGHPUT_PRIORITY_CALCULATOR_H

#include "MetricsBasedPriorityCalculator.h"

namespace enjima::runtime {

    class SimpleThroughputPriorityCalculator : public MetricsBasedPriorityCalculator {
    public:
        SimpleThroughputPriorityCalculator();
        float CalculatePriority(const OperatorContext* opCtxtPtr) override;
        void UpdateState() override;
        void InitializeMetricsForPipeline(core::StreamingPipeline* pStreamingPipeline,
                const std::vector<OperatorContext*>& opCtxtPtrsVec) override;
        void DeactivateMetricsForPipeline(core::StreamingPipeline* pStreamingPipeline) override;

    private:
        ConcurrentUnorderedMapTBB<const operators::StreamingOperator*, size_t> numDownstreamOpsMap_;
    };

}// namespace enjima::runtime


#endif//ENJIMA_SIMPLE_THROUGHPUT_PRIORITY_CALCULATOR_H
