//
// Created by m34ferna on 26/06/24.
//

#ifndef ENJIMA_NON_PREEMPTIVE_THROUGHPUT_OPTIMIZED_MODE_H
#define ENJIMA_NON_PREEMPTIVE_THROUGHPUT_OPTIMIZED_MODE_H

#include "SchedulingPreemptMode.h"
#include "enjima/runtime/scheduling/SchedulingTypes.h"

namespace enjima::runtime {

    class NonPreemptiveThroughputOptimizedMode : public SchedulingPreemptMode {
    public:
        NonPreemptiveThroughputOptimizedMode(MetricsMapT* metricsMapPtr, size_t defaultBlockSize);
        long CalculateNumEventsToProcess(operators::StreamingOperator*& opPtr) const override;

    private:
        MetricsMapT* metricsMapPtr_;
        size_t defaultBlockSize_;
    };

}// namespace enjima::runtime


#endif//ENJIMA_NON_PREEMPTIVE_THROUGHPUT_OPTIMIZED_MODE_H
