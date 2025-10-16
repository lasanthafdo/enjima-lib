//
// Created by m34ferna on 26/06/24.
//

#include "NonPreemptiveThroughputOptimizedMode.h"

namespace enjima::runtime {

    NonPreemptiveThroughputOptimizedMode::NonPreemptiveThroughputOptimizedMode(MetricsMapT* metricsMapPtr,
            size_t defaultBlockSize)
        : metricsMapPtr_(metricsMapPtr), defaultBlockSize_(defaultBlockSize)
    {
    }

    long NonPreemptiveThroughputOptimizedMode::CalculateNumEventsToProcess(operators::StreamingOperator*& opPtr) const
    {
        if (opPtr != nullptr) {
            if (opPtr->IsSourceOperator()) {
                return static_cast<long>(defaultBlockSize_);
            }
            auto metricsTuple = metricsMapPtr_->at(opPtr);
            return static_cast<long>(std::get<0>(metricsTuple)->GetVal());
        }
        return 0;
    }
}// namespace enjima::runtime
