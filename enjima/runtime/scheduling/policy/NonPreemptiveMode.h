//
// Created by m34ferna on 18/06/24.
//

#ifndef ENJIMA_NON_PREEMPTIVE_MODE_H
#define ENJIMA_NON_PREEMPTIVE_MODE_H

#include "SchedulingPreemptMode.h"
#include "enjima/runtime/scheduling/SchedulingTypes.h"

namespace enjima::runtime {

    class NonPreemptiveMode : public SchedulingPreemptMode {
    public:
        NonPreemptiveMode(MetricsMapT* metricsMapPtr, std::atomic<uint64_t>* nextSchedulingUpdateAtMicrosPtr);
        long CalculateNumEventsToProcess(operators::StreamingOperator*& opPtr) const override;

    private:
        MetricsMapT* metricsMapPtr_;
        std::atomic<uint64_t>* nextSchedulingUpdateAtMicrosPtr_;
    };

}// namespace enjima::runtime


#endif//ENJIMA_NON_PREEMPTIVE_MODE_H
