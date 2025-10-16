//
// Created by m34ferna on 26/06/24.
//

#ifndef ENJIMA_NON_PREEMPTIVE_LATENCY_OPTIMIZED_MODE_H
#define ENJIMA_NON_PREEMPTIVE_LATENCY_OPTIMIZED_MODE_H

#include "SchedulingPreemptMode.h"

namespace enjima::runtime {

    class NonPreemptiveBasicMode : public SchedulingPreemptMode {
    public:
        long CalculateNumEventsToProcess(operators::StreamingOperator*& opPtr) const override;
    };

}// namespace enjima::runtime


#endif//ENJIMA_NON_PREEMPTIVE_LATENCY_OPTIMIZED_MODE_H
