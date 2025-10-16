//
// Created by m34ferna on 18/06/24.
//

#ifndef ENJIMA_PREEMPTIVE_MODE_H
#define ENJIMA_PREEMPTIVE_MODE_H

#include "SchedulingPreemptMode.h"

namespace enjima::runtime {

    class PreemptiveMode : public SchedulingPreemptMode {
    public:
        long CalculateNumEventsToProcess(operators::StreamingOperator*& opPtr) const override;
    };

}// namespace enjima::runtime


#endif//ENJIMA_PREEMPTIVE_MODE_H
