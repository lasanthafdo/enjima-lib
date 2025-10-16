//
// Created by m34ferna on 18/06/24.
//

#ifndef ENJIMA_SCHEDULING_PREEMPT_MODE_H
#define ENJIMA_SCHEDULING_PREEMPT_MODE_H

#include "enjima/operators/StreamingOperator.h"

namespace enjima::runtime {

    class SchedulingPreemptMode {
    public:
        virtual ~SchedulingPreemptMode() = default;
        virtual long CalculateNumEventsToProcess(operators::StreamingOperator*& opPtr) const = 0;
    };

    template<typename T>
    concept PreemptModeType = std::is_base_of_v<SchedulingPreemptMode, T>;

}// namespace enjima::runtime

#endif//ENJIMA_SCHEDULING_PREEMPT_MODE_H
