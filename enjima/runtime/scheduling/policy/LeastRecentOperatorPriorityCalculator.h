//
// Created by m34ferna on 07/06/24.
//

#ifndef ENJIMA_LEAST_RECENT_OPERATOR_PRIORITY_CALCULATOR_H
#define ENJIMA_LEAST_RECENT_OPERATOR_PRIORITY_CALCULATOR_H

#include "SchedulingPolicy.h"

namespace enjima::runtime {

    class LeastRecentOperatorPriorityCalculator : public SchedulingPolicy {
    public:
        LeastRecentOperatorPriorityCalculator();
        float CalculatePriority(const OperatorContext* opCtxtPtr) override;
        void UpdateState() override;
    };

}// namespace enjima::runtime

#endif//ENJIMA_LEAST_RECENT_OPERATOR_PRIORITY_CALCULATOR_H
