//
// Created by m34ferna on 26/06/24.
//

#include "NonPreemptiveBasicMode.h"

namespace enjima::runtime {
    long NonPreemptiveBasicMode::CalculateNumEventsToProcess(operators::StreamingOperator*& opPtr) const
    {
        if (opPtr != nullptr) {
            return 1L;
        }
        return 0L;
    }
}// namespace enjima::runtime
