//
// Created by m34ferna on 18/06/24.
//

#include "PreemptiveMode.h"

namespace enjima::runtime {
    long PreemptiveMode::CalculateNumEventsToProcess(operators::StreamingOperator*& opPtr) const
    {
        if (opPtr == nullptr) {
            return 0L;
        }
        return std::numeric_limits<long>::max();
    }
}// namespace enjima::runtime
