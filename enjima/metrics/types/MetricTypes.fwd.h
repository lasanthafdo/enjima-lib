//
// Created by m34ferna on 28/06/24.
//

#ifndef ENJIMA_METRIC_TYPES_FWD_H
#define ENJIMA_METRIC_TYPES_FWD_H

#include <concepts>

namespace enjima::metrics {
    template<std::unsigned_integral T>
    class Counter;
    class PendingInputEventsGauge;
    class ThroughputGauge;
    class OperatorCostGauge;
    class OperatorSelectivityGauge;
    class LatencyGradientGauge;
    class Profiler;
}// namespace enjima::metrics

#endif//ENJIMA_METRIC_TYPES_FWD_H
