//
// Created by m34ferna on 04/06/24.
//

#ifndef ENJIMA_SCHEDULING_TYPES_H
#define ENJIMA_SCHEDULING_TYPES_H

#include "enjima/metrics/types/MetricTypes.fwd.h"
#include "enjima/operators/OperatorsInternals.fwd.h"
#include "oneapi/tbb/concurrent_unordered_map.h"

#include <cstdint>
#include <tuple>

namespace enjima::runtime {
    using SchedulingMetricTupleT = std::tuple<metrics::PendingInputEventsGauge*, metrics::ThroughputGauge*,
            metrics::OperatorCostGauge*, metrics::OperatorSelectivityGauge*, metrics::Counter<uint64_t>*>;
    using MetricsMapT =
            oneapi::tbb::concurrent_unordered_map<const operators::StreamingOperator*, SchedulingMetricTupleT>;
    enum class PreemptMode { kUnassigned, kPreemptive, kNonPreemptive };
    enum class SchedulingMode { kThreadBased, kStateBasedPriority };
    enum class PriorityType {
        kUnassigned,
        kInputQueueSize,
        kAdaptive,
        kLatencyOptimized,
        kSPLatencyOptimized,
        kHighestRate,
        kMinLatency,
        kMinCost,
        kThroughputOptimized,
        kSimpleThroughput,
        kLeastRecent,
        kRoundRobin,
        kFirstComeFirstServed
    };
}// namespace enjima::runtime


#endif//ENJIMA_SCHEDULING_TYPES_H
