//
// Created by m34ferna on 10/02/24.
//

#ifndef ENJIMA_METRIC_NAMES_H
#define ENJIMA_METRIC_NAMES_H

namespace enjima::metrics {

#if ENJIMA_METRICS_LEVEL >= 2
    inline constexpr auto kSchedulingTimeCounterLabel = "schedTime_counter";
    inline constexpr auto kSchedulingCountCounterLabel = "schedCount_counter";
    inline constexpr auto kPriorityUpdateTimeCounterLabel = "priorityUpdateTime_counter";
    inline constexpr auto kPriorityUpdateCountCounterLabel = "priorityUpdateCount_counter";
    inline constexpr auto kSchedVecLockTimeGaugeLabel = "schedVecLockTime_gauge";
    inline constexpr auto kSchedTotCalcTimeGaugeLabel = "schedTotCalcTime_gauge";
    inline constexpr auto kSchedPriorityUpdateTimeGaugeLabel = "schedPriorityUpdateTime_gauge";
#endif

#if ENJIMA_METRICS_LEVEL >= 3
    inline constexpr auto kGapCounterSuffix = "_scheduleGap_counter";
    inline constexpr auto kGapTimeCounterSuffix = "_scheduleGapTime_counter";
    inline constexpr auto kBatchSizeAverageGaugeSuffix = "_batchSizeAvg_gauge";
#endif

    inline constexpr auto kInCounterSuffix = "_in_counter";
    inline constexpr auto kLeftInCounterSuffix = "_leftIn_counter";
    inline constexpr auto kRightInCounterSuffix = "_rightIn_counter";
    inline constexpr auto kOutCounterSuffix = "_out_counter";
    inline constexpr auto kRightOutCounterSuffix = "_rightOut_counter";
    inline constexpr auto kOutSuffix = "_out";
    inline constexpr auto kCounterSuffix = "_counter";
    inline constexpr auto kInThroughputGaugeSuffix = "_inThroughput_gauge";
    inline constexpr auto kOutThroughputGaugeSuffix = "_outThroughput_gauge";
    inline constexpr auto kLatencyHistogramSuffix = "_latency_histogram";
    inline constexpr auto kLatencyGradGaugeSuffix = "_latencyGrad_gauge";
    inline constexpr auto kOutputCostGaugeSuffix = "_outputCost_gauge";
    inline constexpr auto kPendingEventsGaugeSuffix = "_pendingEvents_gauge";
    inline constexpr auto kOperatorCostGaugeSuffix = "_cost_gauge";
    inline constexpr auto kOperatorScheduledTimeGaugeSuffix = "_scheduledTime_gauge";
    inline constexpr auto kOperatorScheduledCountGaugeSuffix = "_scheduledCount_gauge";
    inline constexpr auto kOperatorSelectivityGaugeSuffix = "_selectivity_gauge";
}// namespace enjima::metrics

#endif//ENJIMA_METRIC_NAMES_H
