//
// Created by m34ferna on 09/01/24.
//

#ifndef ENJIMA_PROFILER_H
#define ENJIMA_PROFILER_H

#include "PeriodicMetricsLogger.h"
#include "SystemMetricsProfiler.h"
#include "enjima/common/TypeAliases.h"
#include "enjima/metrics/types/Counter.h"
#include "enjima/metrics/types/CpuGauge.h"
#include "enjima/metrics/types/DoubleAverageGauge.h"
#include "enjima/metrics/types/Gauge.h"
#include "enjima/metrics/types/Histogram.h"
#include "enjima/metrics/types/LatencyGradientGauge.h"
#include "enjima/metrics/types/OperatorCostGauge.h"
#include "enjima/metrics/types/OperatorSelectivityGauge.h"
#include "enjima/metrics/types/PendingInputEventsGauge.h"
#include "enjima/metrics/types/ThroughputGauge.h"

#include <string>
#include <unordered_map>

namespace enjima::metrics {

    class Profiler {
    public:
        explicit Profiler(uint64_t loggingPeriodMs, bool systemMetricsEnabled, std::string systemIdString);
        Profiler(const Profiler&) = delete;
        Profiler& operator=(const Profiler&) = delete;
        ~Profiler();

        Counter<uint64_t>* GetOrCreateCounter(const std::string& metricName);
        DoubleAverageGauge* GetOrCreateDoubleAverageGauge(const std::string& metricName);
        ThroughputGauge* GetOrCreateThroughputGauge(const std::string& metricName, const Counter<uint64_t>* counterPtr);
        OperatorCostGauge* GetOrCreateOperatorCostGauge(const std::string& opName);
        OperatorSelectivityGauge* GetOrCreateOperatorSelectivityGauge(const std::string& opName,
                const Counter<uint64_t>* inCounterPtr, const Counter<uint64_t>* outCounterPtr);
        PendingInputEventsGauge* GetOrCreatePendingEventsGauge(const std::string& opName,
                std::vector<Counter<uint64_t>*> prevOpOutCounterPtrs, const Counter<uint64_t>* currentOpInCounterPtr);
        Histogram<uint64_t>* GetOrCreateHistogram(const std::string& metricName, uint16_t histogramSize);
        LatencyGradientGauge* GetOrCreateLatencyGradientGauge(const std::string& metricName,
                Histogram<uint64_t>* histPtr);
#if ENJIMA_METRICS_LEVEL >= 2
        void CreateOutputCostTracking(const std::string& metricName, std::atomic<float>* outputCostPtr);
#endif


        Counter<uint64_t>* GetCounter(const std::string& metricName);
        [[nodiscard]] Counter<uint64_t>* GetInCounter(const std::string& operatorName) const;
        [[nodiscard]] Counter<uint64_t>* GetOutCounter(const std::string& operatorName) const;
        [[nodiscard]] ThroughputGauge* GetInThroughputGauge(const std::string& operatorName) const;
        [[nodiscard]] Histogram<uint64_t>* GetLatencyHistogram(const std::string& operatorName) const;
        [[nodiscard]] PendingInputEventsGauge* GetPendingEventsGauge(const std::string& operatorName) const;
        [[nodiscard]] OperatorSelectivityGauge* GetOperatorSelectivityGauge(const std::string& operatorName) const;
        [[nodiscard]] OperatorCostGauge* GetOperatorCostGauge(const std::string& operatorName) const;
        [[nodiscard]] DoubleAverageGauge* GetDoubleAverageGauge(const std::string& metricName) const;
        [[nodiscard]] LatencyGradientGauge* GetLatencyGradientGauge(const std::string& metricName) const;
        [[nodiscard]] CpuGauge* GetProcessCpuGauge() const;
        [[nodiscard]] Gauge<uint32_t>* GetNumAvailableCpusGauge() const;
        [[nodiscard]] pid_t GetMetricsLoggerThreadId() const;
        [[nodiscard]] uint32_t GetNumStateBasedWorkers() const;

        [[nodiscard]] ConcurrentUnorderedMapTBB<std::string, Counter<uint64_t>*> GetCounterMap() const;
        [[nodiscard]] ConcurrentUnorderedMapTBB<std::string, Gauge<uint64_t>*> GetUnsignedLongGaugeMap() const;
        [[nodiscard]] ConcurrentUnorderedMapTBB<std::string, Histogram<uint64_t>*> GetHistogramMap() const;
        [[nodiscard]] ConcurrentUnorderedMapTBB<std::string, LatencyGradientGauge*> GetLatencyGradientGaugeMap() const;
        [[nodiscard]] ConcurrentUnorderedMapTBB<std::string, std::atomic<float>*> GetNormalizedOutputCostMap() const;

        [[nodiscard]] ConcurrentUnorderedMapTBB<std::string, DoubleAverageGauge*> GetDoubleAverageGaugeMap() const;
        [[nodiscard]] ConcurrentUnorderedMapTBB<std::string, ThroughputGauge*> GetThroughputGaugeMap() const;
        [[nodiscard]] ConcurrentUnorderedMapTBB<std::string, OperatorCostGauge*> GetCostGaugeMap() const;
        [[nodiscard]] ConcurrentUnorderedMapTBB<std::string, OperatorSelectivityGauge*> GetSelectivityGaugeMap() const;
        [[nodiscard]] ConcurrentUnorderedMapTBB<std::string, PendingInputEventsGauge*> GetPendingEventsGaugeMap() const;

        [[nodiscard]] ConcurrentUnorderedMapTBB<std::string, SystemGauge<long>*> GetSystemGaugeLongMap() const;
        [[nodiscard]] ConcurrentUnorderedMapTBB<std::string, SystemGauge<unsigned long>*>
        GetSystemGaugeUnsignedLongMap() const;
        [[nodiscard]] ConcurrentUnorderedMapTBB<std::string, SystemGauge<double>*> GetSystemGaugeDoubleMap() const;

        void UpdateNumAvailableCpus(uint32_t numAvailableCpus);
        void SetTotalNumSBWorkers(uint32_t numStateBasedWorkers);

        void UpdateSystemMetrics();
        void StartMetricsLogger();
        void ShutdownMetricsLogger();
        void FlushMetricsLogger();
        void SetInternalMetricsUpdateIntervalMs(uint64_t internalMetricsUpdateIntervalMs);

    private:
        PeriodicMetricsLogger* pMetricsLogger_;
        SystemMetricsProfiler* pSysProfiler_;
        uint64_t internalMetricsUpdateIntervalMs_{0};

        ConcurrentUnorderedMapTBB<std::string, Counter<uint64_t>*> uLongCounterMap_;
        ConcurrentUnorderedMapTBB<std::string, Gauge<uint64_t>*> uLongGaugeMap_;
        ConcurrentUnorderedMapTBB<std::string, Histogram<uint64_t>*> uLongHistogramMap_;
        ConcurrentUnorderedMapTBB<std::string, LatencyGradientGauge*> latencyGradientGaugeMap_;
        ConcurrentUnorderedMapTBB<std::string, std::atomic<float>*> outputCostMap_;

        ConcurrentUnorderedMapTBB<std::string, ThroughputGauge*> throughputGaugeMap_;
        ConcurrentUnorderedMapTBB<std::string, OperatorCostGauge*> costGaugeMap_;
        ConcurrentUnorderedMapTBB<std::string, OperatorSelectivityGauge*> selectivityGaugeMap_;
        ConcurrentUnorderedMapTBB<std::string, PendingInputEventsGauge*> pendingEventsGaugeMap_;
        ConcurrentUnorderedMapTBB<std::string, DoubleAverageGauge*> doubleAvgGaugeMap_;

        CpuGauge* cpuGaugePtr_;
        Gauge<uint32_t>* numAvailableCpusGaugePtr_;
        uint32_t numSBWorkers_{0};
    };

}// namespace enjima::metrics

#endif// ENJIMA_PROFILER_H
