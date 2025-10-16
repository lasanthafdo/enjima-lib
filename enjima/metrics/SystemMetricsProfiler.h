//
// Created by m34ferna on 06/05/24.
//

#ifndef ENJIMA_SYSTEM_METRICS_PROFILER_H
#define ENJIMA_SYSTEM_METRICS_PROFILER_H

#include "enjima/common/TypeAliases.h"
#include "enjima/metrics/types/SystemGauge.h"
#include <string>

namespace enjima::metrics {

    class SystemMetricsProfiler {
    public:
        SystemMetricsProfiler();
        ~SystemMetricsProfiler();
        void Init();
        void UpdateSystemMetrics();
        const ConcurrentUnorderedMapTBB<std::string, SystemGauge<long>*>& GetSystemGaugeLongMap();
        const ConcurrentUnorderedMapTBB<std::string, SystemGauge<unsigned long>*>& GetSystemGaugeUnsignedLongMap();
        const ConcurrentUnorderedMapTBB<std::string, SystemGauge<double>*>& GetSystemGaugeDoubleMap();

    private:
        bool AddSystemGauge(const std::string& metricName, const std::string& processName, pid_t processID,
                long* pMetricVal);
        bool AddSystemGauge(const std::string& metricName, const std::string& processName, pid_t processID,
                unsigned long* pMetricVal);
        bool AddSystemGauge(const std::string& metricName, const std::string& processName, pid_t processID,
                double* pMetricVal);

        pid_t enjimaPID_{};
        std::string procFileStream_{};
        ConcurrentUnorderedMapTBB<std::string, SystemGauge<long>*> systemGaugeLongMap_;
        ConcurrentUnorderedMapTBB<std::string, SystemGauge<unsigned long>*> systemGaugeUnsignedLongMap_;
        ConcurrentUnorderedMapTBB<std::string, SystemGauge<double>*> systemGaugeDoubleMap_;
        unsigned long minFlt_{0l};
        unsigned long cMinFlt_{0l};
        unsigned long majFlt_{0l};
        unsigned long cMajFlt_{0l};
        unsigned long uTimeMs_{0l};
        unsigned long sTimeMs_{0l};
        unsigned long cUTimeMs_{0l};
        unsigned long cSTimeMs_{0l};
        long numThreads_{0l};
        unsigned long vmUsageKB_{0l};
        long residentSetSizeKB_{0l};
        unsigned long startData_{0l};
        unsigned long endData_{0l};
        unsigned long startBrk_{0l};
        long processor_{0l};
        double cpuPercent_{0.0};
        unsigned int numProcessors_{0};
        uint64_t lastUpdatedAt_{0};
    };

}// namespace enjima::metrics


#endif//ENJIMA_SYSTEM_METRICS_PROFILER_H
