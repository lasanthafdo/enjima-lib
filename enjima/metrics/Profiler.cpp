//
// Created by m34ferna on 17/03/24.
//

#include "Profiler.h"
#include "MetricNames.h"
#include "enjima/metrics/types/OperatorCpuTimeGauge.h"
#include "enjima/metrics/types/OperatorScheduledCountGauge.h"

namespace enjima::metrics {
    Profiler::Profiler(uint64_t loggingPeriodMs, bool systemMetricsEnabled, std::string systemIdString)
    {
        if (loggingPeriodMs == 0) {
            pMetricsLogger_ = nullptr;
        }
        else {
            pMetricsLogger_ =
                    new PeriodicMetricsLogger(loggingPeriodMs, this, systemMetricsEnabled, std::move(systemIdString));
        }
        if (systemMetricsEnabled) {
            pSysProfiler_ = new SystemMetricsProfiler;
        }
        else {
            pSysProfiler_ = nullptr;
        }
        cpuGaugePtr_ = new CpuGauge;
        numAvailableCpusGaugePtr_ = new Gauge<uint32_t>(0);
        numAvailableCpusGaugePtr_->UpdateVal(0);
    }


    Profiler::~Profiler()
    {
        delete pMetricsLogger_;
        delete pSysProfiler_;
        for (const auto& counterPair: uLongCounterMap_) {
            delete counterPair.second;
        }
        for (const auto& gaugePair: uLongGaugeMap_) {
            delete gaugePair.second;
        }
        for (const auto& latHistogramPair: uLongHistogramMap_) {
            delete latHistogramPair.second;
        }
        for (const auto& latGradGaugePair: latencyGradientGaugeMap_) {
            delete latGradGaugePair.second;
        }
        for (const auto& tpGaugePair: throughputGaugeMap_) {
            delete tpGaugePair.second;
        }
        for (const auto& gaugePair: selectivityGaugeMap_) {
            delete gaugePair.second;
        }
        for (const auto& gaugePair: costGaugeMap_) {
            delete gaugePair.second;
        }
        for (const auto& gaugePair: pendingEventsGaugeMap_) {
            delete gaugePair.second;
        }
        for (const auto& gaugePair: doubleAvgGaugeMap_) {
            delete gaugePair.second;
        }
        delete cpuGaugePtr_;
        delete numAvailableCpusGaugePtr_;
    }

    ThroughputGauge* Profiler::GetOrCreateThroughputGauge(const std::string& metricName,
            const Counter<uint64_t>* counterPtr)
    {
        if (throughputGaugeMap_.contains(metricName)) {
            return throughputGaugeMap_.at(metricName);
        }
        auto pTpGauge = new ThroughputGauge(counterPtr);
        throughputGaugeMap_.emplace(metricName, pTpGauge);
        return pTpGauge;
    }

    ConcurrentUnorderedMapTBB<std::string, Counter<uint64_t>*> Profiler::GetCounterMap() const
    {
        return uLongCounterMap_;
    }

    ConcurrentUnorderedMapTBB<std::string, DoubleAverageGauge*> Profiler::GetDoubleAverageGaugeMap() const
    {
        return doubleAvgGaugeMap_;
    }

    ConcurrentUnorderedMapTBB<std::string, ThroughputGauge*> Profiler::GetThroughputGaugeMap() const
    {
        return throughputGaugeMap_;
    }

    ConcurrentUnorderedMapTBB<std::string, Histogram<uint64_t>*> Profiler::GetHistogramMap() const
    {
        return uLongHistogramMap_;
    }

    ConcurrentUnorderedMapTBB<std::string, LatencyGradientGauge*> Profiler::GetLatencyGradientGaugeMap() const
    {
        return latencyGradientGaugeMap_;
    }

    ConcurrentUnorderedMapTBB<std::string, std::atomic<float>*> Profiler::GetNormalizedOutputCostMap() const
    {
        return outputCostMap_;
    }

    void Profiler::StartMetricsLogger()
    {
        if (pSysProfiler_ != nullptr) {
            pSysProfiler_->Init();
        }
        pMetricsLogger_->Start();
    }

    Counter<uint64_t>* Profiler::GetOrCreateCounter(const std::string& metricName)
    {
        if (uLongCounterMap_.contains(metricName)) {
            return uLongCounterMap_.at(metricName);
        }
        auto pCounter = new Counter<uint64_t>;
        uLongCounterMap_.emplace(metricName, pCounter);
        return pCounter;
    }

    Histogram<uint64_t>* Profiler::GetOrCreateHistogram(const std::string& metricName, uint16_t histogramSize)
    {
        if (uLongHistogramMap_.contains(metricName)) {
            return uLongHistogramMap_.at(metricName);
        }
        auto pHistogram = new Histogram<uint64_t>(histogramSize);
        uLongHistogramMap_.emplace(metricName, pHistogram);
        return pHistogram;
    }

    LatencyGradientGauge* Profiler::GetOrCreateLatencyGradientGauge(const std::string& metricName,
            Histogram<uint64_t>* histPtr)
    {
        if (latencyGradientGaugeMap_.contains(metricName)) {
            return latencyGradientGaugeMap_.at(metricName);
        }
        auto pLatGradGauge = new LatencyGradientGauge(histPtr);
        pLatGradGauge->SetUpdateIntervalMillis(internalMetricsUpdateIntervalMs_);
        latencyGradientGaugeMap_.emplace(metricName, pLatGradGauge);
        return pLatGradGauge;
    }

#if ENJIMA_METRICS_LEVEL >= 2
    void Profiler::CreateOutputCostTracking(const std::string& metricName, std::atomic<float>* outputCostPtr)
    {
        if (!outputCostMap_.contains(metricName)) {
            outputCostMap_.emplace(metricName, outputCostPtr);
        }
    }
#endif

    void Profiler::ShutdownMetricsLogger()
    {
        pMetricsLogger_->Shutdown();
    }

    void Profiler::FlushMetricsLogger()
    {
        pMetricsLogger_->FlushMetricsToFile();
    }

    ConcurrentUnorderedMapTBB<std::string, SystemGauge<long>*> Profiler::GetSystemGaugeLongMap() const
    {
        return pSysProfiler_->GetSystemGaugeLongMap();
    }

    ConcurrentUnorderedMapTBB<std::string, SystemGauge<unsigned long>*> Profiler::GetSystemGaugeUnsignedLongMap() const
    {
        return pSysProfiler_->GetSystemGaugeUnsignedLongMap();
    }

    ConcurrentUnorderedMapTBB<std::string, SystemGauge<double>*> Profiler::GetSystemGaugeDoubleMap() const
    {
        return pSysProfiler_->GetSystemGaugeDoubleMap();
    }

    void Profiler::UpdateSystemMetrics()
    {
        pSysProfiler_->UpdateSystemMetrics();
    }

    Counter<uint64_t>* Profiler::GetCounter(const std::string& metricName)
    {
        if (uLongCounterMap_.contains(metricName)) {
            return uLongCounterMap_.at(metricName);
        }
        return nullptr;
    }

    OperatorCostGauge* Profiler::GetOrCreateOperatorCostGauge(const std::string& opName)
    {
        auto costMetricName = std::string(opName).append(kOperatorCostGaugeSuffix);
        if (costGaugeMap_.contains(costMetricName)) {
            return costGaugeMap_.at(costMetricName);
        }
        auto pCostGauge = new OperatorCostGauge(internalMetricsUpdateIntervalMs_);
        costGaugeMap_.emplace(costMetricName, pCostGauge);

        auto schedTimeMetricName = std::string(opName).append(kOperatorScheduledTimeGaugeSuffix);
        auto pSchedTimeGauge = new OperatorCpuTimeGauge{pCostGauge};
        uLongGaugeMap_.emplace(schedTimeMetricName, pSchedTimeGauge);

        auto schedCountMetricName = std::string(opName).append(kOperatorScheduledCountGaugeSuffix);
        auto pSchedCountGauge = new OperatorScheduledCountGauge{pCostGauge};
        uLongGaugeMap_.emplace(schedCountMetricName, pSchedCountGauge);

        return pCostGauge;
    }

    OperatorSelectivityGauge* Profiler::GetOrCreateOperatorSelectivityGauge(const std::string& opName,
            const Counter<uint64_t>* inCounterPtr, const Counter<uint64_t>* outCounterPtr)
    {
        auto metricName = std::string(opName).append(kOperatorSelectivityGaugeSuffix);
        if (selectivityGaugeMap_.contains(metricName)) {
            return selectivityGaugeMap_.at(metricName);
        }
        auto pSelGauge = new OperatorSelectivityGauge(inCounterPtr, outCounterPtr, internalMetricsUpdateIntervalMs_);
        selectivityGaugeMap_.emplace(metricName, pSelGauge);
        return pSelGauge;
    }

    PendingInputEventsGauge* Profiler::GetOrCreatePendingEventsGauge(const std::string& opName,
            std::vector<Counter<uint64_t>*> prevOpOutCounterPtrs, const Counter<uint64_t>* currentOpInCounterPtr)
    {
        auto metricName = std::string(opName).append(kPendingEventsGaugeSuffix);
        if (pendingEventsGaugeMap_.contains(metricName)) {
            return pendingEventsGaugeMap_.at(metricName);
        }
        auto pPendingEventsGauge = new PendingInputEventsGauge(prevOpOutCounterPtrs, currentOpInCounterPtr);
        pendingEventsGaugeMap_.emplace(metricName, pPendingEventsGauge);
        return pPendingEventsGauge;
    }

    DoubleAverageGauge* Profiler::GetOrCreateDoubleAverageGauge(const std::string& metricName)
    {
        if (doubleAvgGaugeMap_.contains(metricName)) {
            return doubleAvgGaugeMap_.at(metricName);
        }
        auto pDoubleAvgGauge = new DoubleAverageGauge(0.0);
        doubleAvgGaugeMap_.emplace(metricName, pDoubleAvgGauge);
        return pDoubleAvgGauge;
    }

    DoubleAverageGauge* Profiler::GetDoubleAverageGauge(const std::string& metricName) const
    {
        if (doubleAvgGaugeMap_.contains(metricName)) {
            return doubleAvgGaugeMap_.at(metricName);
        }
        return nullptr;
    }

    ConcurrentUnorderedMapTBB<std::string, OperatorCostGauge*> Profiler::GetCostGaugeMap() const
    {
        return costGaugeMap_;
    }

    ConcurrentUnorderedMapTBB<std::string, OperatorSelectivityGauge*> Profiler::GetSelectivityGaugeMap() const
    {
        return selectivityGaugeMap_;
    }

    ConcurrentUnorderedMapTBB<std::string, PendingInputEventsGauge*> Profiler::GetPendingEventsGaugeMap() const
    {
        return pendingEventsGaugeMap_;
    }

    Counter<uint64_t>* Profiler::GetInCounter(const std::string& operatorName) const
    {
        auto metricName = std::string(operatorName).append(kInCounterSuffix);
        if (uLongCounterMap_.contains(metricName)) {
            return uLongCounterMap_.at(metricName);
        }
        return nullptr;
    }

    Counter<uint64_t>* Profiler::GetOutCounter(const std::string& operatorName) const
    {
        auto metricName = std::string(operatorName).append(kOutCounterSuffix);
        if (uLongCounterMap_.contains(metricName)) {
            return uLongCounterMap_.at(metricName);
        }
        return nullptr;
    }

    ThroughputGauge* Profiler::GetInThroughputGauge(const std::string& operatorName) const
    {
        auto metricName = std::string(operatorName).append(kInThroughputGaugeSuffix);
        if (throughputGaugeMap_.contains(metricName)) {
            return throughputGaugeMap_.at(metricName);
        }
        return nullptr;
    }

    Histogram<uint64_t>* Profiler::GetLatencyHistogram(const std::string& operatorName) const
    {
        auto metricName = std::string(operatorName).append(kLatencyHistogramSuffix);
        if (uLongHistogramMap_.contains(metricName)) {
            return uLongHistogramMap_.at(metricName);
        }
        return nullptr;
    }

    PendingInputEventsGauge* Profiler::GetPendingEventsGauge(const std::string& operatorName) const
    {
        auto metricName = std::string(operatorName).append(kPendingEventsGaugeSuffix);
        if (pendingEventsGaugeMap_.contains(metricName)) {
            return pendingEventsGaugeMap_.at(metricName);
        }
        return nullptr;
    }

    OperatorSelectivityGauge* Profiler::GetOperatorSelectivityGauge(const std::string& operatorName) const
    {
        auto metricName = std::string(operatorName).append(kOperatorSelectivityGaugeSuffix);
        if (selectivityGaugeMap_.contains(metricName)) {
            return selectivityGaugeMap_.at(metricName);
        }
        return nullptr;
    }

    OperatorCostGauge* Profiler::GetOperatorCostGauge(const std::string& operatorName) const
    {
        auto metricName = std::string(operatorName).append(kOperatorCostGaugeSuffix);
        if (costGaugeMap_.contains(metricName)) {
            return costGaugeMap_.at(metricName);
        }
        return nullptr;
    }

    LatencyGradientGauge* Profiler::GetLatencyGradientGauge(const std::string& metricName) const
    {
        if (latencyGradientGaugeMap_.contains(metricName)) {
            return latencyGradientGaugeMap_.at(metricName);
        }
        return nullptr;
    }

    CpuGauge* Profiler::GetProcessCpuGauge() const
    {
        return cpuGaugePtr_;
    }

    ConcurrentUnorderedMapTBB<std::string, Gauge<uint64_t>*> Profiler::GetUnsignedLongGaugeMap() const
    {
        return uLongGaugeMap_;
    }

    void Profiler::UpdateNumAvailableCpus(uint32_t numAvailableCpus)
    {
        numAvailableCpusGaugePtr_->UpdateVal(numAvailableCpus);
    }

    void Profiler::SetTotalNumSBWorkers(uint32_t numStateBasedWorkers)
    {
        numSBWorkers_ = numStateBasedWorkers;
    }

    Gauge<uint32_t>* Profiler::GetNumAvailableCpusGauge() const
    {
        return numAvailableCpusGaugePtr_;
    }

    void Profiler::SetInternalMetricsUpdateIntervalMs(uint64_t internalMetricsUpdateIntervalMs)
    {
        internalMetricsUpdateIntervalMs_ = internalMetricsUpdateIntervalMs;
        cpuGaugePtr_->SetUpdateIntervalMicros(internalMetricsUpdateIntervalMs * 1'000);
    }

    pid_t Profiler::GetMetricsLoggerThreadId() const
    {
        return pMetricsLogger_->GetThreadId();
    }

    uint32_t Profiler::GetNumStateBasedWorkers() const
    {
        return numSBWorkers_;
    }

}// namespace enjima::metrics