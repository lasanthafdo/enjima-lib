//
// Created by m34ferna on 17/03/25.
//

#include "LatencyGradientGauge.h"
#include "enjima/runtime/RuntimeUtil.h"

namespace enjima::metrics {
    static const uint64_t kUpdateIntervalCoefficient = 10;

    LatencyGradientGauge::LatencyGradientGauge(Histogram<uint64_t>* latencyHistPtr)
        : Gauge<float>(0.0f), pLatencyHistogram_(latencyHistPtr),
          lastLGASteadyClockTime_(enjima::runtime::GetSteadyClockMillis()),
          lastLGISteadyClockTime_(enjima::runtime::GetSteadyClockMillis())
    {
    }

    float LatencyGradientGauge::GetVal()
    {
        auto expectedUpdateStatus = false;
        if (calcGuardLGI.compare_exchange_strong(expectedUpdateStatus, true, std::memory_order::acq_rel)) {
            auto currentSteadyClockTime = enjima::runtime::GetSteadyClockMillis();
            auto steadyClockTimeMillis = currentSteadyClockTime - lastLGISteadyClockTime_;
            if (steadyClockTimeMillis > updateIntervalMillis_) {
                auto currentAvgLatency = static_cast<float>(pLatencyHistogram_->GetMostRecent());
                auto latencyChange = currentAvgLatency - lastInstantaneousLatency_;
                lastInstantaneousLatencyGradient_.store(latencyChange / static_cast<float>(steadyClockTimeMillis),
                        std::memory_order::release);
                lastInstantaneousLatency_ = currentAvgLatency;
                lastLGISteadyClockTime_ = currentSteadyClockTime;
            }
            calcGuardLGI.store(false, std::memory_order::release);
        }
        return lastInstantaneousLatencyGradient_.load(std::memory_order::acquire);
    }

    float LatencyGradientGauge::GetLatencyGradientOfAvg()
    {
        auto expectedUpdateStatus = false;
        if (calcGuardLGA.compare_exchange_strong(expectedUpdateStatus, true, std::memory_order::acq_rel)) {
            auto currentSteadyClockTime = enjima::runtime::GetSteadyClockMillis();
            auto steadyClockTimeMillis = currentSteadyClockTime - lastLGASteadyClockTime_;
            if (steadyClockTimeMillis > updateIntervalMillis_) {
                auto currentAvgLatency = static_cast<float>(pLatencyHistogram_->GetAverage());
                auto latencyChange = currentAvgLatency - lastAvgLatency_;
                lastLatencyGradientOfAvg_.store(latencyChange / static_cast<float>(steadyClockTimeMillis),
                        std::memory_order::release);
                lastAvgLatency_ = currentAvgLatency;
                lastLGASteadyClockTime_ = currentSteadyClockTime;
            }
            calcGuardLGA.store(false, std::memory_order::release);
        }
        return lastLatencyGradientOfAvg_.load(std::memory_order::acquire);
    }


    float LatencyGradientGauge::GetLatencyGradientWithSCTime(uint64_t scTimeMs)
    {
        auto expectedUpdateStatus = false;
        if (calcGuardLGI.compare_exchange_strong(expectedUpdateStatus, true, std::memory_order::acq_rel)) {
            auto steadyClockTimeMillis = scTimeMs - lastLGISteadyClockTime_;
            if (steadyClockTimeMillis > updateIntervalMillis_) {
                auto currentLatency = static_cast<float>(pLatencyHistogram_->GetMostRecent());
                auto latencyChange = currentLatency - lastInstantaneousLatency_;
                lastInstantaneousLatencyGradient_.store(latencyChange / static_cast<float>(steadyClockTimeMillis),
                        std::memory_order::release);
                lastInstantaneousLatency_ = currentLatency;
                lastLGISteadyClockTime_ = scTimeMs;
            }
            calcGuardLGI.store(false, std::memory_order::release);
        }
        return lastInstantaneousLatencyGradient_.load(std::memory_order::acquire);
    }

    float LatencyGradientGauge::GetLatencyGradientOfAvgWithSCTime(uint64_t scTimeMs)
    {
        auto expectedUpdateStatus = false;
        if (calcGuardLGA.compare_exchange_strong(expectedUpdateStatus, true, std::memory_order::acq_rel)) {
            auto steadyClockTimeMillis = scTimeMs - lastLGASteadyClockTime_;
            if (steadyClockTimeMillis > updateIntervalMillis_) {
                auto currentLatency = static_cast<float>(pLatencyHistogram_->GetAverage());
                auto latencyChange = currentLatency - lastAvgLatency_;
                lastLatencyGradientOfAvg_.store(latencyChange / static_cast<float>(steadyClockTimeMillis),
                        std::memory_order::release);
                lastAvgLatency_ = currentLatency;
                lastLGASteadyClockTime_ = scTimeMs;
            }
            calcGuardLGA.store(false, std::memory_order::release);
        }
        return lastLatencyGradientOfAvg_.load(std::memory_order::acquire);
    }

    void LatencyGradientGauge::SetUpdateIntervalMillis(uint64_t updateIntervalMillis)
    {
        updateIntervalMillis_ = kUpdateIntervalCoefficient * updateIntervalMillis;
    }

#if ENJIMA_METRICS_LEVEL >= 2
    float LatencyGradientGauge::GetNormalizedAvgLatencyGradient() const
    {
        if (normalizedAvgLatGradGaugePtr_ != nullptr) {
            return normalizedAvgLatGradGaugePtr_->load(std::memory_order::relaxed);
        }
        return 0.0f;
    }

    void LatencyGradientGauge::SetPtrToNormalizedAvgLatencyGradientGauge(std::atomic<float>* latencyGradientGaugePtr)
    {
        normalizedAvgLatGradGaugePtr_ = latencyGradientGaugePtr;
    }
#endif
}// namespace enjima::metrics
