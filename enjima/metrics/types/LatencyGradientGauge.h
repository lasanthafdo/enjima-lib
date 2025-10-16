//
// Created by m34ferna on 17/03/25.
//

#ifndef ENJIMA_LATENCY_GRADIENT_GAUGE_H
#define ENJIMA_LATENCY_GRADIENT_GAUGE_H

#include "Gauge.h"
#include "Histogram.h"

namespace enjima::metrics {

    class LatencyGradientGauge : public Gauge<float> {
    public:
        explicit LatencyGradientGauge(Histogram<uint64_t>* latencyHistPtr);
        float GetVal() override;
        float GetLatencyGradientOfAvg();
        float GetLatencyGradientWithSCTime(uint64_t scTimeMs);
        float GetLatencyGradientOfAvgWithSCTime(uint64_t scTimeMs);
        void SetUpdateIntervalMillis(uint64_t updateIntervalMillis);
#if ENJIMA_METRICS_LEVEL >= 2
        float GetNormalizedAvgLatencyGradient() const;
        void SetPtrToNormalizedAvgLatencyGradientGauge(std::atomic<float>* latencyGradientGaugePtr);
#endif


    private:
        Histogram<uint64_t>* pLatencyHistogram_;
        float lastInstantaneousLatency_{0.0f};
        float lastAvgLatency_{0.0f};
        uint64_t lastLGASteadyClockTime_;
        uint64_t lastLGISteadyClockTime_;
        std::atomic<float> lastInstantaneousLatencyGradient_{0.0f};
        std::atomic<float> lastLatencyGradientOfAvg_{0.0f};
        std::atomic<bool> calcGuardLGA{false};
        std::atomic<bool> calcGuardLGI{false};
        uint64_t updateIntervalMillis_{10'000};
#if ENJIMA_METRICS_LEVEL >= 2
        std::atomic<float>* normalizedAvgLatGradGaugePtr_{nullptr};
#endif
    };

}// namespace enjima::metrics


#endif//ENJIMA_LATENCY_GRADIENT_GAUGE_H
