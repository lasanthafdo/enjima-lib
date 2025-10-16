//
// Created by m34ferna on 18/03/25.
//

#ifndef ENJIMA_ADAPTIVE_OPERATOR_CONTEXT_H
#define ENJIMA_ADAPTIVE_OPERATOR_CONTEXT_H

#include "SchedulingPolicy.h"

namespace enjima::runtime {

    class AdaptivePipelineContext {
    public:
        AdaptivePipelineContext(core::StreamingPipeline* pipelinePtr, metrics::LatencyGradientGauge* latGradGaugePtr,
                uint64_t initEventThreshold, uint64_t initIdleThreshold, uint64_t adaptPeriodMs,
                uint64_t maxEventThreshold, uint64_t maxIdleThreshold, float eventThresholdMaxStepSize,
                float idleThresholdMaxStepSize);
        void UpdateLatencyGradient(uint64_t scTimeMs);
        void AdaptParameters(uint64_t currentSteadyClockTimeMs);
        void PrintAdaptiveContext() const;
        [[nodiscard]] uint64_t GetEventThreshold() const;
        [[nodiscard]] uint64_t GetIdleThreshold() const;
        [[nodiscard]] float GetNormalizedAvgLatencyGradient() const;

    private:
        core::StreamingPipeline* pipelinePtr_;
        metrics::LatencyGradientGauge* latGradGaugePtr_;
        float avgLatGrad_{0.0f};
        float lastAvgLatGrad_{0.0f};
        std::atomic<float> normalizedAvgLatGrad_{1.0f};
        std::vector<float> lastNLatGradsVec_;
        size_t latGradVecIndex_{0};
        std::atomic<uint64_t> eventThreshold_;
        std::atomic<uint64_t> idleThreshold_;
        uint64_t lastAdaptationScMs_{};
        uint64_t lastEventThreshold_;
        uint64_t lastIdleThreshold_;
        const uint64_t kAdaptPeriodMs_;
        const uint64_t kMaxEventThreshold_;
        const uint64_t kMaxIdleThreshold_;
        const float kEventThresholdMaxStepSize_;
        const float kIdleThresholdMaxStepSize_;
    };

}// namespace enjima::runtime


#endif//ENJIMA_ADAPTIVE_OPERATOR_CONTEXT_H
