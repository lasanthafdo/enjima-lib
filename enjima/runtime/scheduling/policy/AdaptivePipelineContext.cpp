//
// Created by m34ferna on 18/03/25.
//

#include "AdaptivePipelineContext.h"
#include "enjima/operators/StreamingOperator.h"
#include "enjima/runtime/RuntimeUtil.h"

#include <iostream>

namespace enjima::runtime {
    static const int kGradVecSize = 5;

    AdaptivePipelineContext::AdaptivePipelineContext(core::StreamingPipeline* pipelinePtr,
            metrics::LatencyGradientGauge* latGradGaugePtr, uint64_t initEventThreshold, uint64_t initIdleThreshold,
            uint64_t adaptPeriodMs, uint64_t maxEventThreshold, uint64_t maxIdleThreshold,
            float eventThresholdMaxStepSize, float idleThresholdMaxStepSize)
        : pipelinePtr_(pipelinePtr), latGradGaugePtr_(latGradGaugePtr), eventThreshold_(initEventThreshold),
          idleThreshold_(initIdleThreshold), lastAdaptationScMs_(GetSteadyClockMillis()),
          lastEventThreshold_(initEventThreshold), lastIdleThreshold_(initIdleThreshold),
          kAdaptPeriodMs_(adaptPeriodMs), kMaxEventThreshold_(maxEventThreshold), kMaxIdleThreshold_(maxIdleThreshold),
          kEventThresholdMaxStepSize_(eventThresholdMaxStepSize), kIdleThresholdMaxStepSize_(idleThresholdMaxStepSize)
    {
        lastNLatGradsVec_.reserve(kGradVecSize);
        for (auto i = 0; i < kGradVecSize; i++) {
            lastNLatGradsVec_.emplace_back(0.0f);
        }
#if ENJIMA_METRICS_LEVEL >= 2
        latGradGaugePtr_->SetPtrToNormalizedAvgLatencyGradientGauge(&normalizedAvgLatGrad_);
#endif
    }

    void AdaptivePipelineContext::UpdateLatencyGradient(uint64_t scTimeMs)
    {
#if ENJIMA_LATENCY_GRAD_TYPE == 1
        auto currentGrad = latGradGaugePtr_->GetLatencyGradientWithSCTime(scTimeMs);
#else
        auto currentGrad = latGradGaugePtr_->GetLatencyGradientOfAvgWithSCTime(scTimeMs);
#endif
        lastNLatGradsVec_[latGradVecIndex_] = currentGrad;
        latGradVecIndex_ = (latGradVecIndex_ + 1) % kGradVecSize;
        auto sum = std::accumulate(lastNLatGradsVec_.begin(), lastNLatGradsVec_.end(), 0.0f);
        avgLatGrad_ = sum / static_cast<float>(lastNLatGradsVec_.size());
        normalizedAvgLatGrad_.store(((std::max(std::min(avgLatGrad_, 1.0f), -1.0f)) / 10.0f) + 1.0f,
                std::memory_order::release);
    }

    void AdaptivePipelineContext::AdaptParameters(uint64_t currentSteadyClockTimeMs)
    {
        if ((currentSteadyClockTimeMs - lastAdaptationScMs_ > kAdaptPeriodMs_)) {
            if (lastAvgLatGrad_ < avgLatGrad_) {
                eventThreshold_.store(lastEventThreshold_, std::memory_order::release);
                idleThreshold_.store(lastIdleThreshold_, std::memory_order::release);
            }
            else {
                auto currentEventThreshold = static_cast<float>(eventThreshold_.load(std::memory_order::acquire));
                auto targetedEtStep = std::min(avgLatGrad_ * currentEventThreshold, kEventThresholdMaxStepSize_);
                if (((targetedEtStep + currentEventThreshold) > 0) &&
                        ((targetedEtStep + currentEventThreshold) < kMaxEventThreshold_)) {
                    lastEventThreshold_ = eventThreshold_.fetch_add(static_cast<uint64_t>(targetedEtStep),
                            std::memory_order::acq_rel);
                }
                auto currentIdleThreshold = static_cast<float>(idleThreshold_.load(std::memory_order::acquire));
                auto targetedItStep = std::min(avgLatGrad_ * currentIdleThreshold, kIdleThresholdMaxStepSize_);
                if (((currentIdleThreshold + targetedItStep) > 0) &&
                        ((currentIdleThreshold + targetedItStep) < kMaxIdleThreshold_)) {
                    lastIdleThreshold_ =
                            idleThreshold_.fetch_add(static_cast<uint64_t>(targetedItStep), std::memory_order::acq_rel);
                }
            }
            lastAvgLatGrad_ = avgLatGrad_;
            lastAdaptationScMs_ = currentSteadyClockTimeMs;
        }
    }

    void AdaptivePipelineContext::PrintAdaptiveContext() const
    {
        std::cout << "Pipeline Pointer: " << pipelinePtr_ << std::endl;
        std::cout << "Avg. Latency Gradient: " << avgLatGrad_;
        std::cout << ", Norm. Avg. Latency Gradient: " << normalizedAvgLatGrad_.load(std::memory_order::relaxed)
                  << std::endl;
        std::cout << "Last ET: " << lastEventThreshold_ << ", ET: " << eventThreshold_.load(std::memory_order::relaxed);
        std::cout << ", Last IT: " << lastIdleThreshold_ << ", IT: " << idleThreshold_.load(std::memory_order::relaxed)
                  << std::endl;
        std::cout << std::endl;
    }

    uint64_t AdaptivePipelineContext::GetEventThreshold() const
    {
        return eventThreshold_.load(std::memory_order::acquire);
    }

    uint64_t AdaptivePipelineContext::GetIdleThreshold() const
    {
        return idleThreshold_.load(std::memory_order::acquire);
    }

    float AdaptivePipelineContext::GetNormalizedAvgLatencyGradient() const
    {
        return normalizedAvgLatGrad_.load(std::memory_order::acquire);
    }

}// namespace enjima::runtime
