//
// Created by m34ferna on 07/06/24.
//

#include "AdaptivePriorityCalculator.h"
#include "enjima/core/StreamingPipeline.h"
#include "enjima/metrics/MetricNames.h"
#include "enjima/runtime/RuntimeConfiguration.h"
#include "enjima/runtime/scheduling/OperatorContext.h"

#include <ranges>

namespace enjima::runtime {
    static const std::string kMaxEtId = "maxEventThreshold";
    static const std::string kMaxItId = "maxIdleThreshold";
    static const std::string kMaxEtAdjustmentId = "eventThresholdMaxAdjustment";
    static const std::string kMaxItAdjustmentId = "idleThresholdMaxAdjustment";
    static const std::string kAdaptPeriodId = "adaptPeriodMs";


    AdaptivePriorityCalculator::AdaptivePriorityCalculator(MetricsMapT* metricsMapPtr, metrics::Profiler* profilerPtr,
            uint64_t eventThreshold, uint64_t idleThresholdMs)
        : MetricsBasedPriorityCalculator(metricsMapPtr, eventThreshold, idleThresholdMs),
          numStateBasedWorkers_(profilerPtr->GetNumStateBasedWorkers()), profilerPtr_(profilerPtr),
          lastParamUpdateAtMs_(GetSteadyClockMillis())
    {
        adaptivePipelineCtxtMap_.reserve(100);

        auto config = RuntimeConfiguration("conf/enjima-config.yaml");
        std::list<std::string> paramList{kMaxEtId, kMaxItId, kMaxEtAdjustmentId, kMaxItAdjustmentId, kAdaptPeriodId};
        for (const auto& param: paramList) {
            auto paramValStr = config.GetConfigAsString({"runtime", "scheduling", param});
            if (!paramValStr.empty()) {
                if (param == kMaxEtId) {
                    maxEventThreshold_ = std::stoul(paramValStr);
                }
                if (param == kMaxItId) {
                    maxIdleThreshold_ = std::stoul(paramValStr);
                }
                if (param == kMaxEtAdjustmentId) {
                    eventThresholdMaxStepSize_ = std::stof(paramValStr);
                }
                if (param == kMaxItAdjustmentId) {
                    idleThresholdMaxStepSize_ = std::stof(paramValStr);
                }
                if (param == kAdaptPeriodId) {
                    adaptPeriodMs_ = std::stoul(paramValStr);
                }
            }
        }
    }

    float AdaptivePriorityCalculator::CalculatePriority(const OperatorContext* opCtxtPtr)
    {
        if (numActiveOperators_.load(std::memory_order_relaxed) > numStateBasedWorkers_) {
            auto opPtr = opCtxtPtr->GetOperatorPtr();
            // auto localPriority = 1.0f / static_cast<float>(numDownstreamOpsMap_.at(opPtr) + 1);
#ifdef ENJIMA_USE_NORMALIZED_OUTPUT_COST
            auto localPriority = 1.0f / normalizedOutputCostMap_.at(opPtr).load(std::memory_order::acquire);
#else
            auto localPriority = 1.0f / outputCostMap_.at(opPtr).load(std::memory_order::acquire);
#endif
            auto globalPriority = opCtxtPtr->GetPipelineCtxtPtr()->GetNormalizedAvgLatencyGradient();
            return localPriority * globalPriority;
        }
        return 1.0f;
    }

    void AdaptivePriorityCalculator::InitializeMetricsForPipeline(core::StreamingPipeline* pStreamingPipeline,
            const std::vector<OperatorContext*>& opCtxtPtrsVec)
    {
        trackedPipelines_.emplace_back(pStreamingPipeline);
        metrics::LatencyGradientGauge* latGradGaugePtr = nullptr;
        const auto& allOperators = pStreamingPipeline->GetOperatorsInTopologicalOrder();
        for (const auto& opPtr: allOperators) {
            outputSelectivityMap_.emplace(opPtr, 0.0f);
            cumulativeOutputCostMap_.emplace(opPtr, 0.0f);
#ifdef ENJIMA_USE_NORMALIZED_OUTPUT_COST
#if ENJIMA_METRICS_LEVEL >= 2
            auto [iter, emplace_success] = normalizedOutputCostMap_.emplace(opPtr, 0.0f);
            const auto outputCostPtr = &iter->second;
            profilerPtr_->CreateOutputCostTracking(
                    std::string(opPtr->GetOperatorName()).append(metrics::kOutputCostGaugeSuffix), outputCostPtr);
#else
            normalizedOutputCostMap_.emplace(opPtr, 0.0f);
#endif
#else
            outputCostMap_.emplace(opPtr, 0.0f);
#endif
            if (opPtr->IsSinkOperator()) {
                latGradGaugePtr = profilerPtr_->GetLatencyGradientGauge(
                        opPtr->GetOperatorName() + metrics::kLatencyGradGaugeSuffix);
            }
            auto numDownstreamOps = pStreamingPipeline->GetAllDownstreamOps(opPtr).size();
            numDownstreamOpsMap_.emplace(opPtr, numDownstreamOps);
        }
        numActiveOperators_.fetch_add(allOperators.size(), std::memory_order::acq_rel);
        assert(latGradGaugePtr != nullptr);
        auto& adaptivePipelineCtxt =
                adaptivePipelineCtxtMap_
                        .try_emplace(pStreamingPipeline, pStreamingPipeline, latGradGaugePtr, kEventThreshold_,
                                kIdleThresholdMs_, adaptPeriodMs_, maxEventThreshold_, maxIdleThreshold_,
                                eventThresholdMaxStepSize_, idleThresholdMaxStepSize_)
                        .first->second;
        for (auto opCtxtPtr: opCtxtPtrsVec) {
            opCtxtPtr->SetPipelineCtxtPtr(&adaptivePipelineCtxt);
        }
    }

    void AdaptivePriorityCalculator::DeactivateMetricsForPipeline(core::StreamingPipeline* pStreamingPipeline)
    {
        std::erase(trackedPipelines_, pStreamingPipeline);
        numActiveOperators_.fetch_sub(pStreamingPipeline->GetOperatorsInTopologicalOrder().size(),
                std::memory_order::acq_rel);
    }

    void AdaptivePriorityCalculator::UpdateState()
    {
        auto currentSteadyTimeMs = enjima::runtime::GetSteadyClockMillis();
        auto updateParams = false;
        if (currentSteadyTimeMs - lastParamUpdateAtMs_ >= 100) {
            updateParams = true;
            lastParamUpdateAtMs_ = currentSteadyTimeMs;
        }
        for (const auto* pipelinePtr: trackedPipelines_) {
            auto opPtrVec = pipelinePtr->GetOperatorsInTopologicalOrder();
#ifdef ENJIMA_USE_NORMALIZED_OUTPUT_COST
            auto totOutputCost = 0.0f;
#endif
            for (const auto* opPtr: std::ranges::views::reverse(opPtrVec)) {
                auto outputSelectivity = static_cast<float>(get<3>(metricsMapPtr_->at(opPtr))->GetVal());
                auto downstreamOpVec = pipelinePtr->GetDownstreamOperators(opPtr->GetOperatorId());
                if (!downstreamOpVec.empty()) {
                    auto downstreamOutputSel = outputSelectivityMap_.at(downstreamOpVec[0]);
                    for (auto i = downstreamOpVec.size(); i > 1; i--) {
                        downstreamOutputSel =
                                std::max(downstreamOutputSel, outputSelectivityMap_.at(downstreamOpVec[i - 1]));
                    }
                    outputSelectivity *= downstreamOutputSel;
                }
                outputSelectivityMap_[opPtr] = outputSelectivity;
                auto costGauge = get<2>(metricsMapPtr_->at(opPtr));
                auto outputCost = static_cast<float>(costGauge->GetVal()) / outputSelectivity;
                auto downstreamCumulativeOutputCost = 0.0f;
                for (auto i = downstreamOpVec.size(); i > 0; i--) {
                    downstreamCumulativeOutputCost += cumulativeOutputCostMap_.at(downstreamOpVec[i - 1]);
                }
                outputCost += downstreamCumulativeOutputCost;
#ifdef ENJIMA_USE_NORMALIZED_OUTPUT_COST
                totOutputCost += outputCost;
                outputCostMap_[opPtr] = outputCost;
#else
                outputCostMap_[opPtr].store(outputCost, std::memory_order::release);
#endif
                cumulativeOutputCostMap_[opPtr] = outputCost + downstreamCumulativeOutputCost;
            }
#ifdef ENJIMA_USE_NORMALIZED_OUTPUT_COST
            for (const auto* opPtr: opPtrVec) {
                const auto normalizedOutputCost = outputCostMap_[opPtr] / totOutputCost;
                normalizedOutputCostMap_[opPtr] = normalizedOutputCost;
            }
#endif
            if (updateParams) {
                adaptivePipelineCtxtMap_.at(pipelinePtr).UpdateLatencyGradient(currentSteadyTimeMs);
            }
        }
#ifdef ENJIMA_ADAPT_PARAMETERS
        for (const auto* pipelinePtr: trackedPipelines_) {
            [[maybe_unused]] auto& pipelineCtxt = adaptivePipelineCtxtMap_.at(pipelinePtr);
            pipelineCtxt.AdaptParameters(currentSteadyTimeMs);
        }
#endif
#ifdef ENJIMA_PRINT_SCHED_INFO
        if (currentSteadyTimeMs - lastPrintedAtMs_ > 1'000) {
            for (const auto* pipelinePtr: trackedPipelines_) {
                [[maybe_unused]] auto& pipelineCtxt = adaptivePipelineCtxtMap_.at(pipelinePtr);
                pipelineCtxt.PrintAdaptiveContext();
            }
            lastPrintedAtMs_ = currentSteadyTimeMs;
        }
#endif
    }

    bool AdaptivePriorityCalculator::IsEligibleForScheduling(uint64_t numPendingEvents,
            const OperatorContext* opCtxtPtr, uint8_t lastOperatorStatus) const
    {
        if (numActiveOperators_.load(std::memory_order_relaxed) > numStateBasedWorkers_) {
            auto lastScheduledAtMs = opCtxtPtr->GetLastScheduledInMillis();
            auto pipelineContextPtr = opCtxtPtr->GetPipelineCtxtPtr();
            return static_cast<bool>(lastOperatorStatus & operators::StreamingOperator::kCanOutput) &&
                   (numPendingEvents > pipelineContextPtr->GetEventThreshold() ||
                           (numPendingEvents > 0 && (runtime::GetSystemTimeMillis() - lastScheduledAtMs) >
                                                            pipelineContextPtr->GetIdleThreshold()));
        }
        return true;
    }
}// namespace enjima::runtime
