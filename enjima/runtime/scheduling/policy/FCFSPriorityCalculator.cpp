//
// Created by m34ferna on 07/06/24.
//

#include "FCFSPriorityCalculator.h"
#include "enjima/core/StreamingPipeline.h"
#include "enjima/runtime/IllegalArgumentException.h"
#include "enjima/runtime/scheduling/OperatorContext.h"

namespace enjima::runtime {
    static const uint64_t kMinuteThresholdInMicros = 60 * 1000 * 1000;

    FCFSPriorityCalculator::FCFSPriorityCalculator(MetricsMapT* metricsMapPtr)
        : MetricsBasedPriorityCalculator(metricsMapPtr, 0, 0),
          nextUpperThresholdMicros_(GetSystemTimeMicros() + kMinuteThresholdInMicros)
    {
    }

    float FCFSPriorityCalculator::CalculatePriority(const OperatorContext* opCtxtPtr)
    {
        auto opPtr = opCtxtPtr->GetOperatorPtr();
        const auto* pipelinePtr = pipelineByOpPtrMap_.at(opPtr);
        if (pipelineActivatedMap_.at(pipelinePtr).load(std::memory_order::acquire)) {
            auto priority = static_cast<float>(nextUpperThresholdMicros_.load(std::memory_order::acquire) -
                                               lastEligibleAt_.at(opPtr).load(std::memory_order::acquire));
            return priority;
        }
        return 0.0f;
    }

    void FCFSPriorityCalculator::InitializeMetricsForPipeline(core::StreamingPipeline* pStreamingPipeline,
            [[maybe_unused]] const std::vector<OperatorContext*>& opCtxtPtrsVec)
    {
        for (const auto& opPtr: pStreamingPipeline->GetOperatorsInTopologicalOrder()) {
            downstreamOpVecsMap_.emplace(opPtr, pStreamingPipeline->GetDownstreamOperators(opPtr->GetOperatorId()));
            lastEligibleAt_.emplace(opPtr, 0);
            pipelineByOpPtrMap_.emplace(opPtr, pStreamingPipeline);
        }
        pipelineActivatedMap_.emplace(pStreamingPipeline, true);
    }

    void FCFSPriorityCalculator::UpdateState()
    {
        nextUpperThresholdMicros_.store(GetSystemTimeMicros() + kMinuteThresholdInMicros, std::memory_order::release);
    }

    void FCFSPriorityCalculator::NotifyScheduleCompletion(const OperatorContext* opCtxtPtr)
    {
        const auto* opPtr = opCtxtPtr->GetOperatorPtr();
        const auto* pipelinePtr = pipelineByOpPtrMap_.at(opPtr);
        if (pipelineActivatedMap_.at(pipelinePtr).load(std::memory_order::acquire)) {
            auto& currentOpLastEligibleAtCntr = lastEligibleAt_.at(opPtr);
            auto currentTimeMicros = GetSystemTimeMicros();
            auto currentOpLastEligibleMillis = currentOpLastEligibleAtCntr.load(std::memory_order::acquire) / 1000;
            if (currentOpLastEligibleMillis <= opCtxtPtr->GetLastScheduledInMillis()) {
                currentOpLastEligibleAtCntr.store(currentTimeMicros, std::memory_order::release);
            }
            if (opCtxtPtr->GetLastOperatorStatus() & operators::StreamingOperator::kHasOutput) {
                for (auto& downstreamOpPtr: downstreamOpVecsMap_.at(opPtr)) {
                    lastEligibleAt_.at(downstreamOpPtr).store(currentTimeMicros, std::memory_order::release);
                }
            }
        }
    }

    void FCFSPriorityCalculator::DeactivateMetricsForPipeline(core::StreamingPipeline* pStreamingPipeline)
    {
        if (pStreamingPipeline == nullptr) {
            throw IllegalArgumentException{"Pipeline passed for metrics deactivation is null!"};
        }
        pipelineActivatedMap_.at(pStreamingPipeline).store(false, std::memory_order::release);
    }

    bool FCFSPriorityCalculator::IsEligibleForScheduling(uint64_t numPendingEvents,
            [[maybe_unused]] const OperatorContext* opCtxtPtr, uint8_t lastOperatorStatus) const
    {
        if (numPendingEvents > 0 && (lastOperatorStatus & operators::StreamingOperator::kCanOutput)) {
            return true;
        }
        return false;
    }
}// namespace enjima::runtime
