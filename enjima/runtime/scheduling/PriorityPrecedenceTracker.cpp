//
// Created by m34ferna on 04/06/24.
//

#include "PriorityPrecedenceTracker.h"
#include "enjima/runtime/scheduling/policy/AdaptivePriorityCalculator.h"
#include "enjima/runtime/scheduling/policy/InputSizeBasedPriorityCalculator.h"
#include "enjima/runtime/scheduling/policy/LatencyOptimizedPriorityCalculator.h"
#include "enjima/runtime/scheduling/policy/MinLatencyPriorityCalculator.h"
#include "enjima/runtime/scheduling/policy/SPLatencyOptimizedPriorityCalculator.h"
#include "enjima/runtime/scheduling/policy/SimpleThroughputPriorityCalculator.h"
#include "policy/HighestRatePriorityCalculator.h"

#include <any>

namespace enjima::runtime {

    template<>
    PriorityPrecedenceTracker<NonPreemptiveMode, InputSizeBasedPriorityCalculator>::PriorityPrecedenceTracker(
            metrics::Profiler* profilerPtr, [[maybe_unused]] uint64_t schedulingPeriodMs,
            [[maybe_unused]] uint64_t eventThreshold, [[maybe_unused]] uint64_t idleThresholdMs,
            [[maybe_unused]] size_t defaultBlockSize)
        : profilerPtr_(profilerPtr), schedulingPolicy_(&metricsMap_),
          preemptModeCalc_(&metricsMap_, &nextSchedulingUpdateAtMicros_)
    {
        Init();
    }

    template<>
    PriorityPrecedenceTracker<PreemptiveMode, InputSizeBasedPriorityCalculator>::PriorityPrecedenceTracker(
            metrics::Profiler* profilerPtr, [[maybe_unused]] uint64_t schedulingPeriodMs,
            [[maybe_unused]] uint64_t eventThreshold, [[maybe_unused]] uint64_t idleThresholdMs,
            [[maybe_unused]] size_t defaultBlockSize)
        : profilerPtr_(profilerPtr), schedulingPolicy_(&metricsMap_)
    {
        Init();
    }

    template<>
    PriorityPrecedenceTracker<NonPreemptiveMode, AdaptivePriorityCalculator>::PriorityPrecedenceTracker(
            metrics::Profiler* profilerPtr, [[maybe_unused]] uint64_t schedulingPeriodMs, uint64_t eventThreshold,
            uint64_t idleThresholdMs, [[maybe_unused]] size_t defaultBlockSize)
        : profilerPtr_(profilerPtr), schedulingPolicy_(&metricsMap_, profilerPtr, eventThreshold, idleThresholdMs),
          preemptModeCalc_(&metricsMap_, &nextSchedulingUpdateAtMicros_)
    {
        Init();
    }

    template<>
    PriorityPrecedenceTracker<PreemptiveMode, AdaptivePriorityCalculator>::PriorityPrecedenceTracker(
            metrics::Profiler* profilerPtr, [[maybe_unused]] uint64_t schedulingPeriodMs, uint64_t eventThreshold,
            uint64_t idleThresholdMs, [[maybe_unused]] size_t defaultBlockSize)
        : profilerPtr_(profilerPtr), schedulingPolicy_(&metricsMap_, profilerPtr, eventThreshold, idleThresholdMs)
    {
        Init();
    }

    template<>
    PriorityPrecedenceTracker<NonPreemptiveMode, LatencyOptimizedPriorityCalculator>::PriorityPrecedenceTracker(
            metrics::Profiler* profilerPtr, [[maybe_unused]] uint64_t schedulingPeriodMs, uint64_t eventThreshold,
            uint64_t idleThresholdMs, [[maybe_unused]] size_t defaultBlockSize)
        : profilerPtr_(profilerPtr), schedulingPolicy_(&metricsMap_, eventThreshold, idleThresholdMs),
          preemptModeCalc_(&metricsMap_, &nextSchedulingUpdateAtMicros_)
    {
        Init();
    }

    template<>
    PriorityPrecedenceTracker<PreemptiveMode, LatencyOptimizedPriorityCalculator>::PriorityPrecedenceTracker(
            metrics::Profiler* profilerPtr, [[maybe_unused]] uint64_t schedulingPeriodMs, uint64_t eventThreshold,
            uint64_t idleThresholdMs, [[maybe_unused]] size_t defaultBlockSize)
        : profilerPtr_(profilerPtr), schedulingPolicy_(&metricsMap_, eventThreshold, idleThresholdMs)
    {
        Init();
    }

    template<>
    PriorityPrecedenceTracker<NonPreemptiveBasicMode, HighestRatePriorityCalculator>::PriorityPrecedenceTracker(
            metrics::Profiler* profilerPtr, [[maybe_unused]] uint64_t schedulingPeriodMs,
            [[maybe_unused]] uint64_t eventThreshold, [[maybe_unused]] uint64_t idleThresholdMs,
            [[maybe_unused]] size_t defaultBlockSize)
        : profilerPtr_(profilerPtr), schedulingPolicy_(&metricsMap_)
    {
        Init();
    }

    template<>
    PriorityPrecedenceTracker<NonPreemptiveBasicMode, MinLatencyPriorityCalculator>::PriorityPrecedenceTracker(
            metrics::Profiler* profilerPtr, [[maybe_unused]] uint64_t schedulingPeriodMs,
            [[maybe_unused]] uint64_t eventThreshold, [[maybe_unused]] uint64_t idleThresholdMs,
            [[maybe_unused]] size_t defaultBlockSize)
        : profilerPtr_(profilerPtr), schedulingPolicy_(&metricsMap_)
    {
        Init();
    }

    template<>
    PriorityPrecedenceTracker<NonPreemptiveMode, SPLatencyOptimizedPriorityCalculator>::PriorityPrecedenceTracker(
            metrics::Profiler* profilerPtr, [[maybe_unused]] uint64_t schedulingPeriodMs, uint64_t eventThreshold,
            uint64_t idleThresholdMs, [[maybe_unused]] size_t defaultBlockSize)
        : profilerPtr_(profilerPtr), schedulingPolicy_(&metricsMap_, eventThreshold, idleThresholdMs),
          preemptModeCalc_(&metricsMap_, &nextSchedulingUpdateAtMicros_)
    {
        Init();
    }

    template<>
    PriorityPrecedenceTracker<PreemptiveMode, SPLatencyOptimizedPriorityCalculator>::PriorityPrecedenceTracker(
            metrics::Profiler* profilerPtr, [[maybe_unused]] uint64_t schedulingPeriodMs, uint64_t eventThreshold,
            uint64_t idleThresholdMs, [[maybe_unused]] size_t defaultBlockSize)
        : profilerPtr_(profilerPtr), schedulingPolicy_(&metricsMap_, eventThreshold, idleThresholdMs)
    {
        Init();
    }

    template<>
    PriorityPrecedenceTracker<NonPreemptiveMode, LeastRecentOperatorPriorityCalculator>::PriorityPrecedenceTracker(
            metrics::Profiler* profilerPtr, [[maybe_unused]] uint64_t schedulingPeriodMs,
            [[maybe_unused]] uint64_t eventThreshold, [[maybe_unused]] uint64_t idleThresholdMs,
            [[maybe_unused]] size_t defaultBlockSize)
        : profilerPtr_(profilerPtr), preemptModeCalc_(&metricsMap_, &nextSchedulingUpdateAtMicros_)
    {
        Init();
    }

    template<>
    PriorityPrecedenceTracker<PreemptiveMode, LeastRecentOperatorPriorityCalculator>::PriorityPrecedenceTracker(
            metrics::Profiler* profilerPtr, [[maybe_unused]] uint64_t schedulingPeriodMs,
            [[maybe_unused]] uint64_t eventThreshold, [[maybe_unused]] uint64_t idleThresholdMs,
            [[maybe_unused]] size_t defaultBlockSize)
        : profilerPtr_(profilerPtr)
    {
        Init();
    }

    template<>
    PriorityPrecedenceTracker<PreemptiveMode, ThroughputOptimizedPriorityCalculator>::PriorityPrecedenceTracker(
            metrics::Profiler* profilerPtr, [[maybe_unused]] uint64_t schedulingPeriodMs,
            [[maybe_unused]] uint64_t eventThreshold, [[maybe_unused]] uint64_t idleThresholdMs,
            [[maybe_unused]] size_t defaultBlockSize)
        : profilerPtr_(profilerPtr), schedulingPolicy_(&metricsMap_)
    {
        Init();
    }

    template<>
    PriorityPrecedenceTracker<NonPreemptiveThroughputOptimizedMode,
            ThroughputOptimizedPriorityCalculator>::PriorityPrecedenceTracker(metrics::Profiler* profilerPtr,
            [[maybe_unused]] uint64_t schedulingPeriodMs, [[maybe_unused]] uint64_t eventThreshold,
            [[maybe_unused]] uint64_t idleThresholdMs, size_t defaultBlockSize)
        : profilerPtr_(profilerPtr), schedulingPolicy_(&metricsMap_), preemptModeCalc_(&metricsMap_, defaultBlockSize)
    {
        Init();
    }

    template<>
    PriorityPrecedenceTracker<NonPreemptiveMode, SimpleThroughputPriorityCalculator>::PriorityPrecedenceTracker(
            metrics::Profiler* profilerPtr, [[maybe_unused]] uint64_t schedulingPeriodMs,
            [[maybe_unused]] uint64_t eventThreshold, [[maybe_unused]] uint64_t idleThresholdMs,
            [[maybe_unused]] size_t defaultBlockSize)
        : profilerPtr_(profilerPtr), preemptModeCalc_(&metricsMap_, &nextSchedulingUpdateAtMicros_)
    {
        Init();
    }


    template<>
    PriorityPrecedenceTracker<PreemptiveMode, SimpleThroughputPriorityCalculator>::PriorityPrecedenceTracker(
            metrics::Profiler* profilerPtr, [[maybe_unused]] uint64_t schedulingPeriodMs,
            [[maybe_unused]] uint64_t eventThreshold, [[maybe_unused]] uint64_t idleThresholdMs,
            [[maybe_unused]] size_t defaultBlockSize)
        : profilerPtr_(profilerPtr)
    {
        Init();
    }

    template<>
    PriorityPrecedenceTracker<NonPreemptiveThroughputOptimizedMode,
            MinCostPriorityCalculator>::PriorityPrecedenceTracker(metrics::Profiler* profilerPtr,
            [[maybe_unused]] uint64_t schedulingPeriodMs, [[maybe_unused]] uint64_t eventThreshold,
            [[maybe_unused]] uint64_t idleThresholdMs, size_t defaultBlockSize)
        : profilerPtr_(profilerPtr), schedulingPolicy_(&metricsMap_), preemptModeCalc_(&metricsMap_, defaultBlockSize)
    {
        Init();
    }

    template<>
    PriorityPrecedenceTracker<PreemptiveMode, RoundRobinPriorityCalculator>::PriorityPrecedenceTracker(
            metrics::Profiler* profilerPtr, [[maybe_unused]] uint64_t schedulingPeriodMs,
            [[maybe_unused]] uint64_t eventThreshold, [[maybe_unused]] uint64_t idleThresholdMs,
            [[maybe_unused]] size_t defaultBlockSize)
        : profilerPtr_(profilerPtr), schedulingPolicy_(&metricsMap_)
    {
        Init();
    }

    template<>
    PriorityPrecedenceTracker<NonPreemptiveBasicMode, RoundRobinPriorityCalculator>::PriorityPrecedenceTracker(
            metrics::Profiler* profilerPtr, [[maybe_unused]] uint64_t schedulingPeriodMs,
            [[maybe_unused]] uint64_t eventThreshold, [[maybe_unused]] uint64_t idleThresholdMs,
            [[maybe_unused]] size_t defaultBlockSize)
        : profilerPtr_(profilerPtr), schedulingPolicy_(&metricsMap_)
    {
        Init();
    }

    template<>
    PriorityPrecedenceTracker<NonPreemptiveBasicMode, FCFSPriorityCalculator>::PriorityPrecedenceTracker(
            metrics::Profiler* profilerPtr, [[maybe_unused]] uint64_t schedulingPeriodMs,
            [[maybe_unused]] uint64_t eventThreshold, [[maybe_unused]] uint64_t idleThresholdMs,
            [[maybe_unused]] size_t defaultBlockSize)
        : profilerPtr_(profilerPtr), schedulingPolicy_(&metricsMap_)
    {
        Init();
    }

    // Update method(s)
    template<>
    void PriorityPrecedenceTracker<PreemptiveMode, RoundRobinPriorityCalculator>::UpdatePriority(
            uint64_t nextSchedulingUpdateAtMicros)
    {
        // This is used to lock access to the currentOperators_ vector. This only prevents operators been added
        // or removed during rescheduling.
#if ENJIMA_METRICS_LEVEL >= 3
        auto timeBeforeVecLock = runtime::GetSteadyClockMicros();
#endif
        std::lock_guard<std::mutex> modifyLockGuard{modificationMutex_};
#if ENJIMA_METRICS_LEVEL >= 3
        vecLockTimeAvgGauge_->UpdateVal(static_cast<double>(runtime::GetSteadyClockMicros() - timeBeforeVecLock));
        auto timeBeforeStateUpdate = runtime::GetSteadyClockMicros();
#endif
        schedulingPolicy_.UpdateState();
#if ENJIMA_METRICS_LEVEL >= 3
        priorityUpdateTimeAvgGauge_->UpdateVal(
                static_cast<double>(runtime::GetSteadyClockMicros() - timeBeforeStateUpdate));
        auto timeBeforeTotUpdate = runtime::GetSteadyClockMicros();
#endif
        auto anyWaitingToCancel = std::ranges::any_of(currentOpCtxtPtrs_.begin(), currentOpCtxtPtrs_.end(),
                [](OperatorContext* opCtxtPtr) { return opCtxtPtr->IsWaitingToCancel(); });
        anyOpWaitingToCancel_.store(anyWaitingToCancel, std::memory_order::release);
        for (auto* opCtxtPtr: currentOpCtxtPtrs_) {
            if (opCtxtPtr->IsWaitingToBePickedByScheduler()) {
                auto* schedCtxtPtr = schedCtxtMap_.at(opCtxtPtr);
                schedulingQueue_.Emplace(schedCtxtPtr);
                opCtxtPtr->MarkPickedByScheduler();
            }
        }
        for (auto i = 0ul; i < schedulingQueue_.GetActiveSize(); i++) {
            auto schedCtxtPtrToSchedule = schedulingQueue_.Get(i);
            if (schedCtxtPtrToSchedule->IsActive()) {
                auto opCtxtPtr = schedCtxtPtrToSchedule->GetOpCtxtPtr();
                auto* opPtr = opCtxtPtr->GetOperatorPtr();
                auto numPendingEvents = GetNumPendingEvents(opPtr);
                auto waitingToCancel = opCtxtPtr->IsWaitingToCancel();
                float priority = 0.0f;
                if (schedulingPolicy_.IsEligibleForScheduling(numPendingEvents, opCtxtPtr,
                            operators::StreamingOperator::kCanOutput) ||
                        waitingToCancel) {
                    if (anyWaitingToCancel) {
                        const core::StreamingPipeline* pipelinePtr = opPipelineMap_.at(opCtxtPtr);
                        if (pipelinePtr->IsWaitingToCancel()) {
                            const auto totalOps = pipelinePtr->GetOperatorsInTopologicalOrder().size();
                            if (opCtxtPtr->IsWaitingToCancel()) {
                                const auto& downstreamOps = downstreamOpsByOpPtrMap_.at(opPtr);
                                const auto hasDownStreamEvents =
                                        std::ranges::any_of(downstreamOps, [this](auto downstreamOpPtr) {
                                            const SchedulingMetricTupleT metricsTuple = metricsMap_.at(downstreamOpPtr);
                                            return std::get<0>(metricsTuple)->GetVal() > 0;
                                        });
                                if (!hasDownStreamEvents) {
                                    priority = static_cast<float>(totalOps);
                                }
                            }
                            else {
                                if (!opPtr->IsSourceOperator() && std::get<0>(metricsMap_.at(opPtr))->GetVal() > 0) {
                                    priority = static_cast<float>(totalOps - downstreamOpsByOpPtrMap_.at(opPtr).size());
                                }
                            }
                        }
                    }
                    else {
                        priority = schedulingPolicy_.CalculatePriority(opCtxtPtr);
                    }
                }
                schedCtxtPtrToSchedule->SetPriority(priority);
            }
        }
        schedulingEpoch_.fetch_add(1, std::memory_order::acq_rel);
#if ENJIMA_METRICS_LEVEL >= 3
        totCalcTimeAvgGauge_->UpdateVal(static_cast<double>(runtime::GetSteadyClockMicros() - timeBeforeTotUpdate));
#endif
        nextSchedulingUpdateAtMicros_.store(nextSchedulingUpdateAtMicros, std::memory_order::release);
    }

    // SetPriorityPostRun method(s)
    template<>
    void PriorityPrecedenceTracker<PreemptiveMode, RoundRobinPriorityCalculator>::SetPriorityPostRun(
            SchedulingContext* prevSchedCtxtPtr, const OperatorContext* prevOpCtxtPtr)
    {
        auto opPtr = prevOpCtxtPtr->GetOperatorPtr();
        assert(opPtr != nullptr);
        auto numPendingEvents = GetNumPendingEvents(opPtr);
        float priority = 0.0f;
        if (schedulingPolicy_.IsEligibleForScheduling(numPendingEvents, prevOpCtxtPtr,
                    prevOpCtxtPtr->GetLastOperatorStatus()) ||
                prevOpCtxtPtr->IsWaitingToCancel()) {
            if (anyOpWaitingToCancel_.load(std::memory_order::acquire)) {
                const enjima::core::StreamingPipeline* pipelinePtr = opPipelineMap_.at(prevOpCtxtPtr);
                const auto totalOps = pipelinePtr->GetOperatorsInTopologicalOrder().size();
                if (prevOpCtxtPtr->IsWaitingToCancel()) {
                    const auto& downstreamOps = downstreamOpsByOpPtrMap_.at(opPtr);
                    const auto hasDownStreamEvents = std::ranges::any_of(downstreamOps, [this](auto downstreamOpPtr) {
                        const SchedulingMetricTupleT metricsTuple = metricsMap_.at(downstreamOpPtr);
                        return std::get<0>(metricsTuple)->GetVal() > 0;
                    });
                    if (!hasDownStreamEvents) {
                        priority = static_cast<float>(totalOps);
                    }
                }
                else {
                    if (!opPtr->IsSourceOperator() && std::get<0>(metricsMap_.at(opPtr))->GetVal() > 0) {
                        priority = static_cast<float>(totalOps - downstreamOpsByOpPtrMap_.at(opPtr).size());
                    }
                }
            }
            else {
                priority = schedulingPolicy_.CalculatePriority(prevOpCtxtPtr);
            }
        }
        prevSchedCtxtPtr->SetPriority(priority);
    }

    template<>
    void PriorityPrecedenceTracker<PreemptiveMode, LeastRecentOperatorPriorityCalculator>::TrackPipeline(
            core::StreamingPipeline* pStreamingPipeline)
    {
        // These methods are called only when de-/registering operators. So it is OK to have a lock here
        std::lock_guard<std::mutex> lockGuard(modificationMutex_);
        for (const auto& pStreamOp: pStreamingPipeline->GetOperatorsInTopologicalOrder()) {
            InitOpMetrics(pStreamOp);
            auto* opCtxtPtr = new (schedMemoryCursor_) OperatorContext{pStreamOp, OperatorContext::kInactive};
            schedMemoryCursor_ = static_cast<OperatorContext*>(schedMemoryCursor_) + 1;
#if ENJIMA_METRICS_LEVEL >= 3
            auto gapCounter = profilerPtr_->GetOrCreateCounter(
                    std::string(pStreamOp->GetOperatorName()).append(metrics::kGapCounterSuffix));
            auto gapTimeCounter = profilerPtr_->GetOrCreateCounter(
                    std::string(pStreamOp->GetOperatorName()).append(metrics::kGapTimeCounterSuffix));
            opCtxtPtr->SetScheduleGapCounters(gapCounter, gapTimeCounter);
#endif
            auto* decisionCtxtPtr =
                    new (schedMemoryCursor_) SchedulingDecisionContext{pStreamOp, get<4>(metricsMap_.at(pStreamOp))};
            schedMemoryCursor_ = static_cast<SchedulingDecisionContext*>(schedMemoryCursor_) + 1;
            auto* schedCtxtPtr = new (schedMemoryCursor_) SchedulingContext{opCtxtPtr, decisionCtxtPtr};
            schedMemoryCursor_ = static_cast<SchedulingContext*>(schedMemoryCursor_) + 1;
            currentOpCtxtPtrs_.emplace_back(opCtxtPtr);
            schedCtxtMap_.emplace(opCtxtPtr, schedCtxtPtr);
            std::vector<operators::StreamingOperator*> downstreamOps =
                    pStreamingPipeline->GetAllDownstreamOps(pStreamOp);
            downstreamOpsByOpPtrMap_.emplace(pStreamOp, downstreamOps);
            opPipelineMap_.emplace(opCtxtPtr, pStreamingPipeline);
        }
        currentPipelines_.emplace_back(pStreamingPipeline);
    }

    template<>
    void PriorityPrecedenceTracker<NonPreemptiveMode, LeastRecentOperatorPriorityCalculator>::TrackPipeline(
            core::StreamingPipeline* pStreamingPipeline)
    {
        // These methods are called only when de-/registering operators. So it is OK to have a lock here
        std::lock_guard<std::mutex> lockGuard(modificationMutex_);
        for (const auto& pStreamOp: pStreamingPipeline->GetOperatorsInTopologicalOrder()) {
            InitOpMetrics(pStreamOp);
            auto* opCtxtPtr = new (schedMemoryCursor_) OperatorContext{pStreamOp, OperatorContext::kInactive};
            schedMemoryCursor_ = static_cast<OperatorContext*>(schedMemoryCursor_) + 1;
#if ENJIMA_METRICS_LEVEL >= 3
            auto gapCounter = profilerPtr_->GetOrCreateCounter(
                    std::string(pStreamOp->GetOperatorName()).append(metrics::kGapCounterSuffix));
            auto gapTimeCounter = profilerPtr_->GetOrCreateCounter(
                    std::string(pStreamOp->GetOperatorName()).append(metrics::kGapTimeCounterSuffix));
            opCtxtPtr->SetScheduleGapCounters(gapCounter, gapTimeCounter);
#endif
            auto* decisionCtxtPtr =
                    new (schedMemoryCursor_) SchedulingDecisionContext{pStreamOp, get<4>(metricsMap_.at(pStreamOp))};
            schedMemoryCursor_ = static_cast<SchedulingDecisionContext*>(schedMemoryCursor_) + 1;
            auto* schedCtxtPtr = new (schedMemoryCursor_) SchedulingContext{opCtxtPtr, decisionCtxtPtr};
            schedMemoryCursor_ = static_cast<SchedulingContext*>(schedMemoryCursor_) + 1;
            currentOpCtxtPtrs_.emplace_back(opCtxtPtr);
            schedCtxtMap_.emplace(opCtxtPtr, schedCtxtPtr);
            std::vector<operators::StreamingOperator*> downstreamOps =
                    pStreamingPipeline->GetAllDownstreamOps(pStreamOp);
            downstreamOpsByOpPtrMap_.emplace(pStreamOp, downstreamOps);
            opPipelineMap_.emplace(opCtxtPtr, pStreamingPipeline);
        }
        currentPipelines_.emplace_back(pStreamingPipeline);
    }

    template<>
    bool PriorityPrecedenceTracker<PreemptiveMode, LeastRecentOperatorPriorityCalculator>::UnTrackPipeline(
            core::StreamingPipeline* pStreamingPipeline)
    {
        // These methods are called only when de-/registering operators. So it is OK to have a lock here
        std::lock_guard<std::mutex> lockGuard(modificationMutex_);
        schedulingQueue_.EraseAll(pStreamingPipeline->GetOperatorsInTopologicalOrder());
        return OperatorPrecedenceTracker::UnTrackPipelineWithoutLocking(pStreamingPipeline);
    }

    template<>
    bool PriorityPrecedenceTracker<NonPreemptiveMode, LeastRecentOperatorPriorityCalculator>::UnTrackPipeline(
            core::StreamingPipeline* pStreamingPipeline)
    {
        // These methods are called only when de-/registering operators. So it is OK to have a lock here
        std::lock_guard<std::mutex> lockGuard(modificationMutex_);
        schedulingQueue_.EraseAll(pStreamingPipeline->GetOperatorsInTopologicalOrder());
        return OperatorPrecedenceTracker::UnTrackPipelineWithoutLocking(pStreamingPipeline);
    }

    template<>
    void PriorityPrecedenceTracker<NonPreemptiveThroughputOptimizedMode,
            ThroughputOptimizedPriorityCalculator>::DoPostRunUpdates(SchedulingContext* prevSchedCtxtPtr,
            OperatorContext* prevOpCtxtPtr)
    {
        prevOpCtxtPtr->SetLastOperatorStatus(SchedulingDecisionContext::GetLastOperatorStatus());
        schedulingPolicy_.NotifyScheduleCompletion(prevOpCtxtPtr);
        SetPriorityPostRun(prevSchedCtxtPtr, prevOpCtxtPtr);
    }

    template<>
    void PriorityPrecedenceTracker<PreemptiveMode, RoundRobinPriorityCalculator>::DoPostRunUpdates(
            SchedulingContext* prevSchedCtxtPtr, OperatorContext* prevOpCtxtPtr)
    {
        prevOpCtxtPtr->SetLastOperatorStatus(SchedulingDecisionContext::GetLastOperatorStatus());
        schedulingPolicy_.NotifyScheduleCompletion(prevOpCtxtPtr);
        SetPriorityPostRun(prevSchedCtxtPtr, prevOpCtxtPtr);
    }

    template<>
    void PriorityPrecedenceTracker<NonPreemptiveBasicMode, RoundRobinPriorityCalculator>::DoPostRunUpdates(
            SchedulingContext* prevSchedCtxtPtr, OperatorContext* prevOpCtxtPtr)
    {
        prevOpCtxtPtr->SetLastOperatorStatus(SchedulingDecisionContext::GetLastOperatorStatus());
        schedulingPolicy_.NotifyScheduleCompletion(prevOpCtxtPtr);
        SetPriorityPostRun(prevSchedCtxtPtr, prevOpCtxtPtr);
    }

    template<>
    void PriorityPrecedenceTracker<NonPreemptiveThroughputOptimizedMode, MinCostPriorityCalculator>::DoPostRunUpdates(
            SchedulingContext* prevSchedCtxtPtr, OperatorContext* prevOpCtxtPtr)
    {
        prevOpCtxtPtr->SetLastOperatorStatus(SchedulingDecisionContext::GetLastOperatorStatus());
        schedulingPolicy_.NotifyScheduleCompletion(prevOpCtxtPtr);
        SetPriorityPostRun(prevSchedCtxtPtr, prevOpCtxtPtr);
    }

    template<>
    void PriorityPrecedenceTracker<NonPreemptiveBasicMode, FCFSPriorityCalculator>::DoPostRunUpdates(
            SchedulingContext* prevSchedCtxtPtr, OperatorContext* prevOpCtxtPtr)
    {
        prevOpCtxtPtr->SetLastOperatorStatus(SchedulingDecisionContext::GetLastOperatorStatus());
        schedulingPolicy_.NotifyScheduleCompletion(prevOpCtxtPtr);
        SetPriorityPostRun(prevSchedCtxtPtr, prevOpCtxtPtr);
    }
}// namespace enjima::runtime
