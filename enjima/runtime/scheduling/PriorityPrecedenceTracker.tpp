//
// Created by m34ferna on 14/05/24.
//

namespace enjima::runtime {

    template<PreemptModeType PreemptT, PriorityCalcType CalcT>
    SchedulingContext* PriorityPrecedenceTracker<PreemptT, CalcT>::GetNextInPrecedence(
            SchedulingContext*& prevSchedCtxtPtr)
    {
        auto prevDecisionCtxtPtr = prevSchedCtxtPtr->GetDecisionCtxtPtr();
        if (prevDecisionCtxtPtr->GetScheduleStatus() != SchedulingDecisionContext::ScheduleStatus::kIneligible) {
            auto* prevOpCtxtPtr = prevSchedCtxtPtr->GetOpCtxtPtr();
            if (!prevOpCtxtPtr->TrySetStatusDeScheduled()) {
                // If we couldn't de-schedule, it must be in progress of cancellation
                prevOpCtxtPtr->TrySetStatusReadyToCancel();
            }
            else {
                DoPostRunUpdates(prevSchedCtxtPtr, prevOpCtxtPtr);
                schedulingQueue_.Push(prevSchedCtxtPtr);
            }
            UpdateCostMetricsForOperator(prevDecisionCtxtPtr);
            prevDecisionCtxtPtr->SetDecisionToIneligible();
        }
        while (true) {
            SchedulingContext* nextSchedCtxtPtr = schedulingQueue_.Pop();
            if (nextSchedCtxtPtr != nullptr) {
                auto* candidateOpCtxtPtr = nextSchedCtxtPtr->GetOpCtxtPtr();
                auto* decisionCtxtPtr = nextSchedCtxtPtr->GetDecisionCtxtPtr();
                if (candidateOpCtxtPtr->TrySetStatusScheduled()) {
                    auto nextOpPtr = candidateOpCtxtPtr->GetOperatorPtr();
                    if (!candidateOpCtxtPtr->IsWaitingToCancel()) {
                        auto processedCounter = get<4>(metricsMap_.at(nextOpPtr));
                        uint64_t stopAtCount =
                                preemptModeCalc_.CalculateNumEventsToProcess(nextOpPtr) + processedCounter->GetCount();
                        decisionCtxtPtr->SetDecision(SchedulingDecisionContext::ScheduleStatus::kNormal, stopAtCount);
                        return nextSchedCtxtPtr;
                    }
                    else {
                        auto expectedStatus = OperatorContext::kScheduled;
                        if (nextOpPtr->IsSourceOperator()) {
                            candidateOpCtxtPtr->TrySetStatus(expectedStatus, OperatorContext::kReadyToCancel);
                            // This scheduling context can be discarded since the operator is now ready to cancel
                        }
                        else {
                            candidateOpCtxtPtr->TrySetStatus(expectedStatus, OperatorContext::kCancellationInProgress);
                            decisionCtxtPtr->SetDecision(SchedulingDecisionContext::ScheduleStatus::kToBeCancelled, 0);
                            return nextSchedCtxtPtr;
                        }
                    }
                }
                else {
                    return const_cast<SchedulingContext*>(&kIneligibleSchedCtxt);
                }
            }
            else {
                break;
            }
        }
        return const_cast<SchedulingContext*>(&kIneligibleSchedCtxt);
    }

#ifdef ENJIMA_WORKER_PRIORITY_UPDATES_DISABLED
    template<PreemptModeType PreemptT, PriorityCalcType CalcT>
    void PriorityPrecedenceTracker<PreemptT, CalcT>::DoPostRunUpdates(
            [[maybe_unused]] SchedulingContext* prevSchedCtxtPtr, OperatorContext* prevOpCtxtPtr)
    {
        prevOpCtxtPtr->SetLastOperatorStatus(SchedulingDecisionContext::GetLastOperatorStatus());
    }
#else
    template<PreemptModeType PreemptT, PriorityCalcType CalcT>
    void PriorityPrecedenceTracker<PreemptT, CalcT>::DoPostRunUpdates(SchedulingContext* prevSchedCtxtPtr,
            OperatorContext* prevOpCtxtPtr)
    {
        prevOpCtxtPtr->SetLastOperatorStatus(SchedulingDecisionContext::GetLastOperatorStatus());
        SetPriorityPostRun(prevSchedCtxtPtr, prevOpCtxtPtr);
    }
#endif

    template<PreemptModeType PreemptT, PriorityCalcType CalcT>
    void PriorityPrecedenceTracker<PreemptT, CalcT>::SetPriorityPostRun(SchedulingContext* prevSchedCtxtPtr,
            const OperatorContext* prevOpCtxtPtr)
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
                if (pipelinePtr->IsWaitingToCancel()) {
                    const auto totalOps = pipelinePtr->GetOperatorsInTopologicalOrder().size();
                    priority = totalOps - downstreamOpsByOpPtrMap_.at(opPtr).size();
                }
            }
            else {
                priority = schedulingPolicy_.CalculatePriority(prevOpCtxtPtr);
            }
        }
        prevSchedCtxtPtr->SetPriority(priority);
    }

    template<PreemptModeType PreemptT, PriorityCalcType CalcT>
    void PriorityPrecedenceTracker<PreemptT, CalcT>::UpdatePriority(uint64_t nextSchedulingUpdateAtMicros)
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
                            priority = totalOps - downstreamOpsByOpPtrMap_.at(opPtr).size();
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

    template<PreemptModeType PreemptT, PriorityCalcType CalcT>
    void PriorityPrecedenceTracker<PreemptT, CalcT>::Init()
    {
#if ENJIMA_METRICS_LEVEL >= 3
        vecLockTimeAvgGauge_ = profilerPtr_->GetOrCreateDoubleAverageGauge(metrics::kSchedVecLockTimeGaugeLabel);
        totCalcTimeAvgGauge_ = profilerPtr_->GetOrCreateDoubleAverageGauge(metrics::kSchedTotCalcTimeGaugeLabel);
        priorityUpdateTimeAvgGauge_ =
                profilerPtr_->GetOrCreateDoubleAverageGauge(metrics::kSchedPriorityUpdateTimeGaugeLabel);
#endif
        schedulingQueue_.Init();
        initialized_.store(true, std::memory_order::release);
    }

    template<PreemptModeType PreemptT, PriorityCalcType CalcT>
    bool PriorityPrecedenceTracker<PreemptT, CalcT>::IsInitialized()
    {
        return initialized_.load(std::memory_order::acquire);
    }

    template<PreemptModeType PreemptT, PriorityCalcType CalcT>
    void PriorityPrecedenceTracker<PreemptT, CalcT>::UpdateCostMetricsForOperator(
            SchedulingDecisionContext* prevDecisionCtxtPtr)
    {
        auto opPtr = prevDecisionCtxtPtr->GetOperatorPtr();
        auto costGauge = get<2>(metricsMap_.at(opPtr));
        auto numProcessed = prevDecisionCtxtPtr->GetNumProcessed();
        auto scheduledTime = prevDecisionCtxtPtr->GetScheduledCpuTime();
        if (numProcessed == 0) {
            scheduledTime = 0;
        }
        costGauge->UpdateCostMetrics(numProcessed, scheduledTime);
    }

    template<PreemptModeType PreemptT, PriorityCalcType CalcT>
    uint64_t PriorityPrecedenceTracker<PreemptT, CalcT>::GetNumPendingEvents(operators::StreamingOperator* opPtr) const
    {
        auto metricsTuple = metricsMap_.at(opPtr);
        if (opPtr->IsSourceOperator()) {
            auto costGauge = get<2>(metricsTuple);
            auto sourcePendingEvents = std::lround(
                    static_cast<double>(GetSteadyClockMicros() - nextSchedulingUpdateAtMicros_) / costGauge->GetVal());
            return sourcePendingEvents;
        }
        auto eventGauge = get<0>(metricsTuple);
        return eventGauge->GetVal();
    }

    template<PreemptModeType PreemptT, PriorityCalcType CalcT>
    void PriorityPrecedenceTracker<PreemptT, CalcT>::InitOpMetrics(operators::StreamingOperator* opPtr)
    {
        const auto& opName = opPtr->GetOperatorName();
        auto operatorCostGauge = profilerPtr_->GetOperatorCostGauge(opName);
        auto operatorSelectivityGauge = profilerPtr_->GetOperatorSelectivityGauge(opName);
        auto throughputGauge = profilerPtr_->GetInThroughputGauge(opName);
        auto pendingEventsGauge = profilerPtr_->GetPendingEventsGauge(opName);
        metrics::Counter<uint64_t>* processedCounter;
        if (opPtr->IsSourceOperator()) {
            processedCounter = profilerPtr_->GetOutCounter(opName);
        }
        else {
            processedCounter = profilerPtr_->GetInCounter(opName);
        }
        auto success =
                metricsMap_.emplace(opPtr, SchedulingMetricTupleT{pendingEventsGauge, throughputGauge,
                                                   operatorCostGauge, operatorSelectivityGauge, processedCounter});
        if (!success.second) {
            throw runtime::AlreadyExistsException{"Operator metrics already exist!"};
        }
    }

    template<PreemptModeType PreemptT, PriorityCalcType CalcT>
    void PriorityPrecedenceTracker<PreemptT, CalcT>::TrackPipeline(core::StreamingPipeline* pStreamingPipeline)
    {
        // These methods are called only when de-/registering operators. So it is OK to have a lock here
        std::lock_guard<std::mutex> lockGuard(modificationMutex_);
        std::vector<OperatorContext*> pipelineSpecificOpCtxtPtrsVec;
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
            pipelineSpecificOpCtxtPtrsVec.emplace_back(opCtxtPtr);
            schedCtxtMap_.emplace(opCtxtPtr, schedCtxtPtr);
            std::vector<operators::StreamingOperator*> downstreamOps =
                    pStreamingPipeline->GetAllDownstreamOps(pStreamOp);
            downstreamOpsByOpPtrMap_.emplace(pStreamOp, downstreamOps);
            opPipelineMap_.emplace(opCtxtPtr, pStreamingPipeline);
        }
        currentPipelines_.emplace_back(pStreamingPipeline);
        schedulingPolicy_.InitializeMetricsForPipeline(pStreamingPipeline, pipelineSpecificOpCtxtPtrsVec);
    }

    template<PreemptModeType PreemptT, PriorityCalcType CalcT>
    bool PriorityPrecedenceTracker<PreemptT, CalcT>::UnTrackPipeline(core::StreamingPipeline* pStreamingPipeline)
    {
        // These methods are called only when de-/registering operators. So it is OK to have a lock here
        std::lock_guard<std::mutex> lockGuard(modificationMutex_);
        schedulingPolicy_.DeactivateMetricsForPipeline(pStreamingPipeline);
        schedulingQueue_.EraseAll(pStreamingPipeline->GetOperatorsInTopologicalOrder());
        return OperatorPrecedenceTracker::UnTrackPipelineWithoutLocking(pStreamingPipeline);
    }

    template<PreemptModeType PreemptT, PriorityCalcType CalcT>
    uint64_t PriorityPrecedenceTracker<PreemptT, CalcT>::GetSchedulingEpoch() const
    {
        return schedulingEpoch_.load(std::memory_order::acquire);
    }

}// namespace enjima::runtime
