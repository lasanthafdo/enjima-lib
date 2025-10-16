//
// Created by m34ferna on 02/02/24.
//

namespace enjima::runtime {

    template<PriorityCalcType CalcT>
    PriorityScheduler<PreemptiveMode, CalcT>::PriorityScheduler(uint64_t schedulingPeriodMs,
            metrics::Profiler* profilerPtr, StreamingTask::ProcessingMode processingMode, uint64_t eventThreshold,
            uint64_t idleThresholdMs)
        : schedulingPeriodMs_(schedulingPeriodMs),
          nextSchedulingUpdateAtMicros_(GetSteadyClockMicros() + schedulingPeriodMs_),
          priorityPrecedenceTracker_(profilerPtr, schedulingPeriodMs, eventThreshold, idleThresholdMs, 0),
          profilerPtr_(profilerPtr)
    {
        assert(processingMode != StreamingTask::ProcessingMode::kUnassigned);
        processingMode_ = processingMode;
    }

    template<PriorityCalcType CalcT>
    SchedulingDecisionContext* PriorityScheduler<PreemptiveMode, CalcT>::GetNextOperator()
    {
#if ENJIMA_METRICS_LEVEL >= 2
        // Only enable if metrics are enabled to this level
        auto schedStartTimeMicros = GetSteadyClockMicros();
#endif
        SchedulingDecisionContext* decisionCtxtPtr = nullptr;
        auto threadId = std::this_thread::get_id();
        SchedHashMapT::accessor hmAccessor;
        if (scheduledOperatorsByThreadID_.extract(threadId, hmAccessor)) {
            auto& prevSchedCtxtPtr = *hmAccessor;
            const auto nextSchedCtxtPtr = priorityPrecedenceTracker_.GetNextInPrecedence(prevSchedCtxtPtr);
            auto emplaceAccessorBool = scheduledOperatorsByThreadID_.get_or_emplace(threadId, nextSchedCtxtPtr);
            assert(emplaceAccessorBool.second);
            decisionCtxtPtr = (*emplaceAccessorBool.first)->GetDecisionCtxtPtr();
        }
#if ENJIMA_METRICS_LEVEL >= 2
        // Only enable if metrics are enabled to this level
        totSchedCount_->IncRelaxed();
        totSchedTimeCounter_->IncRelaxed(GetSteadyClockMicros() - schedStartTimeMicros);
#endif

#ifdef ENABLE_THREAD_PARKING
        assert(decisionCtxtPtr != nullptr);
        if (decisionCtxtPtr->GetScheduleStatus() == SchedulingDecisionContext::ScheduleStatus::kIneligible) {
            std::unique_lock<std::mutex> cvLock{threadParkingCvMutex_};
            auto epochAtWait = priorityPrecedenceTracker_.GetSchedulingEpoch();
            threadParkingCv_.wait(cvLock,
                    [&epochAtWait, this] { return priorityPrecedenceTracker_.GetSchedulingEpoch() != epochAtWait; });
        }
#endif
        return decisionCtxtPtr;
    }

    template<PriorityCalcType CalcT>
    void PriorityScheduler<PreemptiveMode, CalcT>::Run()
    {
        try {
            pthread_setname_np(pthread_self(), std::string("scheduler_1").c_str());
            readyPromise_.get_future().wait();
            spdlog::info("Started Priority Scheduler in preemptive mode");
            if (priorityPrecedenceTracker_.IsInitialized()) {
                spdlog::info("Confirmed precedence tracker to be initialized");
            }
            else {
                spdlog::warn("Precedence tracker not initialized!");
            }
            while (schedulerRunning_.load(std::memory_order::acquire) && SleepUntilNextSchedulingUpdate()) {
#if ENJIMA_METRICS_LEVEL >= 2
                auto updateTimeStartMicros = GetSteadyClockMicros();
#endif
                priorityPrecedenceTracker_.UpdatePriority(nextSchedulingUpdateAtMicros_);
                for (auto& [threadId, schedCtxtPtr]: scheduledOperatorsByThreadID_) {
                    auto decisionCtxtPtr = schedCtxtPtr->GetDecisionCtxtPtr();
                    if (decisionCtxtPtr->GetScheduleStatus() !=
                            SchedulingDecisionContext::ScheduleStatus::kIneligible) {
                        decisionCtxtPtr->MarkEndOfSchedule();
                    }
                }
#if ENJIMA_METRICS_LEVEL >= 2
                totUpdateTimeCounter_->IncRelaxed(GetSteadyClockMicros() - updateTimeStartMicros);
                totUpdateCount_->IncRelaxed();
#endif

#ifdef ENABLE_THREAD_PARKING
                std::lock_guard<std::mutex> lockGuard{threadParkingCvMutex_};
                threadParkingCv_.notify_all();
#endif
            }
            spdlog::info("Stopping Priority Scheduler");
        }
        catch (const std::exception& e) {
            spdlog::error("Preemptive priority scheduler thread raised exception : {}", e.what());
            schedExceptionPtr_ = std::current_exception();
        }
    }

    template<PriorityCalcType CalcT>
    void PriorityScheduler<PreemptiveMode, CalcT>::Init()
    {
#if ENJIMA_METRICS_LEVEL >= 2
        totSchedTimeCounter_ = profilerPtr_->GetOrCreateCounter(metrics::kSchedulingTimeCounterLabel);
        totSchedCount_ = profilerPtr_->GetOrCreateCounter(metrics::kSchedulingCountCounterLabel);
        totUpdateTimeCounter_ = profilerPtr_->GetOrCreateCounter(metrics::kPriorityUpdateTimeCounterLabel);
        totUpdateCount_ = profilerPtr_->GetOrCreateCounter(metrics::kPriorityUpdateCountCounterLabel);
#endif
        readyPromise_.set_value();
        Scheduler::Init();
    }

    template<PriorityCalcType CalcT>
    void PriorityScheduler<PreemptiveMode, CalcT>::Shutdown()
    {
        Scheduler::Shutdown();
        {
            std::lock_guard<std::mutex> lockGuard{sleepCvMutex_};
            sleepCv_.notify_all();
        }
        t_.join();
        assert(!schedulerRunning_.load(std::memory_order::acquire) && !t_.joinable());
        if (schedExceptionPtr_ != nullptr) {
            std::rethrow_exception(schedExceptionPtr_);
        }
    }

    template<PriorityCalcType CalcT>
    bool PriorityScheduler<PreemptiveMode, CalcT>::SleepUntilNextSchedulingUpdate()
    {
        auto currentTimeMicros = GetSteadyClockMicros();
        if ((currentTimeMicros + 1) < nextSchedulingUpdateAtMicros_) {
            auto sleepTimeMicros = nextSchedulingUpdateAtMicros_ - currentTimeMicros - 1;
            std::unique_lock<std::mutex> lock(sleepCvMutex_);
            sleepCv_.wait_for(lock, std::chrono::microseconds(sleepTimeMicros),
                    [&] { return !schedulerRunning_.load(std::memory_order::acquire); });
            if (!schedulerRunning_.load(std::memory_order::acquire)) {
                return false;
            }
        }
        nextSchedulingUpdateAtMicros_ = GetSteadyClockMicros() + (schedulingPeriodMs_ * 1000);
        return true;
    }

    template<PriorityCalcType CalcT>
    void PriorityScheduler<PreemptiveMode, CalcT>::RegisterPipeline(core::StreamingPipeline* pStreamPipeline)
    {
        priorityPrecedenceTracker_.TrackPipeline(pStreamPipeline);
        Scheduler::RegisterPipeline(pStreamPipeline);
    }

    template<PriorityCalcType CalcT>
    void PriorityScheduler<PreemptiveMode, CalcT>::DeRegisterPipeline(core::StreamingPipeline* pStreamPipeline)
    {
        Scheduler::DeRegisterPipeline(pStreamPipeline);
        priorityPrecedenceTracker_.UnTrackPipeline(pStreamPipeline);
    }

    template<PriorityCalcType CalcT>
    void PriorityScheduler<PreemptiveMode, CalcT>::ActivateOperator(operators::StreamingOperator* pStreamOp)
    {
        priorityPrecedenceTracker_.ActivateOperator(pStreamOp);
    }

    template<PriorityCalcType CalcT>
    void PriorityScheduler<PreemptiveMode, CalcT>::DeactivateOperator(operators::StreamingOperator* pStreamOp,
            std::chrono::milliseconds waitMs)
    {
        priorityPrecedenceTracker_.DeactivateOperator(pStreamOp, waitMs);
    }

    template<PriorityCalcType CalcT>
    void PriorityScheduler<PreemptiveMode, CalcT>::RegisterTask()
    {
        auto threadId = std::this_thread::get_id();
        scheduledOperatorsByThreadID_.emplace(threadId,
                const_cast<SchedulingContext*>(
                        &PriorityPrecedenceTracker<PreemptiveMode, CalcT>::kIneligibleSchedCtxt));
    }
}// namespace enjima::runtime
