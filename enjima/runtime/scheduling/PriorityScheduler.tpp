//
// Created by m34ferna on 02/02/24.
//

namespace enjima::runtime {

    template<PreemptModeType PreemptT, PriorityCalcType CalcT>
    thread_local SchedulingContext* PriorityScheduler<PreemptT, CalcT>::schedCtxtPtr_ =
            const_cast<SchedulingContext*>(&PriorityPrecedenceTracker<PreemptT, CalcT>::kIneligibleSchedCtxt);

    template<PreemptModeType PreemptT, PriorityCalcType CalcT>
    thread_local SchedulingDecisionContext* PriorityScheduler<PreemptT, CalcT>::decisionCtxtPtr_ = nullptr;

    template<PreemptModeType PreemptT, PriorityCalcType CalcT>
    PriorityScheduler<PreemptT, CalcT>::PriorityScheduler(uint64_t schedulingPeriodMs, metrics::Profiler* profilerPtr,
            StreamingTask::ProcessingMode processingMode, uint64_t eventThreshold, uint64_t idleThresholdMs,
            size_t defaultBlockSize)
        : schedulingPeriodMs_(schedulingPeriodMs),
          nextSchedulingUpdateAtMicros_(enjima::runtime::GetSteadyClockMicros() + schedulingPeriodMs_),
          priorityPrecedenceTracker_(profilerPtr, schedulingPeriodMs, eventThreshold, idleThresholdMs,
                  defaultBlockSize),
          profilerPtr_(profilerPtr)
    {
        assert(processingMode != StreamingTask::ProcessingMode::kUnassigned);
        processingMode_ = processingMode;
    }

    template<PreemptModeType PreemptT, PriorityCalcType CalcT>
    SchedulingDecisionContext* PriorityScheduler<PreemptT, CalcT>::GetNextOperator()
    {
#if ENJIMA_METRICS_LEVEL >= 2
        // Only enable if metrics are enabled to this level
        auto schedStartTimeMicros = GetSteadyClockMicros();
#endif
        schedCtxtPtr_ = priorityPrecedenceTracker_.GetNextInPrecedence(schedCtxtPtr_);
        decisionCtxtPtr_ = schedCtxtPtr_->GetDecisionCtxtPtr();
#if ENJIMA_METRICS_LEVEL >= 2
        // Only enable if metrics are enabled to this level
        totSchedCount_->IncRelaxed();
        totSchedTimeCounter_->IncRelaxed(GetSteadyClockMicros() - schedStartTimeMicros);
#endif
#ifdef ENABLE_THREAD_PARKING
        if (decisionCtxtPtr_->GetScheduleStatus() == SchedulingDecisionContext::ScheduleStatus::kIneligible) {
            std::unique_lock<std::mutex> cvLock{threadParkingCvMutex_};
            auto epochAtWait = priorityPrecedenceTracker_.GetSchedulingEpoch();
            threadParkingCv_.wait(cvLock,
                    [&epochAtWait, this] { return priorityPrecedenceTracker_.GetSchedulingEpoch() != epochAtWait; });
        }
#endif
        return decisionCtxtPtr_;
    }

    template<PreemptModeType PreemptT, PriorityCalcType CalcT>
    void PriorityScheduler<PreemptT, CalcT>::Run()
    {
        try {
            pthread_setname_np(pthread_self(), std::string("scheduler_1").c_str());
            readyPromise_.get_future().wait();
            spdlog::info("Started Priority Scheduler in default mode");
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
            spdlog::error("Non-preemptive priority scheduler thread raised exception : {}", e.what());
            schedExceptionPtr_ = std::current_exception();
        }
    }

    template<PreemptModeType PreemptT, PriorityCalcType CalcT>
    void PriorityScheduler<PreemptT, CalcT>::Init()
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

    template<PreemptModeType PreemptT, PriorityCalcType CalcT>
    void PriorityScheduler<PreemptT, CalcT>::Shutdown()
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

    template<PreemptModeType PreemptT, PriorityCalcType CalcT>
    bool PriorityScheduler<PreemptT, CalcT>::SleepUntilNextSchedulingUpdate()
    {
        auto currentTimeMicros = GetSteadyClockMicros();
        if ((currentTimeMicros + 1) < nextSchedulingUpdateAtMicros_) {
            // Need to wait at millisecond resolution since waiting at microsecond resolution has a
            // performance hit in end-to-end benchmarks
            auto sleepTimeMillis = (nextSchedulingUpdateAtMicros_ - currentTimeMicros - 1) / 1000;
            std::unique_lock<std::mutex> lock(sleepCvMutex_);
            sleepCv_.wait_for(lock, std::chrono::milliseconds(sleepTimeMillis),
                    [&] { return !schedulerRunning_.load(std::memory_order::acquire); });
            if (!schedulerRunning_.load(std::memory_order::acquire)) {
                return false;
            }
        }
        nextSchedulingUpdateAtMicros_ = GetSteadyClockMicros() + (schedulingPeriodMs_ * 1000);
        return true;
    }

    template<PreemptModeType PreemptT, PriorityCalcType CalcT>
    void PriorityScheduler<PreemptT, CalcT>::RegisterPipeline(core::StreamingPipeline* pStreamPipeline)
    {
        priorityPrecedenceTracker_.TrackPipeline(pStreamPipeline);
        Scheduler::RegisterPipeline(pStreamPipeline);
    }

    template<PreemptModeType PreemptT, PriorityCalcType CalcT>
    void PriorityScheduler<PreemptT, CalcT>::DeRegisterPipeline(core::StreamingPipeline* pStreamPipeline)
    {
        Scheduler::DeRegisterPipeline(pStreamPipeline);
        priorityPrecedenceTracker_.UnTrackPipeline(pStreamPipeline);
    }

    template<PreemptModeType PreemptT, PriorityCalcType CalcT>
    void PriorityScheduler<PreemptT, CalcT>::ActivateOperator(operators::StreamingOperator* pStreamOp)
    {
        priorityPrecedenceTracker_.ActivateOperator(pStreamOp);
    }

    template<PreemptModeType PreemptT, PriorityCalcType CalcT>
    void PriorityScheduler<PreemptT, CalcT>::DeactivateOperator(operators::StreamingOperator* pStreamOp,
            std::chrono::milliseconds waitMs)
    {
        priorityPrecedenceTracker_.DeactivateOperator(pStreamOp, waitMs);
    }

    template<PreemptModeType PreemptT, PriorityCalcType CalcT>
    void PriorityScheduler<PreemptT, CalcT>::RegisterTask()
    {
        decisionCtxtPtr_ = const_cast<SchedulingDecisionContext*>(
                &PriorityPrecedenceTracker<PreemptT, CalcT>::kIneligibleDecisionCtxt);
    }
}// namespace enjima::runtime
