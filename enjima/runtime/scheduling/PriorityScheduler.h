//
// Created by m34ferna on 02/02/24.
//

#ifndef ENJIMA_PRIORITY_SCHEDULER_H
#define ENJIMA_PRIORITY_SCHEDULER_H

#include "PriorityPrecedenceTracker.h"
#include "Scheduler.h"
#include "SchedulingDecisionContext.h"
#include "SchedulingTypes.h"
#include "spdlog/spdlog.h"

#if ENJIMA_METRICS_LEVEL >= 2
#include "enjima/metrics/MetricNames.h"
#endif


namespace enjima::runtime {

    template<PreemptModeType PreemptT, PriorityCalcType CalcT>
    class PriorityScheduler : public Scheduler {
    public:
        PriorityScheduler(uint64_t schedulingPeriodMs, metrics::Profiler* profilerPtr,
                StreamingTask::ProcessingMode processingMode, uint64_t eventThreshold, uint64_t idleThresholdMs, size_t defaultBlockSize);
        ~PriorityScheduler() override = default;

        [[nodiscard]] SchedulingDecisionContext* GetNextOperator() override;
        void Init() override;
        void Shutdown() override;
        void RegisterPipeline(core::StreamingPipeline* pStreamPipeline) override;
        void DeRegisterPipeline(core::StreamingPipeline* pStreamPipeline) override;
        void ActivateOperator(operators::StreamingOperator* pStreamOp) override;
        void DeactivateOperator(operators::StreamingOperator* pStreamOp, std::chrono::milliseconds waitMs) override;
        void RegisterTask() override;

    private:
        void Run();
        bool SleepUntilNextSchedulingUpdate();

        std::promise<void> readyPromise_;
        std::thread t_{&PriorityScheduler::Run, this};
        uint64_t schedulingPeriodMs_;
        uint64_t nextSchedulingUpdateAtMicros_;
        PriorityPrecedenceTracker<PreemptT, CalcT> priorityPrecedenceTracker_;
        metrics::Profiler* profilerPtr_;
        std::condition_variable sleepCv_;
        std::mutex sleepCvMutex_;
        thread_local static SchedulingContext* schedCtxtPtr_;
        thread_local static SchedulingDecisionContext* decisionCtxtPtr_;
        std::exception_ptr schedExceptionPtr_;

#ifdef ENABLE_THREAD_PARKING
        std::mutex threadParkingCvMutex_;
        std::condition_variable threadParkingCv_;
#endif

#if ENJIMA_METRICS_LEVEL >= 2
        metrics::Counter<uint64_t>* totSchedTimeCounter_{nullptr};
        metrics::Counter<uint64_t>* totSchedCount_{nullptr};
        metrics::Counter<uint64_t>* totUpdateTimeCounter_{nullptr};
        metrics::Counter<uint64_t>* totUpdateCount_{nullptr};
#endif
    };

    template<PriorityCalcType CalcT>
    class PriorityScheduler<PreemptiveMode, CalcT> : public Scheduler {
    public:
        using SchedHashMapT = ConcurrentUnorderedHashMap<std::thread::id, SchedulingContext*>;

        PriorityScheduler(uint64_t schedulingPeriodMs, metrics::Profiler* profilerPtr,
                StreamingTask::ProcessingMode processingMode, uint64_t eventThreshold, uint64_t idleThresholdMs);
        ~PriorityScheduler() override = default;

        [[nodiscard]] SchedulingDecisionContext* GetNextOperator() override;
        void Init() override;
        void Shutdown() override;
        void RegisterPipeline(core::StreamingPipeline* pStreamPipeline) override;
        void DeRegisterPipeline(core::StreamingPipeline* pStreamPipeline) override;
        void ActivateOperator(operators::StreamingOperator* pStreamOp) override;
        void DeactivateOperator(operators::StreamingOperator* pStreamOp, std::chrono::milliseconds waitMs) override;
        void RegisterTask() override;

    private:
        void Run();
        bool SleepUntilNextSchedulingUpdate();

        std::promise<void> readyPromise_;
        std::thread t_{&PriorityScheduler::Run, this};
        uint64_t schedulingPeriodMs_;
        uint64_t nextSchedulingUpdateAtMicros_;
        PriorityPrecedenceTracker<PreemptiveMode, CalcT> priorityPrecedenceTracker_;
        metrics::Profiler* profilerPtr_;
        std::condition_variable sleepCv_;
        std::mutex sleepCvMutex_;
        SchedHashMapT scheduledOperatorsByThreadID_;
        std::exception_ptr schedExceptionPtr_;

#ifdef ENABLE_THREAD_PARKING
        std::mutex threadParkingCvMutex_;
        std::condition_variable threadParkingCv_;
#endif

#if ENJIMA_METRICS_LEVEL >= 2
        metrics::Counter<uint64_t>* totSchedTimeCounter_{nullptr};
        metrics::Counter<uint64_t>* totSchedCount_{nullptr};
        metrics::Counter<uint64_t>* totUpdateTimeCounter_{nullptr};
        metrics::Counter<uint64_t>* totUpdateCount_{nullptr};
#endif
    };

}// namespace enjima::runtime

#include "PreemptivePriorityScheduler.tpp"
#include "PriorityScheduler.tpp"

#endif//ENJIMA_PRIORITY_SCHEDULER_H
