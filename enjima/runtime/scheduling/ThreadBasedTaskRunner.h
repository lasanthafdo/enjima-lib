//
// Created by m34ferna on 10/06/24.
//

#ifndef ENJIMA_THREAD_BASED_TASK_RUNNER_H
#define ENJIMA_THREAD_BASED_TASK_RUNNER_H

#include "ThreadBasedTask.h"
#include "enjima/runtime/StreamingTask.h"
#include "spdlog/spdlog.h"

namespace enjima::runtime {

    template<StreamingTask::ProcessingMode ProcMode>
    class ThreadBasedTaskRunner {
    public:
        ThreadBasedTaskRunner(operators::StreamingOperator* streamingOperatorPtr, ThreadBasedTask* tbTaskPtr,
                std::atomic<bool>* taskRunningPtr, std::atomic<bool>* downstreamWaitingPtr,
                metrics::Profiler* profilerPtr);
        void Run();

    private:
        void MarkStartOfWork();
        void MarkEndOfWork();

        operators::StreamingOperator* pStreamingOperator_;
        ThreadBasedTask* pThreadBasedTask_;
        std::atomic<bool>* pTaskRunning_;
        std::atomic<bool>* pDownstreamWaiting_;
        uint64_t cpuTimeAtStartOfWork_{0};
        uint64_t numProcessedAtStartOfWork_{0};
        metrics::Counter<uint64_t>* processedCounter_{nullptr};
        metrics::OperatorCostGauge* costGauge_{nullptr};
    };

}// namespace enjima::runtime

#include "ThreadBasedTaskRunner.tpp"

#endif//ENJIMA_THREAD_BASED_TASK_RUNNER_H
