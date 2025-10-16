//
// Created by m34ferna on 08/02/24.
//

#ifndef ENJIMA_STREAMING_TASK_H
#define ENJIMA_STREAMING_TASK_H

#include "ExecutionEngine.fwd.h"
#include "RuntimeTypes.h"
#include "enjima/operators/OperatorsInternals.fwd.h"

#include <atomic>
#include <chrono>
#include <future>

namespace enjima::runtime {

    class StreamingTask {
    public:
        enum class ProcessingMode { kBlockBasedSingle, kBlockBasedBatch, kQueueBasedSingle, kUnassigned };

        StreamingTask(ProcessingMode processingMode, std::string threadName, ExecutionEngine* pExecutionEngine);
        virtual ~StreamingTask() = default;

        virtual void Start() = 0;
        virtual void Cancel() = 0;
        virtual void WaitUntilFinished() = 0;
        virtual bool IsTaskComplete() = 0;
        virtual bool IsTaskComplete(std::chrono::milliseconds waitMs) = 0;

        void PinThreadToCpuListFromConfig(SupportedThreadType threadType);

    protected:
        ProcessingMode processingMode_{ProcessingMode::kBlockBasedSingle};
        std::atomic<bool> taskRunning_{false};
        std::string threadName_{"worker"};

    private:
        virtual void Process() = 0;

        ExecutionEngine* pExecutionEngine_;
    };

}// namespace enjima::runtime

#endif//ENJIMA_STREAMING_TASK_H
