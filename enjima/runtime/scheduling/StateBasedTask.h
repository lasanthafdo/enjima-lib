//
// Created by m34ferna on 01/02/24.
//

#ifndef ENJIMA_STATE_BASED_TASK_H
#define ENJIMA_STATE_BASED_TASK_H

#include "enjima/operators/StreamingOperator.h"
#include "enjima/runtime/ExecutionEngine.h"
#include "enjima/runtime/StreamingTask.h"
#include <future>
#include <thread>

namespace enjima::runtime {

    class StateBasedTask : public StreamingTask {
    public:
        StateBasedTask(Scheduler* scheduler, uint32_t taskId, ProcessingMode processingMode,
                ExecutionEngine* pExecutionEngine);
        ~StateBasedTask() override;

        void Start() override;
        void Cancel() override;
        void WaitUntilFinished() override;
        bool IsTaskComplete() override;
        bool IsTaskComplete(std::chrono::milliseconds waitMs) override;

    private:
        void Process() override;

        Scheduler* scheduler_;
        std::promise<void> readyPromise_;
        std::promise<void> shutdownReadyPromise_;
        std::thread t_{&StateBasedTask::Process, this};
        uint32_t taskId_;
        std::exception_ptr sbTaskExceptionPtr_{nullptr};
    };
}// namespace enjima::runtime

#endif//ENJIMA_STATE_BASED_TASK_H
