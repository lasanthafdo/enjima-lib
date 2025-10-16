//
// Created by m34ferna on 17/05/24.
//

#ifndef ENJIMA_STATE_BASED_TASK_RUNNER_H
#define ENJIMA_STATE_BASED_TASK_RUNNER_H

#include "Scheduler.h"
#include "enjima/operators/StreamingOperator.h"
#include "spdlog/spdlog.h"
#include <type_traits>

namespace enjima::runtime {

    template<StreamingTask::ProcessingMode ProcMode>
    class StateBasedTaskRunner {
    public:
        StateBasedTaskRunner(uint32_t taskId, Scheduler* schedulerPtr, std::atomic<bool>* taskRunningPtr);
        inline void Run();

    private:
        [[nodiscard]] std::string GetProcessingMode() const;
        inline uint8_t DoProcess();

        thread_local static operators::StreamingOperator* pNextOp_;

        const uint32_t taskId_;
        Scheduler* const pScheduler_;
        std::atomic<bool>* const pTaskRunning_;
    };
}// namespace enjima::runtime

#include "StateBasedTaskRunner.tpp"

#endif//ENJIMA_STATE_BASED_TASK_RUNNER_H
