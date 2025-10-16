//
// Created by m34ferna on 01/02/24.
//

#include "StateBasedTask.h"
#include "Scheduler.h"
#include "StateBasedTaskRunner.h"
#include "enjima/runtime/CancellationException.h"
#include "enjima/runtime/IllegalStateException.h"

namespace enjima::runtime {
    using ProcessingModeT = StreamingTask::ProcessingMode;

    StateBasedTask::StateBasedTask(Scheduler* scheduler, uint32_t taskId, ProcessingMode processingMode,
            ExecutionEngine* pExecutionEngine)
        : StreamingTask(processingMode, std::string("sb_worker_").append(std::to_string(taskId)), pExecutionEngine),
          scheduler_(scheduler), taskId_(taskId)
    {
    }

    void StateBasedTask::Start()
    {
        taskRunning_.store(true, std::memory_order::release);
        readyPromise_.set_value();
    }

    void StateBasedTask::Process()
    {
        try {
            readyPromise_.get_future().wait();
            pthread_setname_np(pthread_self(), threadName_.c_str());
            PinThreadToCpuListFromConfig(SupportedThreadType::kSBWorker);
            spdlog::info("Starting state-based task processing for worker {}", taskId_);
            scheduler_->RegisterTask();
            while (!scheduler_->ContainsRunnableOperators()) {}

            switch (processingMode_) {
                case ProcessingModeT::kBlockBasedBatch: {
                    StateBasedTaskRunner<ProcessingModeT::kBlockBasedBatch> taskRunner{taskId_, scheduler_,
                            &taskRunning_};
                    taskRunner.Run();
                    break;
                }
                case ProcessingModeT::kQueueBasedSingle: {
                    StateBasedTaskRunner<ProcessingModeT::kQueueBasedSingle> taskRunner{taskId_, scheduler_,
                            &taskRunning_};
                    taskRunner.Run();
                    break;
                }
                case ProcessingModeT::kBlockBasedSingle:
                default: {
                    StateBasedTaskRunner<ProcessingModeT::kBlockBasedSingle> taskRunner{taskId_, scheduler_,
                            &taskRunning_};
                    taskRunner.Run();
                }
            }
            spdlog::info("Stopping state-based task processing for worker {}", taskId_);
        }
        catch (const std::exception& e) {
            spdlog::error("State-based task with ID {} raised exception : {}", taskId_, e.what());
            sbTaskExceptionPtr_ = std::current_exception();
        }
        shutdownReadyPromise_.set_value();
    }

    void StateBasedTask::Cancel()
    {
        taskRunning_.store(false, std::memory_order::release);
    }

    void StateBasedTask::WaitUntilFinished()
    {
        if (taskRunning_.load(std::memory_order::acquire)) {
            Cancel();
        }
        auto shutdownReadyFuture = shutdownReadyPromise_.get_future();
        if (shutdownReadyFuture.valid()) {
            auto status = shutdownReadyFuture.wait_for(std::chrono::seconds(10));
            if (status == std::future_status::timeout) {
                throw CancellationException{std::string("Timeout when waiting to cancel state-based task with ID: ")
                                .append(std::to_string(taskId_))};
            }
            shutdownReadyFuture.get();
            t_.join();
            assert(!taskRunning_.load(std::memory_order::acquire) && !t_.joinable());
            if (sbTaskExceptionPtr_ != nullptr) {
                std::rethrow_exception(sbTaskExceptionPtr_);
            }
        }
        else {
            throw IllegalStateException{
                    std::string("State for future of shutdown-ready promise is not valid for task with ID: ")
                            .append(std::to_string(taskId_))};
        }
    }

    bool StateBasedTask::IsTaskComplete()
    {
        return !shutdownReadyPromise_.get_future().valid();
    }

    bool StateBasedTask::IsTaskComplete(std::chrono::milliseconds waitMs)
    {
        if (shutdownReadyPromise_.get_future().valid()) {
            return shutdownReadyPromise_.get_future().wait_for(waitMs) == std::future_status::ready;
        }
        return true;
    }

    StateBasedTask::~StateBasedTask() = default;

}// namespace enjima::runtime