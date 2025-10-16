//
// Created by m34ferna on 08/02/24.
//

#include "ThreadBasedTask.h"
#include "ThreadBasedTaskRunner.h"
#include <iostream>

namespace enjima::runtime {
    using namespace std::chrono_literals;

    ThreadBasedTask::ThreadBasedTask(operators::StreamingOperator* pStreamingOperator, ProcessingMode processingMode,
            metrics::Profiler* profilerPtr, ExecutionEngine* pExecutionEngine)
        : StreamingTask(processingMode,
                  std::string("tb_worker_").append(std::to_string(pStreamingOperator->GetOperatorId())),
                  pExecutionEngine),
          pStreamingOperator_(pStreamingOperator), profilerPtr_(profilerPtr)
    {
    }

    void ThreadBasedTask::Start()
    {
        taskRunning_.store(true, std::memory_order::release);
        taskFuture_ = std::async(std::launch::async, &ThreadBasedTask::Process, this);
    }

    void ThreadBasedTask::Process()
    {
        if (taskRunning_.load(std::memory_order::acquire)) {
            PinThreadToCpuListFromConfig(SupportedThreadType::kTBWorker);
            pthread_setname_np(pthread_self(), threadName_.c_str());
            switch (processingMode_) {
                case StreamingTask::ProcessingMode::kQueueBasedSingle: {
                    ThreadBasedTaskRunner<StreamingTask::ProcessingMode::kQueueBasedSingle> taskRunner{
                            pStreamingOperator_, this, &taskRunning_, &downstreamWaiting_, profilerPtr_};
                    taskRunner.Run();
                    break;
                }
                case StreamingTask::ProcessingMode::kBlockBasedBatch: {
                    ThreadBasedTaskRunner<StreamingTask::ProcessingMode::kBlockBasedBatch> taskRunner{
                            pStreamingOperator_, this, &taskRunning_, &downstreamWaiting_, profilerPtr_};
                    taskRunner.Run();
                    break;
                }
                case StreamingTask::ProcessingMode::kBlockBasedSingle:
                default: {
                    ThreadBasedTaskRunner<StreamingTask::ProcessingMode::kBlockBasedSingle> taskRunner{
                            pStreamingOperator_, this, &taskRunning_, &downstreamWaiting_, profilerPtr_};
                    taskRunner.Run();
                }
            }
        }
        TriggerPostCancellationTasks();
    }

    void ThreadBasedTask::Cancel()
    {
        InternalCancel(0);
    }

    void ThreadBasedTask::InternalCancel(operators::ChannelID notifyingChannelId)
    {
        if (upstreamTasksByChannelId_.contains(notifyingChannelId)) {
            numUpstreamCancelRequests_.fetch_add(1, std::memory_order::acq_rel);
        }
        if (taskRunning_.load(std::memory_order::acquire)) {
            if (numUpstreamCancelRequests_.load(std::memory_order::acquire) == upstreamTasksByChannelId_.size()) {
                spdlog::info("Cancelling operator {} (ID: {}) with {} downstream operator(s)",
                        pStreamingOperator_->GetOperatorName(), pStreamingOperator_->GetOperatorId(),
                        downstreamTasksByChannelId_.size());
                taskRunning_.store(false, std::memory_order::release);
            }
        }
        else {
            spdlog::warn("Operator {} (ID: {}) was already cancelled!", pStreamingOperator_->GetOperatorName(),
                    pStreamingOperator_->GetOperatorId());
        }
    }

    void ThreadBasedTask::ForceCancel()
    {
        taskRunning_.store(false, std::memory_order::release);
    }

    void ThreadBasedTask::TriggerPostCancellationTasks()
    {
        // upon cancellation process remaining downstream data and cancel to avoid data loss
        for (const auto& downstreamTaskChannelPair: downstreamTasksByChannelId_) {
            downstreamTaskChannelPair.second->InternalCancel(downstreamTaskChannelPair.first);
        }
        NotifyOutputAvailability();
    }

    bool ThreadBasedTask::IsTaskComplete()
    {
        if (!taskFuture_.valid()) {
            return true;
        }
        auto futureStatus = taskFuture_.wait_for(0ms);
        return futureStatus == std::future_status::ready;
    }

    bool ThreadBasedTask::IsTaskComplete(std::chrono::milliseconds waitMs)
    {
        if (!taskFuture_.valid()) {
            return true;
        }
        auto futureStatus = taskFuture_.wait_for(waitMs);
        return futureStatus == std::future_status::ready;
    }

    void ThreadBasedTask::WaitForUpstreamOutput(operators::ChannelID channelId)
    {
        if (upstreamTasksByChannelId_.contains(channelId)) {
            upstreamTasksByChannelId_.at(channelId)->WaitForOutput();
        }
    }

    void ThreadBasedTask::WaitForOutput()
    {
        if (!taskRunning_.load(std::memory_order::acquire)) {
            return;
        }
        downstreamWaiting_.store(true, std::memory_order::release);
        std::unique_lock<std::mutex> lock(cvMutex_);
        cv_.wait_for(lock, std::chrono::milliseconds(1),
                [&] { return !downstreamWaiting_.load(std::memory_order::acquire); });
    }

    void ThreadBasedTask::NotifyOutputAvailability()
    {
        downstreamWaiting_.store(false, std::memory_order::release);
        std::lock_guard<std::mutex> lockGuard{cvMutex_};
        cv_.notify_all();
    }

    void ThreadBasedTask::SetUpstreamTasks(
            const std::vector<std::pair<StreamingTask*, operators::ChannelID>>& upstreamTaskChannelPairs)
    {
        for (const auto& upstreamTaskChannelPair: upstreamTaskChannelPairs) {
            upstreamTasksByChannelId_.emplace(upstreamTaskChannelPair.second,
                    dynamic_cast<ThreadBasedTask*>(upstreamTaskChannelPair.first));
        }
    }

    void ThreadBasedTask::SetDownstreamTasks(
            const std::vector<std::pair<StreamingTask*, operators::ChannelID>>& downstreamTaskChannelPairs)
    {
        for (const auto& downstreamTaskChannelPair: downstreamTaskChannelPairs) {
            downstreamTasksByChannelId_.emplace(downstreamTaskChannelPair.second,
                    dynamic_cast<ThreadBasedTask*>(downstreamTaskChannelPair.first));
        }
    }

    operators::StreamingOperator* ThreadBasedTask::GetStreamingOperator() const
    {
        return pStreamingOperator_;
    }

    void ThreadBasedTask::WaitUntilFinished()
    {
        try {
            if (taskFuture_.valid()) {
                taskFuture_.get();
            }
        }
        catch (std::exception& e) {
            spdlog::error("Exception at ThreadBasedTask::WaitUntilFinished for operator {} with ID {}, Cause : {}",
                    pStreamingOperator_->GetOperatorName(), pStreamingOperator_->GetOperatorId(), e.what());
            ForceCancel();
            TriggerPostCancellationTasks();
        }
    }

}// namespace enjima::runtime
