//
// Created by m34ferna on 08/02/24.
//

#ifndef ENJIMA_THREAD_BASED_TASK_H
#define ENJIMA_THREAD_BASED_TASK_H

#include "enjima/operators/StreamingOperator.h"
#include "enjima/runtime/ExecutionEngine.fwd.h"
#include "enjima/runtime/StreamingTask.h"
#include <atomic>
#include <chrono>
#include <future>

namespace enjima::runtime {

    class ThreadBasedTask : public StreamingTask {
    public:
        ThreadBasedTask(operators::StreamingOperator* pStreamingOperator, ProcessingMode processingMode,
                metrics::Profiler* profilerPtr, ExecutionEngine* pExecutionEngine);
        void Start() override;
        void Cancel() override;
        void WaitUntilFinished() override;
        bool IsTaskComplete() override;
        bool IsTaskComplete(std::chrono::milliseconds waitMs) override;

        void SetUpstreamTasks(
                const std::vector<std::pair<StreamingTask*, operators::ChannelID>>& upstreamTaskChannelPairs);
        void SetDownstreamTasks(
                const std::vector<std::pair<StreamingTask*, operators::ChannelID>>& downstreamTaskChannelPairs);
        [[nodiscard]] operators::StreamingOperator* GetStreamingOperator() const;

        /**
         * @brief If the process method returns with its enjima::operators::StreamingOperator::DataInputStatus
         * set to kNoReadableInput, this method is called by the processing thread which would invoke an
         * upstream task's WaitForOutput()
         *
         * @see NotifyOutputAvailability()
         */
        void WaitForUpstreamOutput(operators::ChannelID channelId);

        /**
         * @brief If the process method returns with its enjima::operators::StreamingOperator::DataOutputStatus
         * set to kHasReadableOutput, this method is called by the processing thread if a downstream task
         * has previously called WaitForOutput()
         *
         * @see WaitForOutput()
         */
        void NotifyOutputAvailability();

        void ForceCancel();

    private:
        void Process() override;

        /**
         * @brief Called by downstream tasks usually when no more input is available to be processed
         *
         * @see WaitForUpstreamOutput()
         */
        void WaitForOutput();

        void TriggerPostCancellationTasks();
        void InternalCancel(operators::ChannelID notifyingChannelId);

        operators::StreamingOperator* pStreamingOperator_;
        UnorderedHashMapST<operators::ChannelID, ThreadBasedTask*> upstreamTasksByChannelId_;
        UnorderedHashMapST<operators::ChannelID, ThreadBasedTask*> downstreamTasksByChannelId_;
        std::atomic<uint32_t> numUpstreamCancelRequests_;
        std::future<void> taskFuture_;
        std::condition_variable cv_;
        std::mutex cvMutex_;
        std::atomic<bool> downstreamWaiting_{false};
        metrics::Profiler* profilerPtr_;
    };

}// namespace enjima::runtime

#endif//ENJIMA_THREAD_BASED_TASK_H
