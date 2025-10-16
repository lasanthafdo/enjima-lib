//
// Created by m34ferna on 03/02/24.
//

#ifndef ENJIMA_SINK_OPERATOR_H
#define ENJIMA_SINK_OPERATOR_H

#include <iostream>

#include "OperatorUtil.h"
#include "StreamingOperator.h"
#include "enjima/memory/MemoryBlock.h"
#include "enjima/metrics/MetricNames.h"
#include "enjima/queueing/RecordQueueBase.h"

namespace enjima::operators {

    template<typename TInput, typename Duration = std::chrono::milliseconds>
    class SinkOperator : public StreamingOperator {
    public:
        explicit SinkOperator(OperatorID opId, const std::string& opName);
        ~SinkOperator() override;

        void Initialize(runtime::ExecutionEngine* executionEngine, memory::MemoryManager* memoryManager,
                metrics::Profiler* profiler) final;
        void InitializeQueues(std::vector<queueing::RecordQueueBase*> inputQueues,
                size_t outputQueueSize) override;
        std::vector<queueing::RecordQueueBase *> GetOutputQueues() override;

        uint8_t ProcessBlock() final;
        uint8_t ProcessQueue() final;
        uint8_t ProcessBlockInBatches() override;
        [[nodiscard]] ChannelID GetBlockedUpstreamChannelId() const override;
        [[nodiscard]] bool IsSinkOperator() const override;
        consteval TInput GetInputType();

    protected:
#if ENJIMA_METRICS_LEVEL >= 3
        metrics::DoubleAverageGauge* batchSizeGauge_{nullptr};
#endif
        metrics::Counter<uint64_t>* inCounter_{nullptr};
        metrics::Histogram<uint64_t>* latencyHistogram_{nullptr};

    private:
        virtual void ProcessSinkEvent(uint64_t timestamp, TInput inputEvent) = 0;
        virtual void ProcessSinkBatch(void* inputBuffer, uint32_t numRecordsToRead) = 0;

        ChannelID upstreamChannelId_{0};
        memory::MemoryBlock* inputBlockPtr_{nullptr};

        queueing::RecordQueueImpl<core::Record<TInput>>* inputQueue_;
        void* inputRecordBuffer_{nullptr};
    };
}// namespace enjima::operators

#include "SinkOperator.tpp"

#endif//ENJIMA_SINK_OPERATOR_H
