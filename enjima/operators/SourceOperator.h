//
// Created by m34ferna on 01/02/24.
//

#ifndef ENJIMA_SOURCE_OPERATOR_H
#define ENJIMA_SOURCE_OPERATOR_H


#include "OperatorUtil.h"
#include "StreamingOperator.h"
#include "enjima/core/OutputCollector.h"
#include "enjima/memory/MemoryBlock.h"
#include "enjima/memory/MemoryManager.h"
#include "enjima/metrics/MetricNames.h"
#include "enjima/metrics/types/Counter.h"
#include "enjima/queueing/RecordQueueBase.h"
#include "enjima/queueing/RecordQueueImpl.h"
#include <iostream>

#include <ostream>

namespace enjima::operators {

    template<typename TOutput>
    class SourceOperator : public StreamingOperator {
    public:
        explicit SourceOperator(OperatorID opId, const std::string& opName);
        ~SourceOperator() override;

        void Initialize(runtime::ExecutionEngine* executionEngine, memory::MemoryManager* memoryManager,
                metrics::Profiler* profiler) override;
        void InitializeQueues(std::vector<queueing::RecordQueueBase*> inputQueues,
                size_t outputQueueSize) override;
        std::vector<queueing::RecordQueueBase *> GetOutputQueues() override;

        uint8_t ProcessBlock() final;
        uint8_t ProcessQueue() final;
        uint8_t ProcessBlockInBatches() final;
        [[nodiscard]] ChannelID GetBlockedUpstreamChannelId() const override;
        [[nodiscard]] constexpr size_t GetOutputRecordSize() const final;
        [[nodiscard]] bool IsSourceOperator() const override;
        consteval TOutput GetOutputType();

    protected:
        virtual TOutput GenerateQueueEvent() = 0;
        virtual bool GenerateQueueRecord(enjima::core::Record<TOutput>& outputRecord) = 0;
        virtual bool EmitEvent(core::OutputCollector* collector) = 0;
        virtual uint32_t EmitBatch(uint32_t maxRecordsToWrite, void* outputBuffer,
                core::OutputCollector* collector) = 0;

#if ENJIMA_METRICS_LEVEL >= 3
        metrics::DoubleAverageGauge* batchSizeGauge_{nullptr};
#endif
        metrics::Counter<uint64_t>* outCounter_{nullptr};

    private:
        const memory::MemoryBlock* reservedMemoryBlockPtr_{nullptr};
        ChannelID downstreamChannelId_{0};
        memory::MemoryBlock* outputBlockPtr_{nullptr};
        core::OutputCollector* outputCollector_{nullptr};

        queueing::RecordQueueImpl<core::Record<TOutput>>* outputQueue_;
        void* outputRecordBuffer_{nullptr};
    };
}// namespace enjima::operators

#include "SourceOperator.tpp"

#endif//ENJIMA_SOURCE_OPERATOR_H
