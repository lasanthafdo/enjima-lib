//
// Created by m34ferna on 15/12/23.
//

#ifndef ENJIMA_DOUBLE_INPUT_OPERATOR_H
#define ENJIMA_DOUBLE_INPUT_OPERATOR_H

#include "OperatorUtil.h"
#include "enjima/core/OutputCollector.h"
#include "enjima/memory/MemoryBlock.h"
#include "enjima/metrics/MetricNames.h"
#include "enjima/operators/StreamingOperator.h"
#include "enjima/queueing/RecordQueueBase.h"
#include "enjima/queueing/RecordQueueImpl.h"

namespace enjima::operators {
    template<typename TLeft, typename TRight, typename TOutput>
    class DoubleInputOperator : public StreamingOperator {
    public:
        explicit DoubleInputOperator(OperatorID opId, const std::string& opName);
        ~DoubleInputOperator() override;

        void Initialize(runtime::ExecutionEngine* executionEngine, memory::MemoryManager* memoryManager,
                metrics::Profiler* profiler) override;
        void InitializeQueues(std::vector<queueing::RecordQueueBase*> inputQueues, size_t outputQueueSize) override;
        std::vector<queueing::RecordQueueBase*> GetOutputQueues() override;

        uint8_t ProcessBlock() override;
        uint8_t ProcessBlockInBatches() override;
        [[nodiscard]] ChannelID GetBlockedUpstreamChannelId() const override;
        [[nodiscard]] constexpr size_t GetOutputRecordSize() const override;
        consteval TOutput GetOutputType();
        consteval TLeft GetLeftInputType();
        consteval TRight GetRightInputType();

    protected:
        inline bool SetNextOutputBlockIfNecessary();

#if ENJIMA_METRICS_LEVEL >= 3
        metrics::DoubleAverageGauge* batchSizeGauge_{nullptr};
#endif
        metrics::Counter<uint64_t>* inCounter_{nullptr};
        metrics::Counter<uint64_t>* leftInCounter_{nullptr};
        metrics::Counter<uint64_t>* rightInCounter_{nullptr};
        metrics::Counter<uint64_t>* outCounter_{nullptr};
        memory::MemoryBlock* outputBlockPtr_{nullptr};
        ChannelID downstreamChannelId_{0};
        bool hasPendingOverflow_{false};

        queueing::RecordQueueImpl<core::Record<TLeft>>* leftInputQueue_;
        queueing::RecordQueueImpl<core::Record<TRight>>* rightInputQueue_;
        queueing::RecordQueueImpl<core::Record<TOutput>>* outputQueue_;

    private:
        virtual void ProcessLeftEvent(core::Record<TLeft>* pLeftInputRecord,
                enjima::core::OutputCollector* collector) = 0;
        virtual void ProcessRightEvent(core::Record<TRight>* pRightInputRecord,
                enjima::core::OutputCollector* collector) = 0;
        virtual void ProcessBatch(void* leftInputBuffer, uint32_t numRecordsToReadLeft, void* rightInputBuffer,
                uint32_t numRecordsToReadRight, core::OutputCollector* collector) = 0;
        virtual void ProcessPendingOverflow(core::OutputCollector* collector) = 0;
        virtual void ProcessPendingOverflowBatch(core::OutputCollector* collector) = 0;

        const memory::MemoryBlock* reservedMemoryBlockPtr_{nullptr};
        ChannelID upstreamLeftChannelId_{0};
        ChannelID upstreamRightChannelId_{0};
        ChannelID blockedUpstreamChannelId_{0};
        memory::MemoryBlock* leftInputBlockPtr_{nullptr};
        memory::MemoryBlock* rightInputBlockPtr_{nullptr};
        core::OutputCollector* outputCollector_{nullptr};

        void* leftInputRecordBuffer_{nullptr};
        void* rightInputRecordBuffer_{nullptr};
    };
}// namespace enjima::operators

#include "DoubleInputOperator.tpp"

#endif//ENJIMA_DOUBLE_INPUT_OPERATOR_H
