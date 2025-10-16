//
// Created by m34ferna on 15/12/23.
//

#ifndef ENJIMA_SINGLE_INPUT_OPERATOR_H
#define ENJIMA_SINGLE_INPUT_OPERATOR_H

#include "OperatorUtil.h"
#include "enjima/core/OutputCollector.h"
#include "enjima/memory/MemoryBlock.h"
#include "enjima/metrics/MetricNames.h"
#include "enjima/operators/StreamingOperator.h"
#include "enjima/queueing/RecordQueueBase.h"
#include "enjima/queueing/RecordQueueImpl.h"

namespace enjima::operators {
    template<typename TInput, typename TOutput, int NOuts = 1>
    class SingleInputOperator : public StreamingOperator {
    public:
        explicit SingleInputOperator(OperatorID opId, const std::string& opName);
        ~SingleInputOperator() override;

        void Initialize(runtime::ExecutionEngine* executionEngine, memory::MemoryManager* memoryManager,
                metrics::Profiler* profiler) override;
        void InitializeQueues(std::vector<queueing::RecordQueueBase*> inputQueues, size_t outputQueueSize) override;
        std::vector<queueing::RecordQueueBase*> GetOutputQueues() override;

        uint8_t ProcessBlock() override;
        uint8_t ProcessBlockInBatches() override;
        [[nodiscard]] ChannelID GetBlockedUpstreamChannelId() const override;
        [[nodiscard]] constexpr size_t GetOutputRecordSize() const override;
        consteval TOutput GetOutputType();
        consteval TInput GetInputType();

    protected:
#if ENJIMA_METRICS_LEVEL >= 3
        metrics::DoubleAverageGauge* batchSizeGauge_{nullptr};
#endif
        metrics::Counter<uint64_t>* inCounter_{nullptr};
        std::array<metrics::Counter<uint64_t>*, NOuts> outCounterArr_;
        queueing::RecordQueueImpl<core::Record<TInput>>* inputQueue_;
        std::array<queueing::RecordQueueImpl<core::Record<TOutput>>*, NOuts> outputQueueArr_;
        std::array<memory::MemoryBlock*, NOuts> outputBlockPtrArr_;
        std::array<ChannelID, NOuts> downstreamChannelIdArr_;

    private:
        virtual void ProcessEvent(uint64_t timestamp, TInput inputEvent,
                std::array<enjima::core::OutputCollector*, NOuts> collectorArr) = 0;
        virtual void ProcessBatch(void* inputBuffer, uint32_t numRecordsToRead, void* outputBuffer,
                std::array<core::OutputCollector*, NOuts> collectorArr) = 0;

        const memory::MemoryBlock* reservedMemoryBlockPtr_{nullptr};
        ChannelID upstreamChannelId_{0};
        memory::MemoryBlock* inputBlockPtr_{nullptr};
        std::array<core::OutputCollector*, NOuts> outputCollectorArr_;

        void* inputRecordBuffer_{nullptr};
        void* outputRecordBuffer_{nullptr};
    };

    // Specialized template class for single output stream.
    template<typename TInput, typename TOutput>
    class SingleInputOperator<TInput, TOutput, 1> : public StreamingOperator {
    public:
        explicit SingleInputOperator(OperatorID opId, const std::string& opName);
        ~SingleInputOperator() override;

        void Initialize(runtime::ExecutionEngine* executionEngine, memory::MemoryManager* memoryManager,
                metrics::Profiler* profiler) override;
        void InitializeQueues(std::vector<queueing::RecordQueueBase*> inputQueues, size_t outputQueueSize) override;
        std::vector<queueing::RecordQueueBase*> GetOutputQueues() override;

        uint8_t ProcessBlock() override;
        uint8_t ProcessBlockInBatches() override;
        [[nodiscard]] ChannelID GetBlockedUpstreamChannelId() const override;
        [[nodiscard]] constexpr size_t GetOutputRecordSize() const override;
        consteval TOutput GetOutputType();
        consteval TInput GetInputType();

    protected:
#if ENJIMA_METRICS_LEVEL >= 3
        metrics::DoubleAverageGauge* batchSizeGauge_{nullptr};
#endif
        metrics::Counter<uint64_t>* inCounter_{nullptr};
        metrics::Counter<uint64_t>* outCounter_{nullptr};
        queueing::RecordQueueImpl<core::Record<TInput>>* inputQueue_;
        queueing::RecordQueueImpl<core::Record<TOutput>>* outputQueue_;
        memory::MemoryBlock* outputBlockPtr_{nullptr};
        ChannelID downstreamChannelId_{0};
        bool hasPendingOverflow_{false};

    private:
        virtual void ProcessEvent(uint64_t timestamp, TInput inputEvent, enjima::core::OutputCollector* collector) = 0;
        virtual void ProcessBatch(void* inputBuffer, uint32_t numRecordsToRead, void* outputBuffer,
                core::OutputCollector* collector) = 0;
        virtual void ProcessPendingOverflow(core::OutputCollector* collector);
        virtual void ProcessPendingOverflowBatch(core::OutputCollector* collector);

        const memory::MemoryBlock* reservedMemoryBlockPtr_{nullptr};
        ChannelID upstreamChannelId_{0};
        memory::MemoryBlock* inputBlockPtr_{nullptr};
        core::OutputCollector* outputCollector_{nullptr};

        void* inputRecordBuffer_{nullptr};
        void* outputRecordBuffer_{nullptr};
    };

    // Specialized template class for double output streams.
    template<typename TInput, typename TOutput>
    class SingleInputOperator<TInput, TOutput, 2> : public StreamingOperator {
    public:
        explicit SingleInputOperator(OperatorID opId, const std::string& opName);
        ~SingleInputOperator() override;

        void Initialize(runtime::ExecutionEngine* executionEngine, memory::MemoryManager* memoryManager,
                metrics::Profiler* profiler) override;
        void InitializeQueues(std::vector<queueing::RecordQueueBase*> inputQueues, size_t outputQueueSize) override;
        std::vector<queueing::RecordQueueBase*> GetOutputQueues() override;

        uint8_t ProcessBlock() override;
        uint8_t ProcessBlockInBatches() override;
        [[nodiscard]] ChannelID GetBlockedUpstreamChannelId() const override;
        [[nodiscard]] constexpr size_t GetOutputRecordSize() const override;
        consteval TOutput GetOutputType();
        consteval TInput GetInputType();

    protected:
#if ENJIMA_METRICS_LEVEL >= 3
        metrics::DoubleAverageGauge* batchSizeGauge_{nullptr};
#endif
        metrics::Counter<uint64_t>* inCounter_{nullptr};
        metrics::Counter<uint64_t>* leftOutCounter_{nullptr};
        metrics::Counter<uint64_t>* rightOutCounter_{nullptr};
        queueing::RecordQueueImpl<core::Record<TInput>>* inputQueue_;
        queueing::RecordQueueImpl<core::Record<TOutput>>* leftOutputQueue_;
        queueing::RecordQueueImpl<core::Record<TOutput>>* rightOutputQueue_;

    private:
        virtual void ProcessEvent(uint64_t timestamp, TInput inputEvent, core::OutputCollector* leftOutputCollector,
                core::OutputCollector* rightOutputCollector) = 0;
        virtual void ProcessBatch(void* inputBuffer, uint32_t numRecordsToRead, void* outputBuffer,
                core::OutputCollector* leftOutputCollector, core::OutputCollector* rightOutputCollector) = 0;

        const memory::MemoryBlock* reservedMemoryBlockPtr_{nullptr};

        ChannelID upstreamChannelId_{0};
        ChannelID downstreamLeftChannelId_{0};
        ChannelID downstreamRightChannelId_{0};
        memory::MemoryBlock* inputBlockPtr_{nullptr};
        memory::MemoryBlock* leftOutputBlockPtr_{nullptr};
        memory::MemoryBlock* rightOutputBlockPtr_{nullptr};
        core::OutputCollector* leftOutputCollector_{nullptr};
        core::OutputCollector* rightOutputCollector_{nullptr};

        void* inputRecordBuffer_{nullptr};
        void* outputRecordBuffer_{nullptr};
    };

}// namespace enjima::operators

#include "SingleInputDoubleOutOperator.tpp"
#include "SingleInputOperator.tpp"
#include "SingleInputSingleOutOperator.tpp"

#endif//ENJIMA_SINGLE_INPUT_OPERATOR_H
