//
// Created by m34ferna on 01/03/24.
//

#ifndef ENJIMA_EVENT_TIME_WINDOW_JOIN_OPERATOR_H
#define ENJIMA_EVENT_TIME_WINDOW_JOIN_OPERATOR_H

#include "DoubleInputOperator.h"
#include "ankerl/unordered_dense.h"
#include "enjima/api/JoinFunction.h"
#include "enjima/api/KeyExtractionFunction.h"

namespace enjima::operators {

    template<typename TLeft, typename TRight, typename TKey, enjima::api::KeyExtractFuncT<TLeft, TKey> TLeftKeyFunc,
            enjima::api::KeyExtractFuncT<TRight, TKey> TRightKeyFunc, typename TOutput,
            enjima::api::JoinFuncT<TLeft, TRight, TOutput> TFunc>
    class FixedEventTimeWindowJoinOperator : public DoubleInputOperator<TLeft, TRight, TOutput> {
    public:
        explicit FixedEventTimeWindowJoinOperator(OperatorID opId, const std::string& opName, TLeftKeyFunc leftKeyFunc,
                TRightKeyFunc rightKeyFunc, TFunc joinFunc, std::chrono::milliseconds windowSizeSecs);
        ~FixedEventTimeWindowJoinOperator();
        void Initialize(runtime::ExecutionEngine* executionEngine, memory::MemoryManager* memoryManager,
                metrics::Profiler* profiler) override;

    private:
        void ProcessLeftEvent(core::Record<TLeft>* pLeftInputRecord, enjima::core::OutputCollector* collector) override;
        void ProcessRightEvent(core::Record<TRight>* pRightInputRecord,
                enjima::core::OutputCollector* collector) override;
        void ProcessLeftEventIfNecessary(core::Record<TLeft>* pLeftInputRecord);
        void ProcessRightEventIfNecessary(core::Record<TRight>* pRightInputRecord);
        void ProcessBatch(void* leftInputBuffer, uint32_t numRecordsToReadLeft, void* rightInputBuffer,
                uint32_t numRecordsToReadRight, core::OutputCollector* collector) override;
        void ProcessPendingOverflow(core::OutputCollector* collector) override;
        void ProcessPendingOverflowBatch(core::OutputCollector* collector) override;
        void TriggerWindowIfNecessary(core::OutputCollector* collector);
        void TriggerWindowBatchIfNecessary(core::OutputCollector* collector);
        void FlushAndResetOutputBuffer(core::OutputCollector* collector);
        void ProcessLatencyRecord(core::OutputCollector* collector, uint64_t timestamp);

        uint8_t ProcessQueue() override;
        void TriggerWindowIfNecessaryQB();
        void ProcessPendingOverflowQB();

        TLeftKeyFunc leftKeyFunc_;
        TRightKeyFunc rightKeyFunc_;
        TFunc joinFunc_;
        uint64_t maxLeftTimestamp_{0};
        uint64_t maxRightTimestamp_{0};
        uint64_t nextWindowAt_{0};
        uint64_t windowSize_;
        ankerl::unordered_dense::map<TKey, std::vector<size_t>> leftHash_;
        ankerl::unordered_dense::map<TKey, std::vector<size_t>> rightHash_;
        std::vector<core::Record<TLeft>> leftRecordBuf_;
        std::vector<core::Record<TRight>> rightRecordBuf_;
        std::queue<core::Record<TLeft>> nextWnLeftRecordBuf_;
        std::queue<core::Record<TRight>> nextWnRightRecordBuf_;
        std::queue<TOutput> outputEventQBuf_;
        std::vector<core::Record<TOutput>> overflowOutputRecordBuf_;
        std::vector<core::Record<TOutput>>::iterator overflowOutputRecordBufIterator_;
        void* outputRecordBuffer_{nullptr};
        core::Record<TOutput>* nextOutRecordPtr_{nullptr};
        uint32_t numBuffered_{0};
    };
}// namespace enjima::operators

#include "FixedEventTimeWindowJoinOperator.tpp"

#endif//ENJIMA_EVENT_TIME_WINDOW_JOIN_OPERATOR_H
