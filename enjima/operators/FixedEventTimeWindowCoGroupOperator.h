//
// Created by m34ferna on 01/03/24.
//

#ifndef ENJIMA_EVENT_TIME_WINDOW_CO_GROUP_OPERATOR_H
#define ENJIMA_EVENT_TIME_WINDOW_CO_GROUP_OPERATOR_H

#include "DoubleInputOperator.h"
#include "enjima/api/CoGroupFunction.h"

namespace enjima::operators {

    template<typename TLeft, typename TRight, typename TOutput, enjima::api::CoGroupFuncT<TLeft, TRight, TOutput> TFunc>
    class FixedEventTimeWindowCoGroupOperator : public DoubleInputOperator<TLeft, TRight, TOutput> {
    public:
        explicit FixedEventTimeWindowCoGroupOperator(OperatorID opId, const std::string& opName, TFunc coGroupFunc,
                std::chrono::milliseconds windowSizeSecs);
        ~FixedEventTimeWindowCoGroupOperator();
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
        void ProcessPendingOverflowBatch(core::OutputCollector* collector) override;
        void ProcessPendingOverflow(core::OutputCollector* collector) override;
        void TriggerWindowIfNecessary(core::OutputCollector* collector);
        void TriggerWindowBatchIfNecessary(core::OutputCollector* collector);
        void FlushAndResetOutputBuffer(core::OutputCollector* collector);
        void ProcessLatencyRecord(core::OutputCollector* collector, uint64_t timestamp);

        uint8_t ProcessQueue() override;
        void TriggerWindowIfNecessaryQB();
        void ProcessPendingOverflowQB();

        TFunc coGroupFunc_;
        uint64_t maxLeftTimestamp_{0};
        uint64_t maxRightTimestamp_{0};
        uint64_t nextWindowAt_{0};
        uint64_t windowSize_;
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

#include "FixedEventTimeWindowCoGroupOperator.tpp"

#endif//ENJIMA_EVENT_TIME_WINDOW_CO_GROUP_OPERATOR_H
