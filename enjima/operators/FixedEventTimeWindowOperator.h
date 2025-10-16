//
// Created by m34ferna on 01/03/24.
//

#ifndef ENJIMA_FIXED_EVENT_TIME_WINDOW_OPERATOR_H
#define ENJIMA_FIXED_EVENT_TIME_WINDOW_OPERATOR_H

#include "SingleInputOperator.h"
#include "enjima/api/KeyedOrNonKeyedAggregateFunction.h"

namespace enjima::operators {

    template<typename TInput, typename TOutput, bool IsKeyedT, enjima::api::AggFuncT<TInput, TOutput, IsKeyedT> TFunc>
    class FixedEventTimeWindowOperator : public SingleInputOperator<TInput, TOutput> {
    public:
        explicit FixedEventTimeWindowOperator(OperatorID opId, const std::string& opName, TFunc aggFunc,
                std::chrono::milliseconds windowSizeMillis);

    private:
        void ProcessEvent(uint64_t timestamp, TInput inputEvent, enjima::core::OutputCollector* collector) override;
        void ProcessBatch(void* inputBuffer, uint32_t numRecordsToRead, void* outputBuffer,
                core::OutputCollector* collector) override;
        void ProcessPendingOverflow(core::OutputCollector* collector) override;
        void ProcessPendingOverflowBatch(core::OutputCollector* collector) override;
        uint8_t ProcessQueue() override;
        bool SetNextOutputBlockIfNecessary(core::OutputCollector* collector);

        const uint64_t windowSizeMillis_;
        TFunc aggFunc_;
        uint64_t maxWindowTimestamp_{0};
        uint64_t nextWindowTriggerAt_{0};
        std::vector<core::Record<TOutput>> overflowOutputRecordBuf_;
        std::vector<core::Record<TOutput>>::iterator overflowOutputRecordBufIterator_;
    };
}// namespace enjima::operators

#include "FixedEventTimeWindowOperator.tpp"

#endif//ENJIMA_FIXED_EVENT_TIME_WINDOW_OPERATOR_H
