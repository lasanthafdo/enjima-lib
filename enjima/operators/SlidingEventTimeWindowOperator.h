//
// Created by m34ferna on 01/03/24.
//

#ifndef ENJIMA_SLIDING_EVENT_TIME_WINDOW_OPERATOR_H
#define ENJIMA_SLIDING_EVENT_TIME_WINDOW_OPERATOR_H

#include "SingleInputOperator.h"
#include "enjima/api/MergeableKeyedAggregateFunction.h"
#include "enjima/runtime/IllegalArgumentException.h"

namespace enjima::operators {

    template<typename TInput, typename TOutput, enjima::api::MergeableKeyedAggFuncT<TInput,TOutput> TAggFunc>
    class SlidingEventTimeWindowOperator : public SingleInputOperator<TInput, TOutput> {
        using TFunc = enjima::api::MergeableKeyedAggregateFunction<TInput, TOutput>;

    public:
        explicit SlidingEventTimeWindowOperator(OperatorID opId, const std::string& opName, TAggFunc aggFunc,
                std::chrono::milliseconds windowSizeMillis, std::chrono::milliseconds windowSlideMillis);

    private:
        void ProcessEvent(uint64_t timestamp, TInput inputEvent, enjima::core::OutputCollector* collector) override;
        void ProcessBatch(void* inputBuffer, uint32_t numRecordsToRead, void* outputBuffer,
                core::OutputCollector* collector) override;
        uint8_t ProcessQueue() override;

        const uint64_t windowSizeMillis_;
        const uint64_t windowSlideMillis_;
        const uint64_t numPanes_;
        TAggFunc windowAggFunc_;
        uint64_t maxWindowTimestamp_{0};
        uint64_t nextWindowPaneTriggerAt_{0};
        uint64_t currentWindowPaneIdx_{0};
        std::vector<TAggFunc> paneAggFuncVec_;
        std::vector<core::Record<TOutput>> overflowRecordsBufferVec_;
    };
}// namespace enjima::operators

#include "SlidingEventTimeWindowOperator.tpp"

#endif//ENJIMA_SLIDING_EVENT_TIME_WINDOW_OPERATOR_H
