//
// Created by m34ferna on 05/02/24.
//

#ifndef ENJIMA_FILTER_OPERATOR_H
#define ENJIMA_FILTER_OPERATOR_H

#include "OperatorUtil.h"
#include "enjima/memory/MemoryBlock.h"
#include "enjima/operators/SingleInputOperator.h"
#include "enjima/operators/StreamingOperator.h"
#include "enjima/queueing/RecordQueueBase.h"

namespace enjima::operators {

    template<typename TEvent, typename TPred>
    class FilterOperator : public SingleInputOperator<TEvent, TEvent> {
    public:
        explicit FilterOperator(OperatorID opId, const std::string& opName, TPred predicate);


    private:
        void ProcessEvent(uint64_t timestamp, TEvent inputEvent, enjima::core::OutputCollector* collector) override;
        void ProcessBatch(void* inputBuffer, uint32_t numRecordsToRead, void* outputBuffer,
                core::OutputCollector* collector) override;

        uint8_t ProcessQueue() override;

        TPred predicate_;
    };
}// namespace enjima::operators

#include "FilterOperator.tpp"

#endif//ENJIMA_FILTER_OPERATOR_H
