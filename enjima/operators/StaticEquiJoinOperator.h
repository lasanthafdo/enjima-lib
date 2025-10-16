//
// Created by m34ferna on 01/03/24.
//

#ifndef ENJIMA_STATIC_EQUI_JOIN_OPERATOR_H
#define ENJIMA_STATIC_EQUI_JOIN_OPERATOR_H


#include "SingleInputOperator.h"
#include "enjima/api/JoinFunction.h"
#include "enjima/api/KeyExtractionFunction.h"

namespace enjima::operators {

    template<typename TLeft, typename TRight, typename TOutput, typename TKey,
            enjima::api::KeyExtractFuncT<TLeft, TKey> TKeyFunc, enjima::api::JoinFuncT<TLeft, TRight, TOutput> TFunc,
            int NOuts = 1>
    class StaticEquiJoinOperator : public SingleInputOperator<TLeft, TOutput, NOuts> {
    public:
        explicit StaticEquiJoinOperator(OperatorID opId, const std::string& opName, TKeyFunc keyExtractFunc,
                TFunc joinFunc, UnorderedHashMapST<TKey, TRight>& staticData);

    private:
        void ProcessEvent(uint64_t timestamp, TLeft inputEvent, enjima::core::OutputCollector* collector) override;
        void ProcessBatch(void* inputBuffer, uint32_t numRecordsToRead, void* outputBuffer,
                core::OutputCollector* collector) override;
        uint8_t ProcessQueue() override;

        TKeyFunc keyExtractFunc_;
        TFunc joinFunc_;
        UnorderedHashMapST<TKey, TRight> rightSide_;
    };

    template<typename TLeft, typename TRight, typename TOutput, typename TKey,
            enjima::api::KeyExtractFuncT<TLeft, TKey> TKeyFunc, enjima::api::JoinFuncT<TLeft, TRight, TOutput> TFunc>
    class StaticEquiJoinOperator<TLeft, TRight, TOutput, TKey, TKeyFunc, TFunc, 2>
        : public SingleInputOperator<TLeft, TOutput, 2> {
    public:
        explicit StaticEquiJoinOperator(OperatorID opId, const std::string& opName, TKeyFunc keyExtractFunc,
                TFunc joinFunc, UnorderedHashMapST<TKey, TRight>& staticData);

    private:
        void ProcessEvent(uint64_t timestamp, TLeft inputEvent, core::OutputCollector* leftOutputCollector,
                core::OutputCollector* rightOutputCollector) override;
        void ProcessBatch(void* inputBuffer, uint32_t numRecordsToRead, void* outputBuffer,
                core::OutputCollector* leftOutputCollector, core::OutputCollector* rightOutputCollector) override;
        uint8_t ProcessQueue() override;

        TKeyFunc keyExtractFunc_;
        TFunc joinFunc_;
        UnorderedHashMapST<TKey, TRight> rightSide_;
    };

}// namespace enjima::operators

#include "StaticEquiJoinDoubleOutOperator.tpp"
#include "StaticEquiJoinOperator.tpp"

#endif//ENJIMA_STATIC_EQUI_JOIN_OPERATOR_H
