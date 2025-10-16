//
// Created by m34ferna on 01/03/24.
//

#ifndef ENJIMA_STATIC_JOIN_OPERATOR_H
#define ENJIMA_STATIC_JOIN_OPERATOR_H


#include "SingleInputOperator.h"
#include "enjima/api/JoinFunction.h"
#include "enjima/api/JoinPredicate.h"

namespace enjima::operators {

    template<typename TLeft, typename TRight, typename TOutput, enjima::api::JoinPredT<TLeft, TRight> TPred,
            enjima::api::JoinFuncT<TLeft, TRight, TOutput> TFunc>
    class StaticJoinOperator : public SingleInputOperator<TLeft, TOutput> {
    public:
        explicit StaticJoinOperator(OperatorID opId, const std::string& opName, TPred joinPred, TFunc joinFunc,
                std::vector<TRight>& staticData);

    private:
        void ProcessEvent(uint64_t timestamp, TLeft inputEvent, enjima::core::OutputCollector* collector) override;
        void ProcessBatch(void* inputBuffer, uint32_t numRecordsToRead, void* outputBuffer,
                core::OutputCollector* collector) override;
        uint8_t ProcessQueue() override;
        TPred joinPred_;
        TFunc joinFunc_;
        std::vector<TRight> rightSide_;
    };

}// namespace enjima::operators

#include "StaticJoinOperator.tpp"

#endif//ENJIMA_STATIC_JOIN_OPERATOR_H
