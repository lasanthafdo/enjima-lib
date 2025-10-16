//
// Created by m34ferna on 01/03/24.
//

#ifndef ENJIMA_MAP_OPERATOR_H
#define ENJIMA_MAP_OPERATOR_H


#include "SingleInputOperator.h"
#include "enjima/api/MapFunction.h"

namespace enjima::operators {

    template<typename TInput, typename TOutput, enjima::api::MapFuncT<TInput, TOutput> TFunc, int NOuts = 1>
    class MapOperator : public SingleInputOperator<TInput, TOutput, NOuts> {
    public:
        explicit MapOperator(OperatorID opId, const std::string& opName, TFunc mapFunc);

    private:
        void ProcessEvent(uint64_t timestamp, TInput inputEvent,
                std::array<enjima::core::OutputCollector*, NOuts> collectorArr) override;
        void ProcessBatch(void* inputBuffer, uint32_t numRecordsToRead, void* outputBuffer,
                std::array<enjima::core::OutputCollector*, NOuts> collectorArr) override;

        uint8_t ProcessQueue() override;

        TFunc mapFunc_;
    };

    template<typename TInput, typename TOutput, enjima::api::MapFuncT<TInput, TOutput> TFunc>
    class MapOperator<TInput, TOutput, TFunc, 1> : public SingleInputOperator<TInput, TOutput, 1> {
    public:
        explicit MapOperator(OperatorID opId, const std::string& opName, TFunc mapFunc);

    private:
        void ProcessEvent(uint64_t timestamp, TInput inputEvent, enjima::core::OutputCollector* collector) override;
        void ProcessBatch(void* inputBuffer, uint32_t numRecordsToRead, void* outputBuffer,
                core::OutputCollector* collector) override;

        uint8_t ProcessQueue() override;

        TFunc mapFunc_;
    };

    template<typename TInput, typename TOutput, enjima::api::MapFuncT<TInput, TOutput> TFunc>
    class MapOperator<TInput, TOutput, TFunc, 2> : public SingleInputOperator<TInput, TOutput, 2> {
    public:
        explicit MapOperator(OperatorID opId, const std::string& opName, TFunc mapFunc);

    private:
        void ProcessEvent(uint64_t timestamp, TInput inputEvent, core::OutputCollector* leftOutputCollector,
                core::OutputCollector* rightOutputCollector) override;
        void ProcessBatch(void* inputBuffer, uint32_t numRecordsToRead, void* outputBuffer,
                core::OutputCollector* leftOutputCollector, core::OutputCollector* rightOutputCollector) override;
        uint8_t ProcessQueue() override;

        TFunc mapFunc_;
    };
}// namespace enjima::operators

#include "MapDoubleOutOperator.tpp"
#include "MapOperator.tpp"
#include "MapSingleOutOperator.tpp"

#endif//ENJIMA_MAP_OPERATOR_H
