//
// Created by m34ferna on 05/02/24.
//

#ifndef ENJIMA_GENERIC_SINK_OPERATOR_H
#define ENJIMA_GENERIC_SINK_OPERATOR_H

#include "SinkOperator.h"
#include "enjima/api/SinkFunction.h"

namespace enjima::operators {
    template<typename T, typename U>
    concept SinkFuncT = std::is_base_of_v<enjima::api::SinkFunction<U>, T>;

    template<typename TInput, SinkFuncT<TInput> TFunc, typename Duration = std::chrono::milliseconds>
    class GenericSinkOperator : public SinkOperator<TInput, Duration> {
    public:
        explicit GenericSinkOperator(OperatorID opId, const std::string& opName, TFunc sinkFunction);
        [[nodiscard]] size_t GetOutputRecordSize() const final;

    private:
        void ProcessSinkEvent(uint64_t timestamp, TInput inputEvent) override;
        void ProcessSinkBatch(void* inputBuffer, uint32_t numRecordsToRead) override;

        TFunc sinkFunction_;
    };

}// namespace enjima::operators

#include "GenericSinkOperator.tpp"

#endif//ENJIMA_GENERIC_SINK_OPERATOR_H
