//
// Created by m34ferna on 05/02/24.
//

#ifndef ENJIMA_NO_OP_SINK_OPERATOR_H
#define ENJIMA_NO_OP_SINK_OPERATOR_H

#include "SinkOperator.h"
#include "spdlog/spdlog.h"

namespace enjima::operators {

    template<typename TInput, typename Duration = std::chrono::milliseconds>
    class NoOpSinkOperator : public SinkOperator<TInput, Duration> {
    public:
        explicit NoOpSinkOperator(OperatorID opId, const std::string& opName);
        [[nodiscard]] size_t GetOutputRecordSize() const final;

    private:
        void ProcessSinkEvent(uint64_t timestamp, TInput inputEvent) override;
        void ProcessSinkBatch(void* inputBuffer, uint32_t numRecordsToRead) override;
    };

}// namespace enjima::operators

#include "NoOpSinkOperator.tpp"

#endif//ENJIMA_NO_OP_SINK_OPERATOR_H
