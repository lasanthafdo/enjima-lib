//
// Created by m34ferna on 15/12/23.
//

#ifndef ENJIMA_STREAMING_OPERATOR_H
#define ENJIMA_STREAMING_OPERATOR_H

#include "OperatorsTypeAliases.h"
#include "enjima/core/Record.h"
#include "enjima/memory/MemoryManager.fwd.h"
#include "enjima/metrics/Profiler.h"
#include "enjima/queueing/RecordQueueBase.h"
#include "enjima/runtime/ExecutionEngine.fwd.h"
#include <cstddef>

namespace enjima::operators {

    class StreamingOperator {
    public:
        static constexpr uint8_t kBlocked{0b0000'0001};  // represents bit 0
        static constexpr uint8_t kHasOutput{0b0010'0000};// represents bit 5
        static constexpr uint8_t kCanOutput{0b0100'0000};// represents bit 6
        static constexpr uint8_t kHasInput{0b1000'0000}; // represents bit 7

        static constexpr uint8_t kRunThreshold{kHasInput | kCanOutput};
        static constexpr uint8_t kInitStatus{kCanOutput};
        static constexpr uint8_t kQueueInitStatus{kCanOutput | kHasOutput};
        static constexpr uint8_t kFlowThreshold{kHasInput | kCanOutput | kHasOutput};

        explicit StreamingOperator(OperatorID opId, std::string opName);
        virtual ~StreamingOperator();

        virtual void Initialize(runtime::ExecutionEngine* executionEngine, memory::MemoryManager* pMemoryManager,
                metrics::Profiler* pProfiler) = 0;
        virtual void InitializeQueues(std::vector<queueing::RecordQueueBase*> inputQueues, size_t outputQueueSize) = 0;
        virtual std::vector<queueing::RecordQueueBase*> GetOutputQueues() = 0;

        virtual uint8_t ProcessBlock() = 0;
        virtual uint8_t ProcessBlockInBatches() = 0;
        virtual uint8_t ProcessQueue() = 0;

        [[nodiscard]] virtual ChannelID GetBlockedUpstreamChannelId() const = 0;
        [[nodiscard]] OperatorID GetOperatorId() const;
        [[nodiscard]] const std::string& GetOperatorName() const;
        [[nodiscard]] virtual constexpr size_t GetOutputRecordSize() const = 0;
        [[nodiscard]] virtual bool IsSourceOperator() const;
        [[nodiscard]] virtual bool IsSinkOperator() const;

    protected:
        const OperatorID operatorId_;
        const std::string operatorName_;
        memory::MemoryManager* pMemoryManager_{nullptr};
        runtime::ExecutionEngine* pExecutionEngine_{nullptr};
    };
}// namespace enjima::operators
#endif// ENJIMA_STREAMING_OPERATOR_H
