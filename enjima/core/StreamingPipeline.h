//
// Created by m34ferna on 10/01/24.
//

#ifndef ENJIMA_STREAMING_PIPELINE_H
#define ENJIMA_STREAMING_PIPELINE_H

#include "enjima/operators/StreamingOperator.h"
#include "enjima/runtime/StreamingTask.h"
#include <vector>

namespace enjima::core {

    class StreamingPipeline {
    public:
        void Initialize(runtime::ExecutionEngine* pExecutionEngine, memory::MemoryManager* pMemoryManager,
                metrics::Profiler* pProfiler, size_t outputQueueSize);
        [[nodiscard]] const std::vector<operators::StreamingOperator*>& GetOperatorsInTopologicalOrder() const;
        [[nodiscard]] std::vector<operators::StreamingOperator*> GetSourceOperators() const;
        [[nodiscard]] std::vector<operators::StreamingOperator*> GetSinkOperators() const;
        [[nodiscard]] std::vector<operators::OperatorID> GetUpstreamOperatorIDs(operators::OperatorID operatorId) const;
        [[nodiscard]] std::vector<operators::StreamingOperator*> GetUpstreamOperators(
                operators::OperatorID operatorId) const;
        [[nodiscard]] std::vector<operators::OperatorID> GetDownstreamOperatorIDs(
                operators::OperatorID operatorId) const;
        [[nodiscard]] std::vector<operators::StreamingOperator*> GetDownstreamOperators(
                operators::OperatorID operatorId) const;
        [[nodiscard]] std::vector<operators::StreamingOperator*> GetAllDownstreamOps(
                operators::StreamingOperator* const& pStreamOp) const;
        [[nodiscard]] runtime::StreamingTask::ProcessingMode GetProcessingMode() const;
        [[nodiscard]] bool IsWaitingToCancel() const;
        [[nodiscard]] const std::unordered_map<operators::OperatorID, uint8_t>& GetOperatorsLevelsMap() const;
        [[nodiscard]] const operators::StreamingOperator* GetOperatorForID(operators::OperatorID opID) const;
        [[nodiscard]] uint8_t GetDagDepth() const;

        void AddOperatorAtLevel(operators::StreamingOperator* pStreamOp, uint8_t dagLevel);
        void UpdateUpstreamAndDownstreamOperators(operators::OperatorID opID,
                std::vector<operators::OperatorID> upstreamOpIds, std::vector<operators::OperatorID> downstreamOpIds);
        void SetProcessingMode(runtime::StreamingTask::ProcessingMode processingMode);
        void SetWaitingToCancel();

    private:
        std::vector<operators::StreamingOperator*> operatorsInTopologicalOrder_;
        std::unordered_map<operators::OperatorID, operators::StreamingOperator*> opIdToOpPtrMap_;
        std::unordered_map<operators::OperatorID, uint8_t> operatorsLevelsMap_;
        std::unordered_map<operators::OperatorID,
                std::pair<std::vector<operators::OperatorID>, std::vector<operators::OperatorID>>>
                upstreamDownstreamOpIdMap_;
        runtime::StreamingTask::ProcessingMode processingMode_{runtime::StreamingTask::ProcessingMode::kUnassigned};
        std::atomic<bool> waitingToCancel_{false};
        uint8_t dagDepth_{0};
    };

}// namespace enjima::core

#endif// ENJIMA_STREAMING_PIPELINE_H
