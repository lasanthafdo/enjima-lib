//
// Created by m34ferna on 10/01/24.
//

#include "StreamingPipeline.h"
#include "enjima/runtime/AlreadyExistsException.h"
#include "enjima/runtime/IllegalStateException.h"
#include <queue>

namespace enjima::core {
    void StreamingPipeline::Initialize(runtime::ExecutionEngine* pExecutionEngine,
            memory::MemoryManager* pMemoryManager, metrics::Profiler* pProfiler, size_t outputQueueSize)
    {
        UnorderedHashMapST<operators::OperatorID, std::queue<queueing::RecordQueueBase*>> remainingOutputQueueMap;
        for (auto opPtr: operatorsInTopologicalOrder_) {
            opPtr->Initialize(pExecutionEngine, pMemoryManager, pProfiler);
            auto upstreamOpPtrs = GetUpstreamOperators(opPtr->GetOperatorId());
            std::vector<queueing::RecordQueueBase*> opInputQueues{};
            if (!upstreamOpPtrs.empty()) {
                std::vector<metrics::Counter<uint64_t>*> prevOpOutCounterPtrs;
                prevOpOutCounterPtrs.reserve(upstreamOpPtrs.size());
                for (auto upstreamOpPtr: upstreamOpPtrs) {
                    prevOpOutCounterPtrs.emplace_back(pProfiler->GetOutCounter(upstreamOpPtr->GetOperatorName()));
                    if (processingMode_ == runtime::StreamingTask::ProcessingMode::kQueueBasedSingle) {
                        auto& remainingOutputQueues = remainingOutputQueueMap.at(upstreamOpPtr->GetOperatorId());
                        if (remainingOutputQueues.empty()) {
                            throw runtime::IllegalStateException{
                                    std::string("No output queues available as input for operator ")
                                            .append(opPtr->GetOperatorName())
                                            .append(" with id ")
                                            .append(std::to_string(opPtr->GetOperatorId()))};
                        }
                        opInputQueues.push_back(remainingOutputQueues.front());
                        remainingOutputQueues.pop();
                    }
                }
                auto currentOpInCounterPtr = pProfiler->GetInCounter(opPtr->GetOperatorName());
                pProfiler->GetOrCreatePendingEventsGauge(opPtr->GetOperatorName(), prevOpOutCounterPtrs,
                        currentOpInCounterPtr);
            }
            if (processingMode_ == runtime::StreamingTask::ProcessingMode::kQueueBasedSingle) {
                assert(opPtr->IsSourceOperator() || !opInputQueues.empty());
                opPtr->InitializeQueues(opInputQueues, outputQueueSize);
                auto outputQueueVec = opPtr->GetOutputQueues();
                assert(opPtr->IsSinkOperator() || !outputQueueVec.empty());
                for (auto outputQueuePtr: outputQueueVec) {
                    remainingOutputQueueMap[opPtr->GetOperatorId()].push(outputQueuePtr);
                }
            }
        }
        if (processingMode_ == runtime::StreamingTask::ProcessingMode::kQueueBasedSingle) {
            for (auto& [opId, remainingOutputQueues]: remainingOutputQueueMap) {
                if (!remainingOutputQueues.empty()) {
                    throw runtime::IllegalStateException{
                            std::string("Queue of output queues not empty for operator id ")
                                    .append(std::to_string(opId))};
                }
            }
        }
    }

    void StreamingPipeline::AddOperatorAtLevel(operators::StreamingOperator* pStreamOp, uint8_t dagLevel)
    {
        operatorsLevelsMap_.emplace(pStreamOp->GetOperatorId(), dagLevel);
        auto targetLevel = dagLevel + 1;
        auto pos = operatorsInTopologicalOrder_.begin();
        while (pos != operatorsInTopologicalOrder_.cend()) {
            auto currentOpId = (*pos)->GetOperatorId();
            if (operatorsLevelsMap_.at(currentOpId) >= targetLevel) {
                break;
            }
            pos++;
        }
        operatorsInTopologicalOrder_.emplace(pos, pStreamOp);
        opIdToOpPtrMap_.emplace(pStreamOp->GetOperatorId(), pStreamOp);
        dagDepth_ = std::max(dagDepth_, dagLevel);
    }

    void StreamingPipeline::UpdateUpstreamAndDownstreamOperators(operators::OperatorID opID,
            std::vector<operators::OperatorID> upstreamOpIds, std::vector<operators::OperatorID> downstreamOpIds)
    {
        auto resultPair = upstreamDownstreamOpIdMap_.try_emplace(opID, upstreamOpIds, downstreamOpIds);
        if (!resultPair.second) {
            throw runtime::AlreadyExistsException(std::string(
                    "Upstream and downstream operator info exists for operator with id " + std::to_string(opID)));
        }
    }

    const std::vector<operators::StreamingOperator*>& StreamingPipeline::GetOperatorsInTopologicalOrder() const
    {
        return operatorsInTopologicalOrder_;
    }

    std::vector<operators::StreamingOperator*> StreamingPipeline::GetSourceOperators() const
    {
        std::vector<operators::StreamingOperator*> srcOps;
        for (auto streamOp: operatorsInTopologicalOrder_) {
            if (streamOp->IsSourceOperator()) {
                srcOps.push_back(streamOp);
            }
        }
        return srcOps;
    }

    std::vector<operators::StreamingOperator*> StreamingPipeline::GetSinkOperators() const
    {
        std::vector<operators::StreamingOperator*> sinkOps;
        for (auto streamOp: operatorsInTopologicalOrder_) {
            if (streamOp->IsSinkOperator()) {
                sinkOps.push_back(streamOp);
            }
        }
        return sinkOps;
    }

    std::vector<operators::OperatorID> StreamingPipeline::GetUpstreamOperatorIDs(operators::OperatorID operatorId) const
    {
        return upstreamDownstreamOpIdMap_.at(operatorId).first;
    }

    std::vector<operators::StreamingOperator*> StreamingPipeline::GetUpstreamOperators(
            operators::OperatorID operatorId) const
    {
        auto upstreamOpIdVec = upstreamDownstreamOpIdMap_.at(operatorId).first;
        std::vector<operators::StreamingOperator*> upstreamOps;
        upstreamOps.reserve(upstreamOpIdVec.size());
        for (auto upstreamOpId: upstreamOpIdVec) {
            upstreamOps.emplace_back(opIdToOpPtrMap_.at(upstreamOpId));
        }
        return upstreamOps;
    }

    std::vector<operators::OperatorID> StreamingPipeline::GetDownstreamOperatorIDs(
            operators::OperatorID operatorId) const
    {
        return upstreamDownstreamOpIdMap_.at(operatorId).second;
    }

    std::vector<operators::StreamingOperator*> StreamingPipeline::GetDownstreamOperators(
            operators::OperatorID operatorId) const
    {
        auto downstreamOpIdVec = upstreamDownstreamOpIdMap_.at(operatorId).second;
        std::vector<operators::StreamingOperator*> downstreamOps;
        downstreamOps.reserve(downstreamOpIdVec.size());
        for (auto downstreamOpId: downstreamOpIdVec) {
            downstreamOps.emplace_back(opIdToOpPtrMap_.at(downstreamOpId));
        }
        return downstreamOps;
    }

    std::vector<operators::StreamingOperator*> StreamingPipeline::GetAllDownstreamOps(
            operators::StreamingOperator* const& pStreamOp) const
    {
        auto downstreamOps = GetDownstreamOperators(pStreamOp->GetOperatorId());
        std::queue<operators::StreamingOperator*> downstreamOpPtrsToExpand{};
        for (auto downstreamOpPtr: downstreamOps) {
            downstreamOpPtrsToExpand.push(downstreamOpPtr);
        }
        while (!downstreamOpPtrsToExpand.empty()) {
            auto expandedOpPtr = downstreamOpPtrsToExpand.front();
            if (!expandedOpPtr->IsSinkOperator()) {
                auto downstreamOpsOfExpansion = GetDownstreamOperators(expandedOpPtr->GetOperatorId());
                for (auto downstreamOfExpandedOpPtr: downstreamOpsOfExpansion) {
                    downstreamOps.emplace_back(downstreamOfExpandedOpPtr);
                    downstreamOpPtrsToExpand.push(downstreamOfExpandedOpPtr);
                }
            }
            downstreamOpPtrsToExpand.pop();
        }
        return downstreamOps;
    }

    runtime::StreamingTask::ProcessingMode StreamingPipeline::GetProcessingMode() const
    {
        return processingMode_;
    }

    void StreamingPipeline::SetProcessingMode(runtime::StreamingTask::ProcessingMode processingMode)
    {
        processingMode_ = processingMode;
    }

    bool StreamingPipeline::IsWaitingToCancel() const
    {
        return waitingToCancel_.load(std::memory_order::acquire);
    }

    const std::unordered_map<operators::OperatorID, uint8_t>& StreamingPipeline::GetOperatorsLevelsMap() const
    {
        return operatorsLevelsMap_;
    }

    const operators::StreamingOperator* StreamingPipeline::GetOperatorForID(const operators::OperatorID opID) const
    {
        return opIdToOpPtrMap_.at(opID);
    }

    uint8_t StreamingPipeline::GetDagDepth() const
    {
        return dagDepth_;
    }

    void StreamingPipeline::SetWaitingToCancel()
    {
        waitingToCancel_.store(true, std::memory_order::release);
    }
}// namespace enjima::core
