//
// Created by m34ferna on 10/01/24.
//

#include "StreamingJob.h"
#include "IllegalArgumentException.h"
#include "StreamingTask.h"

namespace enjima::runtime {
    void StreamingJob::AddOperator(UPtrOpT streamingOperator)
    {
        if (!streamingOperator->IsSourceOperator()) {
            throw IllegalArgumentException{
                    "Only a source operator can be added if no upstream operator ID is not specified!"};
        }
        const auto& resultPair =
                opSkeletonDag_.try_emplace(streamingOperator->GetOperatorId(), std::move(streamingOperator),
                        std::vector<operators::OperatorID>{}, std::vector<operators::OperatorID>{});
        if (resultPair.second) {
            auto& resultTuple = resultPair.first->second;
            auto currentOpId = get<0>(resultTuple)->GetOperatorId();
            lastAddedOpID_ = currentOpId;
            srcOpIDs_.emplace_back(currentOpId);
            uint8_t currentOpLevel = 1;
            opIdToLevelsMap_.emplace(currentOpId, currentOpLevel);
            opLevelsToIdMap_[currentOpLevel].emplace_back(currentOpId);
            maxLevel_ = std::max(currentOpLevel, maxLevel_);
        }
        else {
            throw IllegalArgumentException{"Failed attempt to add source operator. Operator already exists!"};
        }
    }

    void StreamingJob::AddOperator(UPtrOpT uPtrStreamOp, operators::OperatorID upstreamOpID)
    {
        if (upstreamOpID > 0) {
            const auto& resultPair = opSkeletonDag_.try_emplace(uPtrStreamOp->GetOperatorId(), std::move(uPtrStreamOp),
                    std::vector<operators::OperatorID>{upstreamOpID}, std::vector<operators::OperatorID>{});
            if (resultPair.second) {
                auto& resultTuple = resultPair.first->second;
                auto currentOpId = get<0>(resultTuple)->GetOperatorId();
                lastAddedOpID_ = currentOpId;
                auto& downstreamOpsOfUpstream = get<2>(opSkeletonDag_.at(upstreamOpID));
                downstreamOpsOfUpstream.emplace_back(currentOpId);
                uint8_t currentOpLevel = opIdToLevelsMap_.at(upstreamOpID) + 1;
                opIdToLevelsMap_.emplace(currentOpId, currentOpLevel);
                opLevelsToIdMap_[currentOpLevel].emplace_back(currentOpId);
                maxLevel_ = std::max(currentOpLevel, maxLevel_);
            }
            else {
                throw IllegalArgumentException{"Failed attempt to add intermediate operator. Operator already exists!"};
            }
        }
        else {
            // Path to support the legacy interface. Can be removed if all such code is refactored.
            if (lastAddedOpID_ == 0) {
                AddOperator(std::move(uPtrStreamOp));
            }
            else {
                // Note that this "recursion" can trigger only once. The following call will
                // pass the argument upstreamOpID != 0, and this code block will never execute in that call.
                AddOperator(std::move(uPtrStreamOp), lastAddedOpID_);
            }
            // End of legacy-supporting code section
        }
    }

    void StreamingJob::AddOperator(UPtrOpT uPtrJoinOp,
            std::pair<operators::OperatorID, operators::OperatorID> upstreamOpIDs)
    {
        std::vector<operators::OperatorID> upstreamOpIdVec;
        upstreamOpIdVec.emplace_back(upstreamOpIDs.first);
        upstreamOpIdVec.emplace_back(upstreamOpIDs.second);
        const auto& resultPair = opSkeletonDag_.try_emplace(uPtrJoinOp->GetOperatorId(), std::move(uPtrJoinOp),
                upstreamOpIdVec, std::vector<operators::OperatorID>{});
        if (resultPair.second) {
            auto& resultTuple = resultPair.first->second;
            auto currentOpId = get<0>(resultTuple)->GetOperatorId();
            lastAddedOpID_ = currentOpId;
            auto& downstreamOpsOfLeftStream = get<2>(opSkeletonDag_.at(upstreamOpIDs.first));
            downstreamOpsOfLeftStream.emplace_back(currentOpId);
            auto& downstreamOpsOfRightStream = get<2>(opSkeletonDag_.at(upstreamOpIDs.second));
            downstreamOpsOfRightStream.emplace_back(currentOpId);
            uint8_t currentOpLevel =
                    std::max(opIdToLevelsMap_.at(upstreamOpIDs.first), opIdToLevelsMap_.at(upstreamOpIDs.second)) + 1;
            opIdToLevelsMap_.emplace(currentOpId, currentOpLevel);
            opLevelsToIdMap_[currentOpLevel].emplace_back(currentOpId);
            maxLevel_ = std::max(currentOpLevel, maxLevel_);
        }
        else {
            throw IllegalArgumentException{"Failed attempt to add join operator. Operator already exists!"};
        }
    }

    std::tuple<UPtrOpT, std::vector<operators::OperatorID>, std::vector<operators::OperatorID>>&
    StreamingJob::GetOperatorInfo(operators::OperatorID opId)
    {
        return opSkeletonDag_.at(opId);
    }

    std::vector<operators::OperatorID> StreamingJob::GetOperatorIDsInTopologicalOrder()
    {
        std::vector<operators::OperatorID> opIdsInOrder;
        for (uint8_t currentLevel = 1; currentLevel <= maxLevel_; currentLevel++) {
            if (opLevelsToIdMap_.contains(currentLevel)) {
                auto opIdsAtLevel = opLevelsToIdMap_.at(currentLevel);
                opIdsInOrder.insert(opIdsInOrder.end(), opIdsAtLevel.begin(), opIdsAtLevel.end());
            }
        }
        return opIdsInOrder;
    }

    StreamingTask::ProcessingMode StreamingJob::GetProcessingMode() const
    {
        return processingMode_;
    }

    void StreamingJob::SetProcessingMode(StreamingTask::ProcessingMode processingMode)
    {
        processingMode_ = processingMode;
    }

    operators::OperatorID StreamingJob::GetStartingSourceOperatorId() const
    {
        operators::OperatorID startSrcOpId = 0;
        uint8_t maxDagDepth = 0;
        for (auto srcOpId: srcOpIDs_) {
            auto currentDagDepth = GetDAGDepth(srcOpId);
            if (currentDagDepth > maxDagDepth) {
                maxDagDepth = currentDagDepth;
                startSrcOpId = srcOpId;
            }
        }
        return startSrcOpId;
    }

    uint8_t StreamingJob::GetDAGDepth(operators::OperatorID opId) const
    {
        auto downstreamOpIds = get<2>(opSkeletonDag_.at(opId));
        if (downstreamOpIds.empty()) {
            return 1;
        }
        else {
            uint8_t maxDepth = 0;
            for (auto downstreamOpId: downstreamOpIds) {
                auto currentDownstreamDepth = GetDAGDepth(downstreamOpId);
                if (currentDownstreamDepth > maxDepth) {
                    maxDepth = currentDownstreamDepth;
                }
            }
            return maxDepth + 1;
        }
    }

    uint8_t StreamingJob::GetOperatorDAGLevel(operators::OperatorID opID) const
    {
        return opIdToLevelsMap_.at(opID);
    }
}// namespace enjima::runtime
