//
// Created by m34ferna on 10/01/24.
//

#ifndef ENJIMA_STREAMING_JOB_H
#define ENJIMA_STREAMING_JOB_H

#include "StreamingTask.h"
#include "enjima/operators/StreamingOperator.h"
#include <memory>
#include <vector>

namespace enjima::runtime {

    using UPtrOpT = std::unique_ptr<operators::StreamingOperator>;

    class StreamingJob {
    public:
        void AddOperator(UPtrOpT streamingOperator);
        void AddOperator(UPtrOpT streamingOperator, operators::OperatorID upstreamOpID);
        void AddOperator(UPtrOpT uPtrJoinOp, std::pair<operators::OperatorID, operators::OperatorID> upstreamOpIDs);
        std::tuple<UPtrOpT, std::vector<operators::OperatorID>, std::vector<operators::OperatorID>>& GetOperatorInfo(
                operators::OperatorID opId);
        [[nodiscard]] std::vector<operators::OperatorID> GetOperatorIDsInTopologicalOrder();
        uint8_t GetOperatorDAGLevel(operators::OperatorID opID) const;
        operators::OperatorID GetStartingSourceOperatorId() const;
        uint8_t GetDAGDepth(operators::OperatorID opId) const;
        [[nodiscard]] StreamingTask::ProcessingMode GetProcessingMode() const;
        void SetProcessingMode(StreamingTask::ProcessingMode processingMode);

    private:
        // Map of (currentOpId -> tuple(ptr, upstreamVec, downstreamVec))
        std::unordered_map<operators::OperatorID,
                std::tuple<UPtrOpT, std::vector<operators::OperatorID>, std::vector<operators::OperatorID>>>
                opSkeletonDag_;
        StreamingTask::ProcessingMode processingMode_{StreamingTask::ProcessingMode::kUnassigned};
        operators::OperatorID lastAddedOpID_{0};
        std::vector<operators::OperatorID> srcOpIDs_;
        std::unordered_map<operators::OperatorID, uint8_t> opIdToLevelsMap_;
        std::unordered_map<uint8_t, std::vector<operators::OperatorID>> opLevelsToIdMap_;
        uint8_t maxLevel_{0};
    };

};// namespace enjima::runtime


#endif//ENJIMA_STREAMING_JOB_H
