//
// Created by m34ferna on 24/05/24.
//

#ifndef ENJIMA_OPERATOR_PRECEDENCE_TRACKER_H
#define ENJIMA_OPERATOR_PRECEDENCE_TRACKER_H

#include "OperatorContext.h"
#include "Scheduler.h"
#include "enjima/core/StreamingPipeline.h"
#include "enjima/operators/StreamingOperator.h"
#include "spdlog/spdlog.h"

namespace enjima::runtime {

    template<typename T>
    class OperatorPrecedenceTracker {
    public:
        constexpr static const SchedulingDecisionContext kIneligibleDecisionCtxt = SchedulingDecisionContext{};
        constexpr static const SchedulingContext kIneligibleSchedCtxt = SchedulingContext{&kIneligibleDecisionCtxt};

        OperatorPrecedenceTracker();
        virtual ~OperatorPrecedenceTracker();

        virtual T GetNextInPrecedence(T& prevScheduleInfo) = 0;
        virtual void TrackPipeline(core::StreamingPipeline* pStreamingPipeline);
        virtual bool UnTrackPipeline(core::StreamingPipeline* pStreamingPipeline);
        virtual bool ActivateOperator(operators::StreamingOperator* pStreamingOperator);
        virtual void DeactivateOperator(operators::StreamingOperator* pStreamingOperator,
                std::chrono::milliseconds waitMs);

    protected:
        void TrackPipelineWithoutLocking(core::StreamingPipeline* pStreamingPipeline);
        bool UnTrackPipelineWithoutLocking(core::StreamingPipeline* pStreamingPipeline);

        std::vector<OperatorContext*> currentOpCtxtPtrs_;
        std::vector<core::StreamingPipeline*> currentPipelines_;
        UnorderedHashMapST<const operators::StreamingOperator*, std::vector<operators::StreamingOperator*>>
                downstreamOpsByOpPtrMap_;
        UnorderedHashMapST<const OperatorContext*, SchedulingContext*> schedCtxtMap_;
        UnorderedHashMapST<const OperatorContext*, const core::StreamingPipeline*> opPipelineMap_;
        std::mutex modificationMutex_;
        void* schedMemoryAreaPtr_{nullptr};
        void* schedMemoryCursor_{nullptr};
    };

}// namespace enjima::runtime

#include "OperatorPrecedenceTracker.tpp"

#endif//ENJIMA_OPERATOR_PRECEDENCE_TRACKER_H
