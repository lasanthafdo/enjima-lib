//
// Created by m34ferna on 02/02/24.
//

#ifndef ENJIMA_SCHEDULER_H
#define ENJIMA_SCHEDULER_H

#include "SchedulingDecisionContext.h"
#include "SchedulingTypes.h"
#include "enjima/core/CoreInternals.fwd.h"
#include "enjima/operators/OperatorsInternals.fwd.h"
#include "enjima/runtime/StreamingTask.h"
#include <vector>

namespace enjima::runtime {

    class Scheduler {
    public:
        inline static const size_t kMaxSchedulerQueueSize = 1024;

        virtual ~Scheduler() = default;
        virtual void Init();
        virtual void Shutdown();
        virtual void RegisterPipeline(core::StreamingPipeline* pStreamPipeline);
        virtual void DeRegisterPipeline(core::StreamingPipeline* pStreamPipeline);
        virtual void ActivateOperator(operators::StreamingOperator* pStreamOp) = 0;
        virtual void DeactivateOperator(operators::StreamingOperator* pStreamOp, std::chrono::milliseconds waitMs) = 0;
        virtual void RegisterTask() = 0;
        [[nodiscard]] bool ContainsRunnableOperators() const;
        [[nodiscard]] virtual SchedulingDecisionContext* GetNextOperator() = 0;
        [[nodiscard]] StreamingTask::ProcessingMode GetProcessingMode() const;

    protected:
        std::atomic<bool> schedulerRunning_{false};
        std::vector<operators::StreamingOperator*> runnableOperators_;
        std::vector<core::StreamingPipeline*> runnablePipelines_;
        std::mutex containerMutex_;
        StreamingTask::ProcessingMode processingMode_{StreamingTask::ProcessingMode::kUnassigned};
        std::atomic<bool> schedQEmpty_{true};
    };

}// namespace enjima::runtime

#endif//ENJIMA_SCHEDULER_H
