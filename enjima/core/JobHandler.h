//
// Created by m34ferna on 10/01/24.
//

#ifndef ENJIMA_JOB_HANDLER_H
#define ENJIMA_JOB_HANDLER_H

#include "CoreTypeAliases.h"
#include "ExecutionPlan.h"

namespace enjima::runtime {

    class JobHandler {
    public:
        explicit JobHandler(core::JobID id, core::ExecutionPlan* pExecutionPlan);
        virtual ~JobHandler();
        [[nodiscard]] const core::JobID& GetJobId() const;
        [[nodiscard]] core::ExecutionPlan* GetExecutionPlan() const;

    private:
        core::JobID jobId_;
        core::ExecutionPlan* pExecutionPlan_;
    };
}// namespace enjima::runtime

#endif//ENJIMA_JOB_HANDLER_H
