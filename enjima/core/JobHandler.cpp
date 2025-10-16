//
// Created by m34ferna on 10/01/24.
//

#include "JobHandler.h"

namespace enjima::runtime {
    JobHandler::JobHandler(core::JobID id, core::ExecutionPlan* pExecutionPlan)
        : jobId_(id), pExecutionPlan_(pExecutionPlan)
    {
    }

    JobHandler::~JobHandler()
    {
        delete pExecutionPlan_;
    }

    const core::JobID& JobHandler::GetJobId() const
    {
        return jobId_;
    }

    core::ExecutionPlan* JobHandler::GetExecutionPlan() const
    {
        return pExecutionPlan_;
    }
}// namespace enjima::runtime
