//
// Created by m34ferna on 10/01/24.
//

#include "ExecutionPlan.h"

namespace enjima::core {

    ExecutionPlan::ExecutionPlan(StreamingPipeline* streamingPipeline) : pipeline_(streamingPipeline) {}

    StreamingPipeline* ExecutionPlan::GetPipeline() const
    {
        return pipeline_;
    }

    ExecutionPlan::~ExecutionPlan()
    {
        delete pipeline_;
    }

}// namespace enjima::core
