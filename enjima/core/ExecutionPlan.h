//
// Created by m34ferna on 10/01/24.
//

#ifndef ENJIMA_EXECUTION_PLAN_H
#define ENJIMA_EXECUTION_PLAN_H

#include "StreamingPipeline.h"
#include "enjima/runtime/StreamingJob.h"

namespace enjima::core {

    class ExecutionPlan {
    public:
        explicit ExecutionPlan(StreamingPipeline* streamingPipeline);
        ~ExecutionPlan();
        [[nodiscard]] StreamingPipeline* GetPipeline() const;

    private:
        StreamingPipeline* pipeline_;
    };

}// namespace enjima::core


#endif//ENJIMA_EXECUTION_PLAN_H
