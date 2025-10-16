//
// Created by m34ferna on 04/06/24.
//

#include "StreamingTask.h"
#include "ExecutionEngine.h"

namespace enjima::runtime {
    StreamingTask::StreamingTask(ProcessingMode processingMode, std::string threadName,
            ExecutionEngine* pExecutionEngine)
        : processingMode_(processingMode), threadName_(std::move(threadName)), pExecutionEngine_(pExecutionEngine)
    {
    }

    void StreamingTask::PinThreadToCpuListFromConfig(SupportedThreadType threadType)
    {
        pExecutionEngine_->PinThreadToCpuListFromConfig(threadType);
    }
}// namespace enjima::runtime