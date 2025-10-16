//
// Created by m34ferna on 24/07/24.
//

#include "SchedulingQueue.h"
#include "OperatorContext.h"
#include "enjima/runtime/IllegalArgumentException.h"

namespace enjima::runtime {

    SchedulingContext* SchedulingQueue::Pop()
    {
        auto currentCtxtCnt = trackedCtxtCnt_.load(std::memory_order::acquire);
        while (currentCtxtCnt > 0) {
            // Note: If we use lowest, the worker threads will pick priority 0 tasks and will always be busy
            // auto maxVal = std::numeric_limits<float>::lowest();
            auto maxVal = std::numeric_limits<float>::min();
            size_t maxElementIndex = 0;
            auto found = false;
            for (size_t i = 0; i < currentCtxtCnt; i++) {
                auto currentSchedCtxtPtr = schedCtxtVec_.at(i);
                auto currentElemPriority = currentSchedCtxtPtr->GetPriority();
                if (currentSchedCtxtPtr->IsAvailable() && maxVal < currentElemPriority) {
                    found = true;
                    maxVal = currentElemPriority;
                    maxElementIndex = i;
                }
            }
            if (!found) {
                return nullptr;
            }
            auto candidateSchedCtxtPtr = schedCtxtVec_.at(maxElementIndex);
            if (candidateSchedCtxtPtr->TryAcquire()) {
                return candidateSchedCtxtPtr;
            }
        }
        return nullptr;
    }

    void SchedulingQueue::Push(SchedulingContext* schedCtxtPtr) const
    {
        schedCtxtPtr->TryRelease();
    }

    void SchedulingQueue::Emplace(SchedulingContext* schedCtxtPtr)
    {
        auto currentCtxtCnt = trackedCtxtCnt_.load(std::memory_order::acquire);
        if (currentCtxtCnt < Scheduler::kMaxSchedulerQueueSize) {
            schedCtxtVec_[currentCtxtCnt] = schedCtxtPtr;
            trackedCtxtCnt_.fetch_add(1, std::memory_order::acq_rel);
        }
        else {
            throw runtime::IllegalArgumentException{std::string("Cannot support more than ")
                            .append(std::to_string(Scheduler::kMaxSchedulerQueueSize))
                            .append(" operators currently!")};
        }
    }

    void SchedulingQueue::EraseAll(const std::vector<operators::StreamingOperator*>& opVecToBeErased)
    {
        for (auto i = 0ul; i < trackedCtxtCnt_.load(std::memory_order::acquire); i++) {
            auto schedCtxtPtr = schedCtxtVec_.at(i);
            auto opPtr = schedCtxtPtr->GetOpCtxtPtr()->GetOperatorPtr();
            if (std::find(opVecToBeErased.cbegin(), opVecToBeErased.cend(), opPtr) != opVecToBeErased.cend()) {
                schedCtxtPtr->Deactivate();
            }
        }
    }

    void SchedulingQueue::Init()
    {
        schedCtxtVec_.reserve(Scheduler::kMaxSchedulerQueueSize);
        for (auto i = 0ul; i < Scheduler::kMaxSchedulerQueueSize; i++) {
            schedCtxtVec_.emplace_back(nullptr);
        }
    }

    SchedulingContext* SchedulingQueue::Get(uint64_t idx) const
    {
        return schedCtxtVec_.at(idx);
    }

    uint64_t SchedulingQueue::GetActiveSize() const
    {
        return trackedCtxtCnt_.load(std::memory_order::acquire);
    }

}// namespace enjima::runtime
