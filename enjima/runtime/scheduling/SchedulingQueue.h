//
// Created by m34ferna on 24/07/24.
//

#ifndef ENJIMA_SCHEDULING_QUEUE_H
#define ENJIMA_SCHEDULING_QUEUE_H

#include "SchedulingContext.h"

namespace enjima::runtime {

    class SchedulingQueue {
    public:
        void Init();
        void Push(SchedulingContext* schedCtxtPtr) const;
        SchedulingContext* Pop();
        void EraseAll(const std::vector<operators::StreamingOperator*>& opVecToBeErased);
        void Emplace(SchedulingContext* schedCtxtPtr);
        [[nodiscard]] SchedulingContext* Get(uint64_t idx) const;
        [[nodiscard]] uint64_t GetActiveSize() const;

    private:
        std::vector<SchedulingContext*> schedCtxtVec_;
        std::atomic<uint64_t> trackedCtxtCnt_{0};
    };

}// namespace enjima::runtime


#endif//ENJIMA_SCHEDULING_QUEUE_H
