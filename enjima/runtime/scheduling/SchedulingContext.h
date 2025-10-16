//
// Created by m34ferna on 24/05/24.
//

#ifndef ENJIMA_SCHEDULING_CONTEXT_H
#define ENJIMA_SCHEDULING_CONTEXT_H

#include "InternalSchedulingTypes.fwd.h"
#include "enjima/memory/MemoryTypeAliases.h"
#include "enjima/operators/StreamingOperator.h"

#include <atomic>

namespace enjima::runtime {

    class alignas(hardware_constructive_interference_size) SchedulingContext {
    public:
        const uint8_t kInactive_ = 0;
        const uint8_t kReleased_ = 1;
        const uint8_t kAcquired_ = 2;

        constexpr explicit SchedulingContext(const SchedulingDecisionContext* decisionCtxtPtr)
            : opCtxtPtr_(nullptr), decisionCtxtPtr_(const_cast<SchedulingDecisionContext*>(decisionCtxtPtr)) {};
        SchedulingContext(OperatorContext* opCtxtPtr, SchedulingDecisionContext* decisionCtxtPtr);

        void SetPriority(float priority);
        bool TryAcquire();
        bool TryRelease();
        void Deactivate();
        [[nodiscard]] bool IsAvailable() const;
        [[nodiscard]] bool IsActive() const;
        [[nodiscard]] float GetPriority() const;
        [[nodiscard]] OperatorContext* GetOpCtxtPtr() const;
        [[nodiscard]] SchedulingDecisionContext* GetDecisionCtxtPtr() const;

    private:
        OperatorContext* const opCtxtPtr_;
        SchedulingDecisionContext* const decisionCtxtPtr_;
        std::atomic<uint8_t> internalStatus_{kReleased_};
        alignas(hardware_destructive_interference_size) std::atomic<float> priority_{0.0f};
    };

}// namespace enjima::runtime


#endif//ENJIMA_SCHEDULING_CONTEXT_H
