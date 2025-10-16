
#ifndef ENJIMA_RECORD_QUEUE_H
#define ENJIMA_RECORD_QUEUE_H


#define SPSC

#include "VyukovUnboundedSPSCQueue.h"
#include "enjima/queueing/RecordQueueBase.h"
#include "rigtorp/MPMCQueue.h"
#include "rigtorp/SPSCQueue.h"

namespace enjima::queueing {

    template<typename T>
    class RecordQueueImpl : public RecordQueueBase {
    public:
        explicit RecordQueueImpl(const size_t sz) : q(sz), capacity_(sz) {}
        ~RecordQueueImpl() override = default;

        void push(T& v);
        bool try_push(T& v);
        void pop(T& v);
        bool try_pop(T& v);
        size_t size() override;
        bool empty();
        bool full();

    private:
#if ENJIMA_QUEUE_BASED_Q_TYPE == 1
        rigtorp::SPSCQueue<T> q;
#elif ENJIMA_QUEUE_BASED_Q_TYPE == 2
        rigtorp::MPMCQueue<T> q;
#else
        VyukovUnboundedSPSCQueue<T> q;
#endif

        size_t capacity_;
    };

}// namespace enjima::queueing

#include "RecordQueueImpl.tpp"

#endif