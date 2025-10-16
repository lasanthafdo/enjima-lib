
namespace enjima::queueing {
    template<typename T>
    void RecordQueueImpl<T>::push(T& v)
    {
#if ENJIMA_QUEUE_BASED_Q_TYPE == 0
        while (!q.enqueue(v)) {};
#else
        q.push(v);
#endif
    }

    template<typename T>
    bool RecordQueueImpl<T>::try_push(T& v)
    {
#if ENJIMA_QUEUE_BASED_Q_TYPE == 0
        return q.enqueue(v);
#else
        return q.try_push(v);
#endif
    }

    template<typename T>
    void RecordQueueImpl<T>::pop(T& v)
    {
#if ENJIMA_QUEUE_BASED_Q_TYPE == 1
        T* head = q.front();
        assert(head);
        if (head != nullptr) {
            v = *head;
            q.pop();
        }
#elif ENJIMA_QUEUE_BASED_Q_TYPE == 2
        q.pop(v);
#else
        while (!q.dequeue(v)) {};
#endif
    }

    template<typename T>
    bool RecordQueueImpl<T>::try_pop(T& v)
    {
#if ENJIMA_QUEUE_BASED_Q_TYPE == 1
        T* head = q.front();
        if (head != nullptr) {
            v = *head;
            q.pop();
            return true;
        }
        return false;
#elif ENJIMA_QUEUE_BASED_Q_TYPE == 2
        return q.try_pop(v);
#else
        return q.dequeue(v);
#endif
    }

    template<typename T>
    size_t RecordQueueImpl<T>::size()
    {
        return q.size();
    }

    template<typename T>
    bool RecordQueueImpl<T>::empty()
    {
#if ENJIMA_QUEUE_BASED_Q_TYPE == 1
        return q.empty();
#elif ENJIMA_QUEUE_BASED_Q_TYPE == 2
        return q.empty();
#else
        return q.empty();
#endif
    }

    template<typename T>
    bool RecordQueueImpl<T>::full()
    {
#if ENJIMA_QUEUE_BASED_Q_TYPE == 1
        return q.size() >= q.capacity();
#elif ENJIMA_QUEUE_BASED_Q_TYPE == 2
        return q.size() >= capacity_;
#else
        return q.full();
#endif
    }

}// namespace enjima::queueing