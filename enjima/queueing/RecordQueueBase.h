#ifndef ENJIMA_RECORD_QUEUE_BASE_H
#define ENJIMA_RECORD_QUEUE_BASE_H

namespace enjima::queueing {

    class RecordQueueBase {
    public:
        virtual ~RecordQueueBase() = default;
        virtual size_t size() = 0;
    };

}// namespace enjima::queuing

#endif