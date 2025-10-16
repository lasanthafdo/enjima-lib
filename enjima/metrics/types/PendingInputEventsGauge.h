//
// Created by m34ferna on 07/05/24.
//

#ifndef ENJIMA_PENDING_INPUT_EVENTS_GAUGE_H
#define ENJIMA_PENDING_INPUT_EVENTS_GAUGE_H

#include "Counter.h"
#include "Gauge.h"
#include <vector>

namespace enjima::metrics {

    class PendingInputEventsGauge : public Gauge<uint64_t> {
    public:
        PendingInputEventsGauge(std::vector<Counter<uint64_t>*>& prevOpOutCounters,
                const Counter<uint64_t>* currentOpInCounter);
        uint64_t GetVal() override;

    private:
        std::vector<Counter<uint64_t>*> prevOpOutCounters_;
        const Counter<uint64_t>* currentOpInCounter_;
    };

}// namespace enjima::metrics


#endif//ENJIMA_PENDING_INPUT_EVENTS_GAUGE_H
