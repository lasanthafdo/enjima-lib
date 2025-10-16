//
// Created by m34ferna on 07/05/24.
//

#include "PendingInputEventsGauge.h"
#include <numeric>

namespace enjima::metrics {

    PendingInputEventsGauge::PendingInputEventsGauge(std::vector<Counter<uint64_t>*>& prevOpOutCounters,
            const Counter<uint64_t>* currentOpInCounter)
        : Gauge<uint64_t>(0), prevOpOutCounters_(prevOpOutCounters.begin(), prevOpOutCounters.end()),
          currentOpInCounter_(currentOpInCounter)
    {
    }

    uint64_t PendingInputEventsGauge::GetVal()
    {
        auto outCountersSum = std::accumulate(prevOpOutCounters_.begin(), prevOpOutCounters_.end(), 0ul,
                [](uint64_t a, Counter<uint64_t>* b) { return a + b->GetCount(); });
        return outCountersSum - currentOpInCounter_->GetCount();
    }
}// namespace enjima::metrics
