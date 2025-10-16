//
// Created by m34ferna on 26/03/24.
//

#include "ThroughputGauge.h"
#include "enjima/runtime/RuntimeUtil.h"

namespace enjima::metrics {
    ThroughputGauge::ThroughputGauge(const Counter<uint64_t>* counter) : Gauge<double>(0.0), counter_(counter) {}

    double ThroughputGauge::GetVal()
    {
        if (lastCalculatedAt_ == 0) {
            lastCounterVal_ = counter_->GetCount();
            lastCalculatedAt_ = enjima::runtime::GetSystemTimeMillis();
            return 0;
        }
        auto currentCounterVal = counter_->GetCount();
        auto currentTime = enjima::runtime::GetSystemTimeMillis();
        auto throughput = static_cast<double>(currentCounterVal - lastCounterVal_) * 1000.0 /
                          static_cast<double>(currentTime - lastCalculatedAt_);
        lastCounterVal_ = currentCounterVal;
        lastCalculatedAt_ = currentTime;
        return throughput;
    }

    ThroughputGauge::~ThroughputGauge() = default;
}// namespace enjima::metrics
