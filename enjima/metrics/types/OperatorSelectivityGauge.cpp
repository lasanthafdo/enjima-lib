//
// Created by m34ferna on 07/05/24.
//

#include "OperatorSelectivityGauge.h"
#include "enjima/runtime/RuntimeUtil.h"

namespace enjima::metrics {
    OperatorSelectivityGauge::OperatorSelectivityGauge(const Counter<uint64_t>* inCounter,
            const Counter<uint64_t>* outCounter, uint64_t updateIntervalMs)
        : Gauge<double>(1.0), inCounter_(inCounter), outCounter_(outCounter),
          lastCalculatedAt_(enjima::runtime::GetSystemTimeMillis()), updateIntervalMs_(updateIntervalMs)
    {
    }

    double OperatorSelectivityGauge::GetVal()
    {
        auto expectedGuardVal = false;
        if (calcGuard_.compare_exchange_strong(expectedGuardVal, true, std::memory_order::acq_rel)) {
            auto currentTimeMs = enjima::runtime::GetSystemTimeMillis();
            if (currentTimeMs - lastCalculatedAt_ >= updateIntervalMs_) {
                auto currentInCount = inCounter_->GetCount();
                auto currentOutCount = outCounter_->GetCount();
                auto inEvents = currentInCount - lastInCount_;
                auto outEvents = currentOutCount - lastOutCount_;
                if (inEvents > 0 && outEvents > 0) {
                    lasSelectivity_.store((double) outEvents / (double) inEvents, std::memory_order::release);
                    lastInCount_ = currentInCount;
                    lastOutCount_ = currentOutCount;
                }
                lastCalculatedAt_ = currentTimeMs;
            }
            calcGuard_.store(false, std::memory_order::release);
        }
        return lasSelectivity_.load(std::memory_order::acquire);
    }
}// namespace enjima::metrics
