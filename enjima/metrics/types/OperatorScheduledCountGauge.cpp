//
// Created by m34ferna on 11/06/24.
//

#include "OperatorScheduledCountGauge.h"

namespace enjima::metrics {
    uint64_t OperatorScheduledCountGauge::GetVal()
    {
        return opCostGauge_->GetNumTimesScheduled();
    }

    OperatorScheduledCountGauge::OperatorScheduledCountGauge(OperatorCostGauge* opCostGauge)
        : Gauge<uint64_t>(0), opCostGauge_(opCostGauge)
    {
    }
}// namespace enjima::metrics
