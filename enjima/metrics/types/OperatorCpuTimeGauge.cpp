//
// Created by m34ferna on 11/06/24.
//

#include "OperatorCpuTimeGauge.h"

namespace enjima::metrics {
    uint64_t OperatorCpuTimeGauge::GetVal()
    {
        return opCostGauge_->GetTotalOperatorCpuTimeMicros();
    }

    OperatorCpuTimeGauge::OperatorCpuTimeGauge(OperatorCostGauge* opCostGauge)
        : Gauge<uint64_t>(0), opCostGauge_(opCostGauge)
    {
    }
}// namespace enjima::metrics
