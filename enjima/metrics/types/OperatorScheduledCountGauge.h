//
// Created by m34ferna on 11/06/24.
//

#ifndef ENJIMA_OPERATOR_SCHEDULED_COUNT_GAUGE_H
#define ENJIMA_OPERATOR_SCHEDULED_COUNT_GAUGE_H

#include "Gauge.h"
#include "OperatorCostGauge.h"
#include <cstdint>

namespace enjima::metrics {

    class OperatorScheduledCountGauge : public Gauge<uint64_t> {
    public:
        explicit OperatorScheduledCountGauge(OperatorCostGauge* opCostGauge);
        uint64_t GetVal() override;

    private:
        OperatorCostGauge* opCostGauge_;
    };

}// namespace enjima::metrics


#endif//ENJIMA_OPERATOR_SCHEDULED_COUNT_GAUGE_H
