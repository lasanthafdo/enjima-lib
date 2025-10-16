//
// Created by m34ferna on 26/03/24.
//

#ifndef ENJIMA_THROUGHPUT_GAUGE_H
#define ENJIMA_THROUGHPUT_GAUGE_H

#include "Counter.h"
#include "Gauge.h"

namespace enjima::metrics {

    class ThroughputGauge : public Gauge<double> {
    public:
        explicit ThroughputGauge(const Counter<uint64_t>* counter);
        double GetVal() override;
        ~ThroughputGauge() override;

    private:
        const Counter<uint64_t>* counter_;
        uint64_t lastCounterVal_{0};
        uint64_t lastCalculatedAt_{0};
    };

}// namespace enjima::metrics


#endif//ENJIMA_THROUGHPUT_GAUGE_H
