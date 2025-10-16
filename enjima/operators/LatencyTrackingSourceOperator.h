//
// Created by m34ferna on 27/04/24.
//

#ifndef ENJIMA_LATENCY_TRACKING_SOURCE_OPERATOR_H
#define ENJIMA_LATENCY_TRACKING_SOURCE_OPERATOR_H

#include "SourceOperator.h"

namespace enjima::operators {

    template<typename TOutput, typename Duration>
    class LatencyTrackingSourceOperator : public SourceOperator<TOutput> {
    public:
        static constexpr uint8_t kConstantRate_ = 0;
        static constexpr uint8_t kStepIncrease_ = 1;
        static constexpr uint8_t kUnpredictableRateVariationsInRange_ = 2;
        static constexpr uint8_t kUserDefinedRateChange_ = 4;
        static constexpr uint8_t kConceptDrift_ = 8;

        LatencyTrackingSourceOperator(OperatorID opId, const std::string& opName, uint64_t latencyRecordEmitPeriodMs);

    protected:
        uint64_t latencyRecordEmitPeriodMs_{0};
        uint64_t latencyRecordLastEmittedAt_{0};
        bool emitLatencyMarker_{false};
        uint64_t latencyMarkerTime_{0};
    };

}// namespace enjima::operators

#include "LatencyTrackingSourceOperator.tpp"

#endif//ENJIMA_LATENCY_TRACKING_SOURCE_OPERATOR_H
