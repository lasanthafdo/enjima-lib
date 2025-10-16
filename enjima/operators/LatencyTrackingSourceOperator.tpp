//
// Created by m34ferna on 27/04/24.
//

namespace enjima::operators {
    template<typename TOutput, typename Duration>
    LatencyTrackingSourceOperator<TOutput, Duration>::LatencyTrackingSourceOperator(OperatorID opId,
            const std::string& opName, uint64_t latencyRecordEmitPeriodMs)
        : SourceOperator<TOutput>(opId, opName), latencyRecordEmitPeriodMs_(latencyRecordEmitPeriodMs)
    {
    }

}// namespace enjima::operators
