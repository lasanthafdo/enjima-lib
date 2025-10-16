//
// Created by m34ferna on 05/02/24.
//

namespace enjima::operators {
    template<typename TInput, typename Duration>
    NoOpSinkOperator<TInput, Duration>::NoOpSinkOperator(OperatorID opId, const std::string& opName)
        : SinkOperator<TInput, Duration>(opId, opName)
    {
    }

    template<typename TInput, typename Duration>
    size_t NoOpSinkOperator<TInput, Duration>::GetOutputRecordSize() const
    {
        return 0;
    }

    template<typename TInput, typename Duration>
    void NoOpSinkOperator<TInput, Duration>::ProcessSinkEvent(uint64_t timestamp, TInput inputEvent)
    {
    }

    template<typename TInput, typename Duration>
    void NoOpSinkOperator<TInput, Duration>::ProcessSinkBatch(void* inputBuffer, uint32_t numRecordsToRead)
    {
        if (numRecordsToRead > 0) {
            core::Record<TInput>* pInputRecord;
            for (auto i = numRecordsToRead; i > 0; i--) {
                pInputRecord = static_cast<core::Record<TInput>*>(inputBuffer);
                if (pInputRecord->GetRecordType() == core::Record<TInput>::RecordType::kLatency) {
#if ENJIMA_METRICS_LEVEL >= 3
                    auto metricsVecPtr = pInputRecord->GetAdditionalMetricsPtr();
                    metricsVecPtr->emplace_back(runtime::GetSystemTimeMicros());
                    this->latencyHistogram_->AddAdditionalMetrics(metricsVecPtr);
#endif
                    auto currentTimestamp = enjima::runtime::GetSystemTime<Duration>();
                    this->latencyHistogram_->Update(currentTimestamp - pInputRecord->GetTimestamp());
                }
                inputBuffer = static_cast<core::Record<TInput>*>(inputBuffer) + 1;
            }
        }
    }

}// namespace enjima::operators
