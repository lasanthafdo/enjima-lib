//
// Created by m34ferna on 05/02/24.
//

namespace enjima::operators {
    template<typename TInput, SinkFuncT<TInput> TFunc, typename Duration>
    GenericSinkOperator<TInput, TFunc, Duration>::GenericSinkOperator(OperatorID opId, const std::string& opName,
            TFunc sinkFunction)
        : SinkOperator<TInput, Duration>(opId, opName), sinkFunction_(sinkFunction)
    {
    }

    template<typename TInput, SinkFuncT<TInput> TFunc, typename Duration>
    size_t GenericSinkOperator<TInput, TFunc, Duration>::GetOutputRecordSize() const
    {
        return 0;
    }

    template<typename TInput, SinkFuncT<TInput> TFunc, typename Duration>
    void GenericSinkOperator<TInput, TFunc, Duration>::ProcessSinkEvent(uint64_t timestamp, TInput inputEvent)
    {
        sinkFunction_.Execute(timestamp, inputEvent);
    }

    template<typename TInput, SinkFuncT<TInput> TFunc, typename Duration>
    void GenericSinkOperator<TInput, TFunc, Duration>::ProcessSinkBatch(void* inputBuffer, uint32_t numRecordsToRead)
    {
        core::Record<TInput>* pInputRecord;
        for (auto i = numRecordsToRead; i > 0; i--) {
            pInputRecord = static_cast<core::Record<TInput>*>(inputBuffer);
            inputBuffer = static_cast<core::Record<TInput>*>(inputBuffer) + 1;
            if (pInputRecord->GetRecordType() != core::Record<TInput>::RecordType::kLatency) {
                sinkFunction_.Execute(pInputRecord->GetTimestamp(), pInputRecord->GetData());
            }
            else {
#if ENJIMA_METRICS_LEVEL >= 3
                auto metricsVecPtr = pInputRecord->GetAdditionalMetricsPtr();
                metricsVecPtr->emplace_back(runtime::GetSystemTimeMicros());
                this->latencyHistogram_->AddAdditionalMetrics(metricsVecPtr);
#endif
                auto currentTimestamp = enjima::runtime::GetSystemTime<Duration>();
                this->latencyHistogram_->Update(currentTimestamp - pInputRecord->GetTimestamp());
            }
        }
    }

}// namespace enjima::operators
