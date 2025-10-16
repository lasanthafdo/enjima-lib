//
// Created by m34ferna on 05/02/24.
//

namespace enjima::operators {
    template<typename TEvent, typename TPred>
    FilterOperator<TEvent, TPred>::FilterOperator(OperatorID opId, const std::string& opName, TPred predicate)
        : SingleInputOperator<TEvent, TEvent>(opId, opName), predicate_(predicate)
    {
    }

    template<typename TEvent, typename TPred>
    void FilterOperator<TEvent, TPred>::ProcessEvent(uint64_t timestamp, TEvent inputEvent,
            enjima::core::OutputCollector* collector)
    {
        if (predicate_(inputEvent)) {
            collector->CollectWithTimestamp<TEvent>(timestamp, std::move(inputEvent));
        }
    }

    template<typename TEvent, typename TPred>
    void FilterOperator<TEvent, TPred>::ProcessBatch(void* inputBuffer, uint32_t numRecordsToRead, void* outputBuffer,
            core::OutputCollector* collector)
    {
        auto numEmitted = 0u;
        auto nextOutRecordPtr = static_cast<core::Record<TEvent>*>(outputBuffer);
        for (uint32_t i = 0; i < numRecordsToRead; i++) {
            core::Record<TEvent>* inputRecord = static_cast<core::Record<TEvent>*>(inputBuffer) + i;
            if (inputRecord->GetRecordType() != core::Record<TEvent>::RecordType::kLatency) {
                if (predicate_(inputRecord->GetData())) {
                    new (nextOutRecordPtr) core::Record<TEvent>(inputRecord->GetTimestamp(), inputRecord->GetData());
                    nextOutRecordPtr += 1;
                    numEmitted++;
                }
            }
            else {
#if ENJIMA_METRICS_LEVEL >= 3
                auto metricsVecPtr = inputRecord->GetAdditionalMetricsPtr();
                metricsVecPtr->emplace_back(runtime::GetSystemTimeMicros());
                new (nextOutRecordPtr) core::Record<TEvent>(core::Record<TEvent>::RecordType::kLatency,
                        inputRecord->GetTimestamp(), metricsVecPtr);
#else
                new (nextOutRecordPtr)
                        core::Record<TEvent>(core::Record<TEvent>::RecordType::kLatency, inputRecord->GetTimestamp());
#endif
                nextOutRecordPtr += 1;
                numEmitted++;
            }
        }

        collector->CollectBatch<TEvent>(outputBuffer, numEmitted);
    }

    template<typename TEvent, typename TPred>
    uint8_t FilterOperator<TEvent, TPred>::ProcessQueue()
    {
        auto operatorStatus = StreamingOperator::kQueueInitStatus;
        size_t numProcessed = 0;
        while (!this->inputQueue_->empty() && !this->outputQueue_->full() &&
                numProcessed <= this->pMemoryManager_->GetDefaultNumEventsPerBlock()) {
            core::Record<TEvent> inputRec;
            this->inputQueue_->pop(inputRec);
            this->inCounter_->Inc();
            numProcessed++;
            if (inputRec.GetRecordType() != core::Record<TEvent>::RecordType::kLatency) {
                if (predicate_(inputRec.GetData())) {
                    this->outputQueue_->push(inputRec);
                    this->outCounter_->Inc();
                }
            }
            else {
                this->outputQueue_->push(inputRec);
                this->outCounter_->Inc();
            }
        }
        if (!this->inputQueue_->empty()) {
            operatorStatus |= StreamingOperator::kHasInput;
        }
        if (this->outputQueue_->empty()) {
            operatorStatus &= ~StreamingOperator::kHasOutput;
        }
        if (this->outputQueue_->full()) {
            operatorStatus &= ~StreamingOperator::kCanOutput;
        }
        return operatorStatus;
    }


}// namespace enjima::operators
