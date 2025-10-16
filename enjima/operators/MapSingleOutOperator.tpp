//
// Created by m34ferna on 01/03/24.
//

namespace enjima::operators {
    template<typename TInput, typename TOutput, enjima::api::MapFuncT<TInput, TOutput> TFunc>
    MapOperator<TInput, TOutput, TFunc, 1>::MapOperator(OperatorID opId, const std::string& opName, TFunc mapFunc)
        : SingleInputOperator<TInput, TOutput, 1>(opId, opName), mapFunc_(mapFunc)
    {
    }

    template<typename TInput, typename TOutput, enjima::api::MapFuncT<TInput, TOutput> TFunc>
    void MapOperator<TInput, TOutput, TFunc, 1>::ProcessEvent(uint64_t timestamp, TInput inputEvent,
            enjima::core::OutputCollector* collector)
    {
        collector->CollectWithTimestamp<TOutput>(timestamp, mapFunc_(inputEvent));
    }

    template<typename TInput, typename TOutput, enjima::api::MapFuncT<TInput, TOutput> TFunc>
    void MapOperator<TInput, TOutput, TFunc, 1>::ProcessBatch(void* inputBuffer, uint32_t numRecordsToRead,
            void* outputBuffer, core::OutputCollector* collector)
    {
        auto numEmitted = 0u;
        auto nextOutRecordPtr = static_cast<core::Record<TOutput>*>(outputBuffer);
        for (uint32_t i = 0; i < numRecordsToRead; i++) {
            core::Record<TInput>* inputRecord = static_cast<core::Record<TInput>*>(inputBuffer) + i;
            if (inputRecord->GetRecordType() != core::Record<TInput>::RecordType::kLatency) {
                new (nextOutRecordPtr)
                        core::Record<TOutput>(inputRecord->GetTimestamp(), mapFunc_(inputRecord->GetData()));
            }
            else {
#if ENJIMA_METRICS_LEVEL >= 3
                auto metricsVecPtr = inputRecord->GetAdditionalMetricsPtr();
                metricsVecPtr->emplace_back(runtime::GetSystemTimeMicros());
                new (nextOutRecordPtr) core::Record<TOutput>(core::Record<TOutput>::RecordType::kLatency,
                        inputRecord->GetTimestamp(), metricsVecPtr);
#else
                new (nextOutRecordPtr)
                        core::Record<TOutput>(core::Record<TOutput>::RecordType::kLatency, inputRecord->GetTimestamp());
#endif
            }
            nextOutRecordPtr += 1;
            numEmitted++;
        }

        collector->CollectBatch<TOutput>(outputBuffer, numEmitted);
    }

    template<typename TInput, typename TOutput, enjima::api::MapFuncT<TInput, TOutput> TFunc>
    uint8_t MapOperator<TInput, TOutput, TFunc, 1>::ProcessQueue()
    {
        auto operatorStatus = StreamingOperator::kQueueInitStatus;
        size_t numProcessed = 0;
        while (!this->inputQueue_->empty() && !this->outputQueue_->full() &&
                numProcessed <= this->pMemoryManager_->GetDefaultNumEventsPerBlock()) {
            core::Record<TInput> inputRec;
            this->inputQueue_->pop(inputRec);
            this->inCounter_->Inc();
            numProcessed++;
            if (inputRec.GetRecordType() != core::Record<TInput>::RecordType::kLatency) {
                core::Record<TOutput> record(inputRec.GetTimestamp(), mapFunc_(inputRec.GetData()));
                this->outputQueue_->push(record);
            }
            else {
                core::Record<TOutput> record(core::Record<TOutput>::RecordType::kLatency, inputRec.GetTimestamp());
                this->outputQueue_->push(record);
            }

            this->outCounter_->Inc();
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
