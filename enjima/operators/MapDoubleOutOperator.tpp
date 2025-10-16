//
// Created by m34ferna on 01/03/24.
//

namespace enjima::operators {
    template<typename TInput, typename TOutput, enjima::api::MapFuncT<TInput, TOutput> TFunc>
    MapOperator<TInput, TOutput, TFunc, 2>::MapOperator(OperatorID opId, const std::string& opName, TFunc mapFunc)
        : SingleInputOperator<TInput, TOutput, 2>(opId, opName), mapFunc_(mapFunc)
    {
    }

    template<typename TInput, typename TOutput, enjima::api::MapFuncT<TInput, TOutput> TFunc>
    void MapOperator<TInput, TOutput, TFunc, 2>::ProcessEvent(uint64_t timestamp, TInput inputEvent,
            core::OutputCollector* leftOutputCollector, core::OutputCollector* rightOutputCollector)
    {
        auto outEvent = mapFunc_(inputEvent);
        leftOutputCollector->CollectWithTimestamp<TOutput>(timestamp, outEvent);
        rightOutputCollector->CollectWithTimestamp<TOutput>(timestamp, outEvent);
    }

    template<typename TInput, typename TOutput, enjima::api::MapFuncT<TInput, TOutput> TFunc>
    void MapOperator<TInput, TOutput, TFunc, 2>::ProcessBatch(void* inputBuffer, uint32_t numRecordsToRead,
            void* outputBuffer, core::OutputCollector* leftOutputCollector, core::OutputCollector* rightOutputCollector)
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

        leftOutputCollector->CollectBatch<TOutput>(outputBuffer, numEmitted);
        rightOutputCollector->CollectBatch<TOutput>(outputBuffer, numEmitted);
    }

    template<typename TInput, typename TOutput, enjima::api::MapFuncT<TInput, TOutput> TFunc>
    uint8_t MapOperator<TInput, TOutput, TFunc, 2>::ProcessQueue()
    {
        auto operatorStatus = StreamingOperator::kQueueInitStatus;
        size_t numProcessed = 0;
        while (!this->inputQueue_->empty() && !this->leftOutputQueue_->full() && !this->rightOutputQueue_->full() &&
                numProcessed <= this->pMemoryManager_->GetDefaultNumEventsPerBlock()) {
            core::Record<TInput> inputRec;
            this->inputQueue_->pop(inputRec);
            this->inCounter_->Inc();
            numProcessed++;
            if (inputRec.GetRecordType() != core::Record<TInput>::RecordType::kLatency) {
                core::Record<TOutput> record(inputRec.GetTimestamp(), mapFunc_(inputRec.GetData()));
                this->leftOutputQueue_->push(record);
                this->rightOutputQueue_->push(record);
            }
            else {
                core::Record<TOutput> record(core::Record<TOutput>::RecordType::kLatency, inputRec.GetTimestamp());
                this->leftOutputQueue_->push(record);
                this->rightOutputQueue_->push(record);
            }

            this->leftOutCounter_->Inc();
            this->rightOutCounter_->Inc();
        }
        if (!this->inputQueue_->empty()) {
            operatorStatus |= StreamingOperator::kHasInput;
        }
        if (this->leftOutputQueue_->empty() && this->rightOutputQueue_->empty()) {
            operatorStatus &= ~StreamingOperator::kHasOutput;
        }
        if (this->leftOutputQueue_->full() || this->rightOutputQueue_->full()) {
            operatorStatus &= ~StreamingOperator::kCanOutput;
        }
        return operatorStatus;
    }
}// namespace enjima::operators
