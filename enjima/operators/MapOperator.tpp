//
// Created by m34ferna on 01/03/24.
//

namespace enjima::operators {
    template<typename TInput, typename TOutput, enjima::api::MapFuncT<TInput, TOutput> TFunc, int NOuts>
    MapOperator<TInput, TOutput, TFunc, NOuts>::MapOperator(OperatorID opId, const std::string& opName, TFunc mapFunc)
        : SingleInputOperator<TInput, TOutput, NOuts>(opId, opName), mapFunc_(mapFunc)
    {
    }

    template<typename TInput, typename TOutput, enjima::api::MapFuncT<TInput, TOutput> TFunc, int NOuts>
    void MapOperator<TInput, TOutput, TFunc, NOuts>::ProcessEvent(uint64_t timestamp, TInput inputEvent,
            std::array<enjima::core::OutputCollector*, NOuts> collectorArr)
    {
        auto outEvent = mapFunc_(inputEvent);
        for (enjima::core::OutputCollector* collector: collectorArr) {
            collector->CollectWithTimestamp<TOutput>(timestamp, outEvent);
        }
    }

    template<typename TInput, typename TOutput, enjima::api::MapFuncT<TInput, TOutput> TFunc, int NOuts>
    void MapOperator<TInput, TOutput, TFunc, NOuts>::ProcessBatch(void* inputBuffer, uint32_t numRecordsToRead,
            void* outputBuffer, std::array<enjima::core::OutputCollector*, NOuts> collectorArr)
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

        for (enjima::core::OutputCollector* collector: collectorArr) {
            collector->CollectBatch<TOutput>(outputBuffer, numEmitted);
        }
    }

    template<typename TInput, typename TOutput, enjima::api::MapFuncT<TInput, TOutput> TFunc, int NOuts>
    uint8_t MapOperator<TInput, TOutput, TFunc, NOuts>::ProcessQueue()
    {
        auto operatorStatus = StreamingOperator::kQueueInitStatus;
        size_t numProcessed = 0;
        while (!this->inputQueue_->empty() &&
                std::all_of(this->outputQueueArr_.cbegin(), this->outputQueueArr_.cend(),
                        [](auto outputQueuePtr) { return !outputQueuePtr->full(); }) &&
                numProcessed <= this->pMemoryManager_->GetDefaultNumEventsPerBlock()) {
            core::Record<TInput> inputRec;
            this->inputQueue_->pop(inputRec);
            this->inCounter_->Inc();
            numProcessed++;
            if (inputRec.GetRecordType() != core::Record<TInput>::RecordType::kLatency) {
                core::Record<TOutput> record(inputRec.GetTimestamp(), mapFunc_(inputRec.GetData()));
                for (auto& outputQueue: this->outputQueueArr_) {
                    outputQueue->push(record);
                }
            }
            else {
                core::Record<TOutput> record(core::Record<TOutput>::RecordType::kLatency, inputRec.GetTimestamp());
                for (auto& outputQueue: this->outputQueueArr_) {
                    outputQueue->push(record);
                }
            }
            for (auto& outCounter: this->outCounterArr_) {
                outCounter->Inc();
            }
        }
        if (!this->inputQueue_->empty()) {
            operatorStatus |= StreamingOperator::kHasInput;
        }
        if (std::all_of(this->outputQueueArr_.cbegin(), this->outputQueueArr_.cend(),
                    [](auto outputQueuePtr) { return outputQueuePtr->empty(); })) {
            operatorStatus &= ~StreamingOperator::kHasOutput;
        }
        if (std::any_of(this->outputQueueArr_.cbegin(), this->outputQueueArr_.cend(),
                    [](auto outputQueuePtr) { return outputQueuePtr->full(); })) {
            operatorStatus &= ~StreamingOperator::kCanOutput;
        }
        return operatorStatus;
    }
}// namespace enjima::operators
