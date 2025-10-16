//
// Created by m34ferna on 01/03/24.
//

namespace enjima::operators {
    template<typename TLeft, typename TRight, typename TOutput, typename TKey,
            enjima::api::KeyExtractFuncT<TLeft, TKey> TKeyFunc, enjima::api::JoinFuncT<TLeft, TRight, TOutput> TFunc>
    StaticEquiJoinOperator<TLeft, TRight, TOutput, TKey, TKeyFunc, TFunc, 2>::StaticEquiJoinOperator(OperatorID opId,
            const std::string& opName, TKeyFunc keyExtractFunc, TFunc joinFunc,
            UnorderedHashMapST<TKey, TRight>& staticData)
        : SingleInputOperator<TLeft, TOutput, 2>(opId, opName), keyExtractFunc_(keyExtractFunc), joinFunc_(joinFunc)
    {
        rightSide_.swap(staticData);
    }

    template<typename TLeft, typename TRight, typename TOutput, typename TKey,
            enjima::api::KeyExtractFuncT<TLeft, TKey> TKeyFunc, enjima::api::JoinFuncT<TLeft, TRight, TOutput> TFunc>
    void StaticEquiJoinOperator<TLeft, TRight, TOutput, TKey, TKeyFunc, TFunc, 2>::ProcessEvent(uint64_t timestamp,
            TLeft inputEvent, core::OutputCollector* leftOutputCollector, core::OutputCollector* rightOutputCollector)
    {
        auto inputEventKey = keyExtractFunc_(inputEvent);
        if (rightSide_.contains(inputEventKey)) {
            auto outEvent = joinFunc_(inputEvent, rightSide_[inputEventKey]);
            leftOutputCollector->CollectWithTimestamp<TOutput>(timestamp, outEvent);
            rightOutputCollector->CollectWithTimestamp<TOutput>(timestamp, outEvent);
        }
    }

    template<typename TLeft, typename TRight, typename TOutput, typename TKey,
            enjima::api::KeyExtractFuncT<TLeft, TKey> TKeyFunc, enjima::api::JoinFuncT<TLeft, TRight, TOutput> TFunc>
    void StaticEquiJoinOperator<TLeft, TRight, TOutput, TKey, TKeyFunc, TFunc, 2>::ProcessBatch(void* inputBuffer,
            uint32_t numRecordsToRead, void* outputBuffer, core::OutputCollector* leftOutputCollector,
            core::OutputCollector* rightOutputCollector)
    {
        auto numEmitted = 0u;
        auto nextOutRecordPtr = static_cast<core::Record<TOutput>*>(outputBuffer);
        for (uint32_t i = 0; i < numRecordsToRead; i++) {
            core::Record<TLeft>* inputRecord = static_cast<core::Record<TLeft>*>(inputBuffer) + i;
            if (inputRecord->GetRecordType() != core::Record<TLeft>::RecordType::kLatency) {
                auto inputEvent = inputRecord->GetData();
                auto inputEventKey = keyExtractFunc_(inputEvent);
                if (rightSide_.contains(inputEventKey)) {
                    new (nextOutRecordPtr) core::Record<TOutput>(inputRecord->GetTimestamp(),
                            joinFunc_(inputEvent, rightSide_[inputEventKey]));
                    nextOutRecordPtr += 1;
                    numEmitted++;
                }
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
                nextOutRecordPtr += 1;
                numEmitted++;
            }
        }

        leftOutputCollector->CollectBatch<TOutput>(outputBuffer, numEmitted);
        rightOutputCollector->CollectBatch<TOutput>(outputBuffer, numEmitted);
    }

    template<typename TLeft, typename TRight, typename TOutput, typename TKey,
            enjima::api::KeyExtractFuncT<TLeft, TKey> TKeyFunc, enjima::api::JoinFuncT<TLeft, TRight, TOutput> TFunc>
    uint8_t StaticEquiJoinOperator<TLeft, TRight, TOutput, TKey, TKeyFunc, TFunc, 2>::ProcessQueue()
    {
        auto operatorStatus = StreamingOperator::kQueueInitStatus;
        size_t numProcessed = 0;
        while (!this->inputQueue_->empty() && !this->leftOutputQueue_->full() && !this->rightOutputQueue_->full() &&
                numProcessed <= this->pMemoryManager_->GetDefaultNumEventsPerBlock()) {
            core::Record<TLeft> inputRec;
            this->inputQueue_->pop(inputRec);
            this->inCounter_->Inc();
            numProcessed++;
            auto inputEvent = inputRec.GetData();
            auto inputEventKey = keyExtractFunc_(inputEvent);

            if (inputRec.GetRecordType() != core::Record<TLeft>::RecordType::kLatency) {
                if (rightSide_.contains(inputEventKey)) {
                    core::Record<TOutput> record(inputRec.GetTimestamp(),
                            joinFunc_(inputEvent, rightSide_[inputEventKey]));
                    this->leftOutputQueue_->push(record);
                    this->rightOutputQueue_->push(record);
                    this->leftOutCounter_->IncRelaxed();
                    this->rightOutCounter_->IncRelaxed();
                }
            }
            else {
                core::Record<TOutput> record(core::Record<TOutput>::RecordType::kLatency, inputRec.GetTimestamp());
                this->leftOutputQueue_->push(record);
                this->rightOutputQueue_->push(record);
                this->leftOutCounter_->IncRelaxed();
                this->rightOutCounter_->IncRelaxed();
            }
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
