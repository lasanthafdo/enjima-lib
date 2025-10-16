//
// Created by m34ferna on 01/03/24.
//

namespace enjima::operators {
    template<typename TInput, typename TOutput, bool IsKeyedT, enjima::api::AggFuncT<TInput, TOutput, IsKeyedT> TFunc>
    FixedEventTimeWindowOperator<TInput, TOutput, IsKeyedT, TFunc>::FixedEventTimeWindowOperator(OperatorID opId,
            const std::string& opName, TFunc aggFunc, std::chrono::milliseconds windowSizeMillis)
        : SingleInputOperator<TInput, TOutput>(opId, opName), windowSizeMillis_(windowSizeMillis.count()),
          aggFunc_(aggFunc)
    {
    }

    template<typename TInput, typename TOutput, bool IsKeyedT, enjima::api::AggFuncT<TInput, TOutput, IsKeyedT> TFunc>
    void FixedEventTimeWindowOperator<TInput, TOutput, IsKeyedT, TFunc>::ProcessEvent(uint64_t timestamp,
            TInput inputEvent, enjima::core::OutputCollector* collector)
    {
        assert(overflowOutputRecordBuf_.empty());
        if (nextWindowTriggerAt_ == 0) {
            nextWindowTriggerAt_ = timestamp + windowSizeMillis_;
        }
        if (timestamp > maxWindowTimestamp_) {
            maxWindowTimestamp_ = timestamp;
        }
        if (maxWindowTimestamp_ >= nextWindowTriggerAt_) {
            auto maxWritableEvents = this->outputBlockPtr_->template GetNumWritableEvents<TOutput>();
            bool outputOverflow = false;
            auto numEmitted = 0u;
            auto resultMap = aggFunc_.GetResult();
            for (const auto& emittedEvent: resultMap) {
                auto outTimestamp = nextWindowTriggerAt_ - 1;
                if (outputOverflow || numEmitted >= maxWritableEvents) {
                    overflowOutputRecordBuf_.emplace_back(outTimestamp, emittedEvent);
                    if (!outputOverflow) {
                        outputOverflow = true;
                    }
                }
                else {
                    collector->CollectWithTimestamp<TOutput>(outTimestamp, emittedEvent);
                }
            }
            if (outputOverflow) {
                overflowOutputRecordBufIterator_ = overflowOutputRecordBuf_.begin();
                this->hasPendingOverflow_ = true;
                ProcessPendingOverflow(collector);
            }
            nextWindowTriggerAt_ += windowSizeMillis_;
        }
        aggFunc_(inputEvent);
    }

    template<typename TInput, typename TOutput, bool IsKeyedT, enjima::api::AggFuncT<TInput, TOutput, IsKeyedT> TFunc>
    void FixedEventTimeWindowOperator<TInput, TOutput, IsKeyedT, TFunc>::ProcessPendingOverflow(
            core::OutputCollector* collector)
    {
        assert((overflowOutputRecordBuf_.end() - overflowOutputRecordBufIterator_) <
                std::numeric_limits<uint32_t>::max());
        while (overflowOutputRecordBufIterator_ != overflowOutputRecordBuf_.end()) {
            collector->Collect(*overflowOutputRecordBufIterator_);
            overflowOutputRecordBufIterator_++;
            if (!SetNextOutputBlockIfNecessary(collector)) {
                break;
            }
        }
        assert(overflowOutputRecordBufIterator_ <= overflowOutputRecordBuf_.end());
        if (overflowOutputRecordBufIterator_ == overflowOutputRecordBuf_.end()) {
            overflowOutputRecordBuf_.clear();
            this->hasPendingOverflow_ = false;
        }
    }

    template<typename TInput, typename TOutput, bool IsKeyedT, enjima::api::AggFuncT<TInput, TOutput, IsKeyedT> TFunc>
    void FixedEventTimeWindowOperator<TInput, TOutput, IsKeyedT, TFunc>::ProcessBatch(void* inputBuffer,
            uint32_t numRecordsToRead, void* outputBuffer, core::OutputCollector* collector)
    {
        assert(overflowOutputRecordBuf_.empty());
        if (nextWindowTriggerAt_ == 0) {
            // numRecordsToRead shouldn't be 0, ever!!
            auto firstInputRecord = static_cast<core::Record<TInput>*>(inputBuffer);
            uint32_t tmp = 1;
            while (firstInputRecord->GetRecordType() == core::Record<TInput>::RecordType::kLatency &&
                    tmp < numRecordsToRead) {
                firstInputRecord = static_cast<core::Record<TInput>*>(inputBuffer) + tmp;
                tmp++;
            }
            if (firstInputRecord->GetRecordType() == core::Record<TInput>::RecordType::kData) {
                nextWindowTriggerAt_ = firstInputRecord->GetTimestamp() + windowSizeMillis_;
            }
        }
        auto maxWritableEvents = this->outputBlockPtr_->template GetNumWritableEvents<TOutput>();
        bool outputOverflow = false;
        auto nextOutRecordPtr = static_cast<core::Record<TOutput>*>(outputBuffer);
        auto numEmitted = 0u;
        for (uint32_t i = 0; i < numRecordsToRead; i++) {
            auto inputRecord = static_cast<core::Record<TInput>*>(inputBuffer) + i;
            if (inputRecord->GetRecordType() != core::Record<TInput>::RecordType::kLatency) {
                auto timestamp = inputRecord->GetTimestamp();
                if (timestamp > maxWindowTimestamp_) {
                    maxWindowTimestamp_ = timestamp;
                }
                if (maxWindowTimestamp_ >= nextWindowTriggerAt_) {
                    auto resultMap = aggFunc_.GetResult();
                    auto outTimestamp = nextWindowTriggerAt_ - 1;
                    for (const auto& emittedEvent: resultMap) {
                        if (outputOverflow || numEmitted >= maxWritableEvents) {
                            overflowOutputRecordBuf_.emplace_back(outTimestamp, emittedEvent);
                            if (!outputOverflow) {
                                outputOverflow = true;
                            }
                        }
                        else {
                            new (nextOutRecordPtr) core::Record<TOutput>(outTimestamp, emittedEvent);
                            nextOutRecordPtr += 1;
                            numEmitted++;
                        }
                    }
                    nextWindowTriggerAt_ += windowSizeMillis_;
                }
                aggFunc_(inputRecord->GetData());
            }
            else {
                if (outputOverflow || numEmitted >= maxWritableEvents) {
#if ENJIMA_METRICS_LEVEL >= 3
                    auto metricsVecPtr = inputRecord->GetAdditionalMetricsPtr();
                    metricsVecPtr->emplace_back(runtime::GetSystemTimeMicros());
                    overflowRecordsBufferVec_.emplace_back(core::Record<TOutput>::RecordType::kLatency,
                            inputRecord->GetTimestamp(), metricsVecPtr);
#else
                    overflowOutputRecordBuf_.emplace_back(core::Record<TOutput>::RecordType::kLatency,
                            inputRecord->GetTimestamp());
#endif
                    if (!outputOverflow) {
                        outputOverflow = true;
                    }
                }
                else {
#if ENJIMA_METRICS_LEVEL >= 3
                    auto metricsVecPtr = inputRecord->GetAdditionalMetricsPtr();
                    metricsVecPtr->emplace_back(runtime::GetSystemTimeMicros());
                    new (nextOutRecordPtr) core::Record<TOutput>(core::Record<TOutput>::RecordType::kLatency,
                            inputRecord->GetTimestamp(), metricsVecPtr);
#else
                    new (nextOutRecordPtr) core::Record<TOutput>(core::Record<TOutput>::RecordType::kLatency,
                            inputRecord->GetTimestamp());
#endif
                    nextOutRecordPtr += 1;
                    numEmitted++;
                }
            }
        }
        collector->CollectBatch<TOutput>(outputBuffer, numEmitted);
        if (outputOverflow) {
            overflowOutputRecordBufIterator_ = overflowOutputRecordBuf_.begin();
            this->hasPendingOverflow_ = true;
            ProcessPendingOverflowBatch(collector);
        }
    }

    template<typename TInput, typename TOutput, bool IsKeyedT, enjima::api::AggFuncT<TInput, TOutput, IsKeyedT> TFunc>
    void FixedEventTimeWindowOperator<TInput, TOutput, IsKeyedT, TFunc>::ProcessPendingOverflowBatch(
            core::OutputCollector* collector)
    {
        assert((overflowOutputRecordBuf_.end() - overflowOutputRecordBufIterator_) <
                std::numeric_limits<uint32_t>::max());
        while (overflowOutputRecordBufIterator_ != overflowOutputRecordBuf_.end()) {
            assert(overflowOutputRecordBufIterator_ < overflowOutputRecordBuf_.end());
            auto remainingOverflow = overflowOutputRecordBuf_.end() - overflowOutputRecordBufIterator_;
            auto numEventsToEmitThisBatch = std::min(this->outputBlockPtr_->template GetNumWritableEvents<TOutput>(),
                    static_cast<uint32_t>(remainingOverflow));
            collector->CollectBatch<TOutput>(overflowOutputRecordBufIterator_.base(), numEventsToEmitThisBatch);
            overflowOutputRecordBufIterator_ += numEventsToEmitThisBatch;
            if (!SetNextOutputBlockIfNecessary(collector)) {
                break;
            }
        }
        assert(overflowOutputRecordBufIterator_ <= overflowOutputRecordBuf_.end());
        if (overflowOutputRecordBufIterator_ == overflowOutputRecordBuf_.end()) {
            overflowOutputRecordBuf_.clear();
            this->hasPendingOverflow_ = false;
        }
    }

    template<typename TInput, typename TOutput, bool IsKeyedT, enjima::api::AggFuncT<TInput, TOutput, IsKeyedT> TFunc>
    bool FixedEventTimeWindowOperator<TInput, TOutput, IsKeyedT, TFunc>::SetNextOutputBlockIfNecessary(
            core::OutputCollector* collector)
    {
        if (!this->outputBlockPtr_->CanWrite()) {
            this->outputBlockPtr_ = enjima::operators::GetNextOutputBlock<TOutput>(this->downstreamChannelId_,
                    this->outputBlockPtr_, this->pMemoryManager_);
            collector->SetOutputBlock(this->outputBlockPtr_);
        }
        return this->outputBlockPtr_->CanWrite();
    }

    template<typename TInput, typename TOutput, bool IsKeyedT, enjima::api::AggFuncT<TInput, TOutput, IsKeyedT> TFunc>
    uint8_t FixedEventTimeWindowOperator<TInput, TOutput, IsKeyedT, TFunc>::ProcessQueue()
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
                auto timestamp = inputRec.GetTimestamp();
                auto inputEvent = inputRec.GetData();
                if (nextWindowTriggerAt_ == 0) {
                    nextWindowTriggerAt_ = timestamp + windowSizeMillis_;
                }
                if (timestamp > maxWindowTimestamp_) {
                    maxWindowTimestamp_ = timestamp;
                }
                if (maxWindowTimestamp_ >= nextWindowTriggerAt_) {
                    auto resultMap = aggFunc_.GetResult();
                    auto outTimestamp = nextWindowTriggerAt_ - 1;
                    for (const auto& emittedEvent: resultMap) {
                        core::Record<TOutput> record(outTimestamp, emittedEvent);
                        this->outputQueue_->push(record);
                        this->outCounter_->Inc();
                    }
                    nextWindowTriggerAt_ += windowSizeMillis_;
                }
                aggFunc_(inputEvent);
            }
            else {
                core::Record<TOutput> rec(core::Record<TOutput>::RecordType::kLatency, inputRec.GetTimestamp());
                this->outputQueue_->push(rec);
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
