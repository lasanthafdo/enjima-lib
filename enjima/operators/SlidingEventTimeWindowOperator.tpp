//
// Created by m34ferna on 01/03/24.
//

namespace enjima::operators {
    template<typename TInput, typename TOutput, enjima::api::MergeableKeyedAggFuncT<TInput, TOutput> TAggFunc>
    SlidingEventTimeWindowOperator<TInput, TOutput, TAggFunc>::SlidingEventTimeWindowOperator(OperatorID opId,
            const std::string& opName, TAggFunc aggFunc, std::chrono::milliseconds windowSizeMillis,
            std::chrono::milliseconds windowSlideMillis)
        : SingleInputOperator<TInput, TOutput>(opId, opName), windowSizeMillis_(windowSizeMillis.count()),
          windowSlideMillis_(windowSlideMillis.count()), numPanes_(windowSizeMillis_ / windowSlideMillis_),
          windowAggFunc_(aggFunc)
    {
        if (windowSizeMillis_ == 0 || windowSlideMillis_ == 0) {
            throw runtime::IllegalArgumentException{"Window size and window slide size must be a positive integer!"};
        }
        if (windowSizeMillis_ % windowSlideMillis_ != 0) {
            throw runtime::IllegalArgumentException{"Window size must be an integer multiple of the window slide!"};
        }
        paneAggFuncVec_.emplace_back(TAggFunc{});
    }

    template<typename TInput, typename TOutput, enjima::api::MergeableKeyedAggFuncT<TInput, TOutput> TAggFunc>
    void SlidingEventTimeWindowOperator<TInput, TOutput, TAggFunc>::ProcessEvent(uint64_t timestamp, TInput inputEvent,
            enjima::core::OutputCollector* collector)
    {
        if (nextWindowPaneTriggerAt_ == 0) {
            nextWindowPaneTriggerAt_ = timestamp + windowSlideMillis_;
        }
        if (timestamp > maxWindowTimestamp_) {
            maxWindowTimestamp_ = timestamp;
        }
        if (maxWindowTimestamp_ >= nextWindowPaneTriggerAt_) {
            assert(paneAggFuncVec_.size() <= numPanes_);
            if (paneAggFuncVec_.size() == numPanes_) {
                windowAggFunc_.Merge(paneAggFuncVec_);
                auto resultVec = windowAggFunc_.GetResult();
                for (const auto& emittedEvent: resultVec) {
                    collector->CollectWithTimestamp<TOutput>(nextWindowPaneTriggerAt_ - 1, emittedEvent);
                }
                windowAggFunc_.Reset();
            }
            currentWindowPaneIdx_ = (currentWindowPaneIdx_ + 1) % numPanes_;
            if (paneAggFuncVec_.size() == currentWindowPaneIdx_) {
                paneAggFuncVec_.emplace_back(TAggFunc{});
            }
            paneAggFuncVec_[currentWindowPaneIdx_].Reset();
            nextWindowPaneTriggerAt_ += windowSlideMillis_;
        }
        paneAggFuncVec_[currentWindowPaneIdx_](inputEvent);
    }

    template<typename TInput, typename TOutput, enjima::api::MergeableKeyedAggFuncT<TInput, TOutput> TAggFunc>
    void SlidingEventTimeWindowOperator<TInput, TOutput, TAggFunc>::ProcessBatch(void* inputBuffer,
            uint32_t numRecordsToRead, void* outputBuffer, core::OutputCollector* collector)
    {
        if (nextWindowPaneTriggerAt_ == 0) {
            // numRecordsToRead shouldn't be 0, ever!!
            auto firstInputRecord = static_cast<core::Record<TInput>*>(inputBuffer);
            uint32_t tmp = 1;
            while (firstInputRecord->GetRecordType() == core::Record<TInput>::RecordType::kLatency &&
                    tmp < numRecordsToRead) {
                firstInputRecord = static_cast<core::Record<TInput>*>(inputBuffer) + tmp;
                tmp++;
            }
            if (firstInputRecord->GetRecordType() == core::Record<TInput>::RecordType::kData) {
                nextWindowPaneTriggerAt_ = firstInputRecord->GetTimestamp() + windowSlideMillis_;
            }
            paneAggFuncVec_.emplace_back(TAggFunc{});
            windowAggFunc_.InitializeActivePane(paneAggFuncVec_[currentWindowPaneIdx_]);
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
                if (maxWindowTimestamp_ >= nextWindowPaneTriggerAt_) {
                    assert(paneAggFuncVec_.size() <= numPanes_);
                    if (paneAggFuncVec_.size() == numPanes_) {
                        windowAggFunc_.Merge(paneAggFuncVec_);
                        auto resultVec = windowAggFunc_.GetResult();
                        auto outTimestamp = nextWindowPaneTriggerAt_ - 1;
                        for (const auto& emittedEvent: resultVec) {
                            if (outputOverflow || numEmitted >= maxWritableEvents) {
                                overflowRecordsBufferVec_.emplace_back(outTimestamp, emittedEvent);
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
                        windowAggFunc_.Reset();
                    }
                    currentWindowPaneIdx_ = (currentWindowPaneIdx_ + 1) % numPanes_;
                    if (paneAggFuncVec_.size() == currentWindowPaneIdx_) {
                        paneAggFuncVec_.emplace_back(TAggFunc{});
                    }
                    paneAggFuncVec_[currentWindowPaneIdx_].Reset();
                    windowAggFunc_.InitializeActivePane(paneAggFuncVec_[currentWindowPaneIdx_]);
                    nextWindowPaneTriggerAt_ += windowSlideMillis_;
                }
                paneAggFuncVec_[currentWindowPaneIdx_](inputRecord->GetData());
            }
            else {
                if (outputOverflow || numEmitted >= maxWritableEvents) {
#if ENJIMA_METRICS_LEVEL >= 3
                    auto metricsVecPtr = inputRecord->GetAdditionalMetricsPtr();
                    metricsVecPtr->emplace_back(runtime::GetSystemTimeMicros());
                    overflowRecordsBufferVec_.emplace_back(core::Record<TOutput>::RecordType::kLatency,
                            inputRecord->GetTimestamp(), metricsVecPtr);
#else
                    overflowRecordsBufferVec_.emplace_back(core::Record<TOutput>::RecordType::kLatency,
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
            assert(!this->outputBlockPtr_->CanWrite());
            auto numOverflowEmitted = 0u;
            auto overflowSize = overflowRecordsBufferVec_.size();
            auto beginPtr = overflowRecordsBufferVec_.begin();
            while (numOverflowEmitted < overflowSize) {
                while (!this->outputBlockPtr_->CanWrite()) {
                    this->outputBlockPtr_ = GetNextOutputBlock<TOutput>(this->downstreamChannelId_,
                            this->outputBlockPtr_, this->pMemoryManager_);
                }
                collector->SetOutputBlock(this->outputBlockPtr_);
                auto numEventsToEmitThisBatch =
                        std::min(this->outputBlockPtr_->template GetNumWritableEvents<TOutput>(),
                                static_cast<uint32_t>(overflowSize - numOverflowEmitted));
                collector->CollectBatch<TOutput>(beginPtr.base(), numEventsToEmitThisBatch);
                numOverflowEmitted += numEventsToEmitThisBatch;
                beginPtr += numEventsToEmitThisBatch;
            }
            overflowRecordsBufferVec_.clear();
        }
    }

    template<typename TInput, typename TOutput, enjima::api::MergeableKeyedAggFuncT<TInput, TOutput> TAggFunc>
    uint8_t SlidingEventTimeWindowOperator<TInput, TOutput, TAggFunc>::ProcessQueue()
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
                if (nextWindowPaneTriggerAt_ == 0) {
                    nextWindowPaneTriggerAt_ = timestamp + windowSizeMillis_;
                }
                if (timestamp > maxWindowTimestamp_) {
                    maxWindowTimestamp_ = timestamp;
                }
                if (maxWindowTimestamp_ >= nextWindowPaneTriggerAt_) {
                    auto resultMap = windowAggFunc_.GetResult();
                    auto outTimestamp = nextWindowPaneTriggerAt_ - 1;
                    for (const auto& emittedEvent: resultMap) {
                        core::Record<TOutput> record(outTimestamp, emittedEvent);
                        this->outputQueue_->push(record);
                        this->outCounter_->Inc();
                    }
                    nextWindowPaneTriggerAt_ += windowSizeMillis_;
                }
                windowAggFunc_(inputEvent);
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
