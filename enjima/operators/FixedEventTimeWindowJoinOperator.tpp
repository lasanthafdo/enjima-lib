//
// Created by m34ferna on 01/03/24.
//

namespace enjima::operators {

    template<typename TLeft, typename TRight, typename TKey, enjima::api::KeyExtractFuncT<TLeft, TKey> TLeftKeyFunc,
            enjima::api::KeyExtractFuncT<TRight, TKey> TRightKeyFunc, typename TOutput,
            enjima::api::JoinFuncT<TLeft, TRight, TOutput> TFunc>
    FixedEventTimeWindowJoinOperator<TLeft, TRight, TKey, TLeftKeyFunc, TRightKeyFunc, TOutput,
            TFunc>::FixedEventTimeWindowJoinOperator(OperatorID opId, const std::string& opName,
            TLeftKeyFunc leftKeyFunc, TRightKeyFunc rightKeyFunc, TFunc joinFunc,
            std::chrono::milliseconds windowSizeSecs)
        : DoubleInputOperator<TLeft, TRight, TOutput>(opId, opName), leftKeyFunc_(leftKeyFunc),
          rightKeyFunc_(rightKeyFunc), joinFunc_(joinFunc), windowSize_(windowSizeSecs.count())
    {
    }

    template<typename TLeft, typename TRight, typename TKey, enjima::api::KeyExtractFuncT<TLeft, TKey> TLeftKeyFunc,
            enjima::api::KeyExtractFuncT<TRight, TKey> TRightKeyFunc, typename TOutput,
            enjima::api::JoinFuncT<TLeft, TRight, TOutput> TFunc>
    void FixedEventTimeWindowJoinOperator<TLeft, TRight, TKey, TLeftKeyFunc, TRightKeyFunc, TOutput,
            TFunc>::ProcessLeftEvent(core::Record<TLeft>* pLeftInputRecord, enjima::core::OutputCollector* collector)
    {
        assert(overflowOutputRecordBuf_.empty());
        auto timestamp = pLeftInputRecord->GetTimestamp();
        if (nextWindowAt_ == 0) {
            nextWindowAt_ = timestamp + windowSize_;
        }
        ProcessLeftEventIfNecessary(pLeftInputRecord);
        TriggerWindowIfNecessary(collector);
    }

    template<typename TLeft, typename TRight, typename TKey, enjima::api::KeyExtractFuncT<TLeft, TKey> TLeftKeyFunc,
            enjima::api::KeyExtractFuncT<TRight, TKey> TRightKeyFunc, typename TOutput,
            enjima::api::JoinFuncT<TLeft, TRight, TOutput> TFunc>
    void FixedEventTimeWindowJoinOperator<TLeft, TRight, TKey, TLeftKeyFunc, TRightKeyFunc, TOutput,
            TFunc>::ProcessLeftEventIfNecessary(core::Record<TLeft>* pLeftInputRecord)
    {
        auto timestamp = pLeftInputRecord->GetTimestamp();
        if (timestamp <= nextWindowAt_) {
            auto leftInputEvent = pLeftInputRecord->GetData();
            auto bufferPos = leftRecordBuf_.size();
            auto leftKey = leftKeyFunc_(leftInputEvent);
            leftRecordBuf_.emplace_back(*pLeftInputRecord);
            leftHash_[leftKey].emplace_back(bufferPos);
            if (rightHash_.contains(leftKey)) {
                auto matchedVecPos = rightHash_[leftKey];
                for (auto rightBufferPos: matchedVecPos) {
                    outputEventQBuf_.emplace(joinFunc_(leftInputEvent, rightRecordBuf_[rightBufferPos].GetData()));
                }
            }
        }
        else {
            nextWnLeftRecordBuf_.push(*pLeftInputRecord);
        }
        if (timestamp > maxLeftTimestamp_) {
            maxLeftTimestamp_ = timestamp;
        }
    }

    template<typename TLeft, typename TRight, typename TKey, enjima::api::KeyExtractFuncT<TLeft, TKey> TLeftKeyFunc,
            enjima::api::KeyExtractFuncT<TRight, TKey> TRightKeyFunc, typename TOutput,
            enjima::api::JoinFuncT<TLeft, TRight, TOutput> TFunc>
    void FixedEventTimeWindowJoinOperator<TLeft, TRight, TKey, TLeftKeyFunc, TRightKeyFunc, TOutput,
            TFunc>::ProcessRightEvent(core::Record<TRight>* pRightInputRecord, enjima::core::OutputCollector* collector)
    {
        assert(overflowOutputRecordBuf_.empty());
        auto timestamp = pRightInputRecord->GetTimestamp();
        if (nextWindowAt_ == 0) {
            nextWindowAt_ = timestamp + windowSize_;
        }
        ProcessRightEventIfNecessary(pRightInputRecord);
        TriggerWindowIfNecessary(collector);
    }

    template<typename TLeft, typename TRight, typename TKey, enjima::api::KeyExtractFuncT<TLeft, TKey> TLeftKeyFunc,
            enjima::api::KeyExtractFuncT<TRight, TKey> TRightKeyFunc, typename TOutput,
            enjima::api::JoinFuncT<TLeft, TRight, TOutput> TFunc>
    void FixedEventTimeWindowJoinOperator<TLeft, TRight, TKey, TLeftKeyFunc, TRightKeyFunc, TOutput,
            TFunc>::ProcessRightEventIfNecessary(core::Record<TRight>* pRightInputRecord)
    {
        auto timestamp = pRightInputRecord->GetTimestamp();
        if (timestamp <= nextWindowAt_) {
            auto rightInputEvent = pRightInputRecord->GetData();
            auto bufferPos = rightRecordBuf_.size();
            auto rightKey = rightKeyFunc_(rightInputEvent);
            rightRecordBuf_.emplace_back(*pRightInputRecord);
            rightHash_[rightKey].emplace_back(bufferPos);
            if (leftHash_.contains(rightKey)) {
                auto matchedVecPos = leftHash_[rightKey];
                for (auto leftBufferPos: matchedVecPos) {
                    outputEventQBuf_.emplace(joinFunc_(leftRecordBuf_[leftBufferPos].GetData(), rightInputEvent));
                }
            }
        }
        else {
            nextWnRightRecordBuf_.push(*pRightInputRecord);
        }
        if (timestamp > maxRightTimestamp_) {
            maxRightTimestamp_ = timestamp;
        }
    }

    template<typename TLeft, typename TRight, typename TKey, enjima::api::KeyExtractFuncT<TLeft, TKey> TLeftKeyFunc,
            enjima::api::KeyExtractFuncT<TRight, TKey> TRightKeyFunc, typename TOutput,
            enjima::api::JoinFuncT<TLeft, TRight, TOutput> TFunc>
    void FixedEventTimeWindowJoinOperator<TLeft, TRight, TKey, TLeftKeyFunc, TRightKeyFunc, TOutput,
            TFunc>::TriggerWindowIfNecessary(core::OutputCollector* collector)
    {
        auto maxCompletedTimestamp = std::min(maxLeftTimestamp_, maxRightTimestamp_);
        if (maxCompletedTimestamp >= nextWindowAt_ && nextWindowAt_ > 0) {
            leftRecordBuf_.clear();
            leftRecordBuf_.shrink_to_fit();
            leftHash_.clear();
            // leftHash_.shrink_to_fit();
            rightRecordBuf_.clear();
            rightRecordBuf_.shrink_to_fit();
            rightHash_.clear();
            // rightHash_.shrink_to_fit();

            while (!outputEventQBuf_.empty()) {
                size_t maxWritable = this->outputBlockPtr_->template GetNumWritableEvents<TOutput>();
                auto numWritable = std::min(maxWritable, outputEventQBuf_.size());
                for (auto i = numWritable; i > 0; i--) {
                    auto emittedEvent = outputEventQBuf_.front();
                    collector->CollectWithTimestamp<TOutput>(nextWindowAt_ - 1, emittedEvent);
                    outputEventQBuf_.pop();
                }
                if (!this->SetNextOutputBlockIfNecessary()) {
                    assert(overflowOutputRecordBuf_.empty());
                    if (!outputEventQBuf_.empty()) {
                        while (!outputEventQBuf_.empty()) {
                            auto emittedEvent = outputEventQBuf_.front();
                            overflowOutputRecordBuf_.emplace_back(nextWindowAt_ - 1, emittedEvent);
                            outputEventQBuf_.pop();
                        }
                        overflowOutputRecordBufIterator_ = overflowOutputRecordBuf_.begin();
                        this->hasPendingOverflow_ = true;
                    }
                    break;
                }
            }
            nextWindowAt_ += windowSize_;
            // Clean up the buffers, hashes and queues properly
            assert(outputEventQBuf_.empty());
            auto leftQSize = nextWnLeftRecordBuf_.size();
            for (auto l = 0ul; l < leftQSize; l++) {
                auto nextLeftRecord = nextWnLeftRecordBuf_.front();
                ProcessLeftEventIfNecessary(&nextLeftRecord);
                nextWnLeftRecordBuf_.pop();
            }
            auto rightQSize = nextWnRightRecordBuf_.size();
            for (auto r = 0ul; r < rightQSize; r++) {
                auto nextRightRecord = nextWnRightRecordBuf_.front();
                ProcessRightEventIfNecessary(&nextRightRecord);
                nextWnRightRecordBuf_.pop();
            }
        }
    }

    template<typename TLeft, typename TRight, typename TKey, enjima::api::KeyExtractFuncT<TLeft, TKey> TLeftKeyFunc,
            enjima::api::KeyExtractFuncT<TRight, TKey> TRightKeyFunc, typename TOutput,
            enjima::api::JoinFuncT<TLeft, TRight, TOutput> TFunc>
    void FixedEventTimeWindowJoinOperator<TLeft, TRight, TKey, TLeftKeyFunc, TRightKeyFunc, TOutput,
            TFunc>::ProcessPendingOverflow(core::OutputCollector* collector)
    {
        assert((overflowOutputRecordBuf_.end() - overflowOutputRecordBufIterator_) <
                std::numeric_limits<uint32_t>::max());
        while (overflowOutputRecordBufIterator_ != overflowOutputRecordBuf_.end()) {
            collector->Collect(*overflowOutputRecordBufIterator_);
            overflowOutputRecordBufIterator_++;
            if (!this->SetNextOutputBlockIfNecessary()) {
                break;
            }
        }
        assert(overflowOutputRecordBufIterator_ <= overflowOutputRecordBuf_.end());
        if (overflowOutputRecordBufIterator_ == overflowOutputRecordBuf_.end()) {
            overflowOutputRecordBuf_.clear();
            this->hasPendingOverflow_ = false;
        }
    }

    template<typename TLeft, typename TRight, typename TKey, enjima::api::KeyExtractFuncT<TLeft, TKey> TLeftKeyFunc,
            enjima::api::KeyExtractFuncT<TRight, TKey> TRightKeyFunc, typename TOutput,
            enjima::api::JoinFuncT<TLeft, TRight, TOutput> TFunc>
    void
    FixedEventTimeWindowJoinOperator<TLeft, TRight, TKey, TLeftKeyFunc, TRightKeyFunc, TOutput, TFunc>::ProcessBatch(
            void* leftInputBuffer, uint32_t numRecordsToReadLeft, void* rightInputBuffer,
            uint32_t numRecordsToReadRight, core::OutputCollector* collector)
    {
        assert(overflowOutputRecordBuf_.empty());
        if (nextWindowAt_ == 0) {
            if (numRecordsToReadLeft > 0) {
                auto firstLeftInputRecord = static_cast<core::Record<TLeft>*>(leftInputBuffer);
                uint32_t tmp = 1;
                while (firstLeftInputRecord->GetRecordType() == core::Record<TLeft>::RecordType::kLatency &&
                        tmp < numRecordsToReadLeft) {
                    firstLeftInputRecord = static_cast<core::Record<TLeft>*>(leftInputBuffer) + tmp;
                    tmp++;
                }
                if (firstLeftInputRecord->GetRecordType() == core::Record<TLeft>::RecordType::kData) {
                    nextWindowAt_ = firstLeftInputRecord->GetTimestamp() + windowSize_;
                }
            }
            if (nextWindowAt_ == 0 && numRecordsToReadRight > 0) {
                auto firstRightInputRecord = static_cast<core::Record<TRight>*>(rightInputBuffer);
                uint32_t tmp = 1;
                while (firstRightInputRecord->GetRecordType() == core::Record<TRight>::RecordType::kLatency &&
                        tmp < numRecordsToReadRight) {
                    firstRightInputRecord = static_cast<core::Record<TRight>*>(rightInputBuffer) + tmp;
                    tmp++;
                }
                if (firstRightInputRecord->GetRecordType() == core::Record<TRight>::RecordType::kData) {
                    nextWindowAt_ = firstRightInputRecord->GetTimestamp() + windowSize_;
                }
            }
        }

        nextOutRecordPtr_ = static_cast<enjima::core::Record<TOutput>*>(outputRecordBuffer_);
        for (uint32_t i = 0; i < numRecordsToReadLeft; i++) {
            auto pLeftInputRecord = static_cast<core::Record<TLeft>*>(leftInputBuffer) + i;
            if (pLeftInputRecord->GetRecordType() != core::Record<TLeft>::RecordType::kLatency) {
                ProcessLeftEventIfNecessary(pLeftInputRecord);
            }
            else {
                ProcessLatencyRecord(collector, pLeftInputRecord->GetTimestamp());
            }
        }
        for (uint32_t i = 0; i < numRecordsToReadRight; i++) {
            auto pRightInputRecord = static_cast<core::Record<TRight>*>(rightInputBuffer) + i;
            if (pRightInputRecord->GetRecordType() != core::Record<TRight>::RecordType::kLatency) {
                ProcessRightEventIfNecessary(pRightInputRecord);
            }
            else {
                ProcessLatencyRecord(collector, pRightInputRecord->GetTimestamp());
            }
        }
        TriggerWindowBatchIfNecessary(collector);
        FlushAndResetOutputBuffer(collector);
    }

    template<typename TLeft, typename TRight, typename TKey, enjima::api::KeyExtractFuncT<TLeft, TKey> TLeftKeyFunc,
            enjima::api::KeyExtractFuncT<TRight, TKey> TRightKeyFunc, typename TOutput,
            enjima::api::JoinFuncT<TLeft, TRight, TOutput> TFunc>
    void FixedEventTimeWindowJoinOperator<TLeft, TRight, TKey, TLeftKeyFunc, TRightKeyFunc, TOutput,
            TFunc>::ProcessLatencyRecord(core::OutputCollector* collector, uint64_t timestamp)
    {
        assert(this->outputBlockPtr_->template GetNumWritableEvents<TOutput>() >= numBuffered_);
        size_t numWritable = this->outputBlockPtr_->template GetNumWritableEvents<TOutput>() - numBuffered_;
        if (numWritable == 0) {
            FlushAndResetOutputBuffer(collector);
            this->SetNextOutputBlockIfNecessary();
        }
        if (this->outputBlockPtr_->CanWrite()) {
#if ENJIMA_METRICS_LEVEL >= 3
            auto metricsVecPtr = inputRecord->GetAdditionalMetricsPtr();
            metricsVecPtr->emplace_back(runtime::GetSystemTimeMicros());
            new (nextOutRecordPtr_)
                    core::Record<TOutput>(core::Record<TOutput>::RecordType::kLatency, timesamp, metricsVecPtr);
#else
            new (nextOutRecordPtr_)
                    enjima::core::Record<TOutput>(enjima::core::Record<TOutput>::RecordType::kLatency, timestamp);
#endif
            nextOutRecordPtr_ += 1;
            numBuffered_++;
        }
        else {
            overflowOutputRecordBuf_.emplace_back(enjima::core::Record<TOutput>::RecordType::kLatency, timestamp);
        }
    }

    template<typename TLeft, typename TRight, typename TKey, enjima::api::KeyExtractFuncT<TLeft, TKey> TLeftKeyFunc,
            enjima::api::KeyExtractFuncT<TRight, TKey> TRightKeyFunc, typename TOutput,
            enjima::api::JoinFuncT<TLeft, TRight, TOutput> TFunc>
    void FixedEventTimeWindowJoinOperator<TLeft, TRight, TKey, TLeftKeyFunc, TRightKeyFunc, TOutput,
            TFunc>::ProcessPendingOverflowBatch(core::OutputCollector* collector)
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
            if (!this->SetNextOutputBlockIfNecessary()) {
                break;
            }
        }
        assert(overflowOutputRecordBufIterator_ <= overflowOutputRecordBuf_.end());
        if (overflowOutputRecordBufIterator_ == overflowOutputRecordBuf_.end()) {
            overflowOutputRecordBuf_.clear();
            this->hasPendingOverflow_ = false;
        }
    }

    template<typename TLeft, typename TRight, typename TKey, enjima::api::KeyExtractFuncT<TLeft, TKey> TLeftKeyFunc,
            enjima::api::KeyExtractFuncT<TRight, TKey> TRightKeyFunc, typename TOutput,
            enjima::api::JoinFuncT<TLeft, TRight, TOutput> TFunc>
    void FixedEventTimeWindowJoinOperator<TLeft, TRight, TKey, TLeftKeyFunc, TRightKeyFunc, TOutput,
            TFunc>::TriggerWindowBatchIfNecessary(core::OutputCollector* collector)
    {
        auto maxCompletedTimestamp = std::min(maxLeftTimestamp_, maxRightTimestamp_);
        if (maxCompletedTimestamp >= nextWindowAt_ && nextWindowAt_ > 0) {
            leftRecordBuf_.clear();
            leftRecordBuf_.shrink_to_fit();
            leftHash_.clear();
            // leftHash_.shrink_to_fit();
            rightRecordBuf_.clear();
            rightRecordBuf_.shrink_to_fit();
            rightHash_.clear();
            // rightHash_.shrink_to_fit();

            while (!outputEventQBuf_.empty()) {
                size_t maxWritable = this->outputBlockPtr_->template GetNumWritableEvents<TOutput>() - numBuffered_;
                if (maxWritable > 0) {
                    auto numWritable = std::min(maxWritable, outputEventQBuf_.size());
                    for (auto i = numWritable; i > 0; i--) {
                        auto emittedEvent = outputEventQBuf_.front();
                        new (nextOutRecordPtr_) core::Record<TOutput>(nextWindowAt_ - 1, emittedEvent);
                        nextOutRecordPtr_ += 1;
                        numBuffered_++;
                        outputEventQBuf_.pop();
                    }
                }
                FlushAndResetOutputBuffer(collector);
                if (!this->SetNextOutputBlockIfNecessary()) {
                    assert(overflowOutputRecordBuf_.empty());
                    if (!outputEventQBuf_.empty()) {
                        while (!outputEventQBuf_.empty()) {
                            auto emittedEvent = outputEventQBuf_.front();
                            overflowOutputRecordBuf_.emplace_back(nextWindowAt_ - 1, emittedEvent);
                            outputEventQBuf_.pop();
                        }
                        overflowOutputRecordBufIterator_ = overflowOutputRecordBuf_.begin();
                        this->hasPendingOverflow_ = true;
                    }
                    break;
                }
            }
            nextWindowAt_ += windowSize_;
            // Clean up the buffers, hashes and queues properly
            assert(outputEventQBuf_.empty());
            auto leftQSize = nextWnLeftRecordBuf_.size();
            for (auto l = 0ul; l < leftQSize; l++) {
                auto nextLeftRecord = nextWnLeftRecordBuf_.front();
                ProcessLeftEventIfNecessary(&nextLeftRecord);
                nextWnLeftRecordBuf_.pop();
            }
            auto rightQSize = nextWnRightRecordBuf_.size();
            for (auto r = 0ul; r < rightQSize; r++) {
                auto nextRightRecord = nextWnRightRecordBuf_.front();
                ProcessRightEventIfNecessary(&nextRightRecord);
                nextWnRightRecordBuf_.pop();
            }
        }
    }

    template<typename TLeft, typename TRight, typename TKey, enjima::api::KeyExtractFuncT<TLeft, TKey> TLeftKeyFunc,
            enjima::api::KeyExtractFuncT<TRight, TKey> TRightKeyFunc, typename TOutput,
            enjima::api::JoinFuncT<TLeft, TRight, TOutput> TFunc>
    void FixedEventTimeWindowJoinOperator<TLeft, TRight, TKey, TLeftKeyFunc, TRightKeyFunc, TOutput,
            TFunc>::FlushAndResetOutputBuffer(core::OutputCollector* collector)
    {
        assert(numBuffered_ <= this->outputBlockPtr_->template GetNumWritableEvents<TOutput>());
        collector->CollectBatch<TOutput>(outputRecordBuffer_, numBuffered_);
        numBuffered_ = 0;
        nextOutRecordPtr_ = static_cast<enjima::core::Record<TOutput>*>(outputRecordBuffer_);
    }

    template<typename TLeft, typename TRight, typename TKey, enjima::api::KeyExtractFuncT<TLeft, TKey> TLeftKeyFunc,
            enjima::api::KeyExtractFuncT<TRight, TKey> TRightKeyFunc, typename TOutput,
            enjima::api::JoinFuncT<TLeft, TRight, TOutput> TFunc>
    void FixedEventTimeWindowJoinOperator<TLeft, TRight, TKey, TLeftKeyFunc, TRightKeyFunc, TOutput, TFunc>::Initialize(
            runtime::ExecutionEngine* executionEngine, memory::MemoryManager* memoryManager,
            metrics::Profiler* profiler)
    {
        DoubleInputOperator<TLeft, TRight, TOutput>::Initialize(executionEngine, memoryManager, profiler);
        outputRecordBuffer_ = aligned_alloc(ENJIMA_BUFFER_ALIGNMENT, this->outputBlockPtr_->GetTotalCapacity());
    }

    template<typename TLeft, typename TRight, typename TKey, enjima::api::KeyExtractFuncT<TLeft, TKey> TLeftKeyFunc,
            enjima::api::KeyExtractFuncT<TRight, TKey> TRightKeyFunc, typename TOutput,
            enjima::api::JoinFuncT<TLeft, TRight, TOutput> TFunc>
    uint8_t
    FixedEventTimeWindowJoinOperator<TLeft, TRight, TKey, TLeftKeyFunc, TRightKeyFunc, TOutput, TFunc>::ProcessQueue()
    {
        auto operatorStatus = StreamingOperator::kQueueInitStatus;
        if (this->hasPendingOverflow_) {
            ProcessPendingOverflowQB();
            if (this->hasPendingOverflow_) {
                operatorStatus &= ~StreamingOperator::kCanOutput;
                return operatorStatus;
            }
        }
        assert(overflowOutputRecordBuf_.empty());
        size_t numProcessed = 0;
        while ((!this->leftInputQueue_->empty() || !this->rightInputQueue_->empty()) && !this->outputQueue_->full() &&
                numProcessed <= this->pMemoryManager_->GetDefaultNumEventsPerBlock()) {
            if (this->leftInputQueue_->size() > 0) {
                core::Record<TLeft> leftInputRecord;
                this->leftInputQueue_->pop(leftInputRecord);
                this->leftInCounter_->IncRelaxed();
                this->inCounter_->IncRelaxed();
                numProcessed++;
                if (leftInputRecord.GetRecordType() != core::Record<TLeft>::RecordType::kLatency) {
                    auto timestamp = leftInputRecord.GetTimestamp();
                    if (nextWindowAt_ == 0) {
                        nextWindowAt_ = timestamp + windowSize_;
                    }
                    ProcessLeftEventIfNecessary(&leftInputRecord);
                    TriggerWindowIfNecessaryQB();
                }
                else {
                    auto leftLatencyRecord = core::Record<TOutput>(core::Record<TOutput>::RecordType::kLatency,
                            leftInputRecord.GetTimestamp());
                    this->outputQueue_->push(leftLatencyRecord);
                    this->outCounter_->IncRelaxed();
                }
            }
            if (this->rightInputQueue_->size() > 0) {
                core::Record<TRight> rightInputRecord;
                this->rightInputQueue_->pop(rightInputRecord);
                this->rightInCounter_->IncRelaxed();
                this->inCounter_->IncRelaxed();
                numProcessed++;
                if (rightInputRecord.GetRecordType() != core::Record<TRight>::RecordType::kLatency) {
                    auto timestamp = rightInputRecord.GetTimestamp();
                    if (nextWindowAt_ == 0) {
                        nextWindowAt_ = timestamp + windowSize_;
                    }
                    ProcessRightEventIfNecessary(&rightInputRecord);
                    TriggerWindowIfNecessaryQB();
                }
                else {
                    auto rightLatencyRecord = core::Record<TOutput>(core::Record<TOutput>::RecordType::kLatency,
                            rightInputRecord.GetTimestamp());
                    this->outputQueue_->push(rightLatencyRecord);
                    this->outCounter_->IncRelaxed();
                }
            }
        }
        if (!this->leftInputQueue_->empty() || !this->rightInputQueue_->empty()) {
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

    template<typename TLeft, typename TRight, typename TKey, enjima::api::KeyExtractFuncT<TLeft, TKey> TLeftKeyFunc,
            enjima::api::KeyExtractFuncT<TRight, TKey> TRightKeyFunc, typename TOutput,
            enjima::api::JoinFuncT<TLeft, TRight, TOutput> TFunc>
    void FixedEventTimeWindowJoinOperator<TLeft, TRight, TKey, TLeftKeyFunc, TRightKeyFunc, TOutput,
            TFunc>::TriggerWindowIfNecessaryQB()
    {
        auto maxCompletedTimestamp = std::min(maxLeftTimestamp_, maxRightTimestamp_);
        if (maxCompletedTimestamp >= nextWindowAt_ && nextWindowAt_ > 0) {
            leftRecordBuf_.clear();
            leftRecordBuf_.shrink_to_fit();
            leftHash_.clear();
            // leftHash_.shrink_to_fit();
            rightRecordBuf_.clear();
            rightRecordBuf_.shrink_to_fit();
            rightHash_.clear();
            // rightHash_.shrink_to_fit();

            while (!outputEventQBuf_.empty()) {
                auto emittedEvent = outputEventQBuf_.front();
                auto outputRecord = core::Record<TOutput>(nextWindowAt_ - 1, emittedEvent);
                if (this->outputQueue_->try_push(outputRecord)) {
                    this->outCounter_->IncRelaxed();
                    outputEventQBuf_.pop();
                }
                else {
                    break;
                }
            }
            assert(overflowOutputRecordBuf_.empty());
            if (!outputEventQBuf_.empty()) {
                while (!outputEventQBuf_.empty()) {
                    auto emittedEvent = outputEventQBuf_.front();
                    overflowOutputRecordBuf_.emplace_back(nextWindowAt_ - 1, emittedEvent);
                    outputEventQBuf_.pop();
                }
                overflowOutputRecordBufIterator_ = overflowOutputRecordBuf_.begin();
                this->hasPendingOverflow_ = true;
            }
            nextWindowAt_ += windowSize_;
            // Clean up the buffers, hashes and queues properly
            assert(outputEventQBuf_.empty());
            auto leftQSize = nextWnLeftRecordBuf_.size();
            for (auto l = 0ul; l < leftQSize; l++) {
                auto nextLeftRecord = nextWnLeftRecordBuf_.front();
                ProcessLeftEventIfNecessary(&nextLeftRecord);
                nextWnLeftRecordBuf_.pop();
            }
            auto rightQSize = nextWnRightRecordBuf_.size();
            for (auto r = 0ul; r < rightQSize; r++) {
                auto nextRightRecord = nextWnRightRecordBuf_.front();
                ProcessRightEventIfNecessary(&nextRightRecord);
                nextWnRightRecordBuf_.pop();
            }
        }
    }

    template<typename TLeft, typename TRight, typename TKey, enjima::api::KeyExtractFuncT<TLeft, TKey> TLeftKeyFunc,
            enjima::api::KeyExtractFuncT<TRight, TKey> TRightKeyFunc, typename TOutput,
            enjima::api::JoinFuncT<TLeft, TRight, TOutput> TFunc>
    void FixedEventTimeWindowJoinOperator<TLeft, TRight, TKey, TLeftKeyFunc, TRightKeyFunc, TOutput,
            TFunc>::ProcessPendingOverflowQB()
    {
        assert((overflowOutputRecordBuf_.end() - overflowOutputRecordBufIterator_) <
                std::numeric_limits<uint32_t>::max());
        while (overflowOutputRecordBufIterator_ != overflowOutputRecordBuf_.end()) {
            if (this->outputQueue_->try_push(*overflowOutputRecordBufIterator_)) {
                this->outCounter_->IncRelaxed();
                overflowOutputRecordBufIterator_++;
            }
            else {
                break;
            }
        }
        assert(overflowOutputRecordBufIterator_ <= overflowOutputRecordBuf_.end());
        if (overflowOutputRecordBufIterator_ == overflowOutputRecordBuf_.end()) {
            overflowOutputRecordBuf_.clear();
            this->hasPendingOverflow_ = false;
        }
    }

    template<typename TLeft, typename TRight, typename TKey, enjima::api::KeyExtractFuncT<TLeft, TKey> TLeftKeyFunc,
            enjima::api::KeyExtractFuncT<TRight, TKey> TRightKeyFunc, typename TOutput,
            enjima::api::JoinFuncT<TLeft, TRight, TOutput> TFunc>
    FixedEventTimeWindowJoinOperator<TLeft, TRight, TKey, TLeftKeyFunc, TRightKeyFunc, TOutput,
            TFunc>::~FixedEventTimeWindowJoinOperator()
    {
        free(outputRecordBuffer_);
    }
}// namespace enjima::operators
