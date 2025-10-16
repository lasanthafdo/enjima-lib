//
// Created by m34ferna on 15/12/23.
//

namespace enjima::operators {
    template<typename TInput, typename TOutput, int NOuts>
    SingleInputOperator<TInput, TOutput, NOuts>::SingleInputOperator(enjima::operators::OperatorID opId,
            const std::string& opName)
        : StreamingOperator(opId, opName)
    {
    }

    template<typename TInput, typename TOutput, int NOuts>
    SingleInputOperator<TInput, TOutput, NOuts>::~SingleInputOperator()
    {
        for (auto outputCollector: outputCollectorArr_) {
            delete outputCollector;
        }
        free(inputRecordBuffer_);
        free(outputRecordBuffer_);
    }

    template<typename TInput, typename TOutput, int NOuts>
    uint8_t SingleInputOperator<TInput, TOutput, NOuts>::ProcessBlock()
    {
        auto operatorStatus = kInitStatus;
        if (inputBlockPtr_->IsBlockReturned()) {
            inputBlockPtr_ = GetNextInputBlock(upstreamChannelId_, inputBlockPtr_, pMemoryManager_);
        }
        for (auto i = 0; i < NOuts; i++) {
            if (!outputBlockPtrArr_[i]->CanWrite()) {
                outputBlockPtrArr_[i] =
                        GetNextOutputBlock<TOutput>(downstreamChannelIdArr_[i], outputBlockPtrArr_[i], pMemoryManager_);
                outputCollectorArr_[i]->SetOutputBlock(outputBlockPtrArr_[i]);
            }
        }
        while (inputBlockPtr_->CanRead() && std::ranges::all_of(outputBlockPtrArr_, [&](auto& memBlockPtr) {
            return memBlockPtr != reservedMemoryBlockPtr_;
        })) {
            // If you can write, first write
            if (std::ranges::all_of(outputBlockPtrArr_, [&](auto& memBlockPtr) { return memBlockPtr->CanWrite(); })) {
                core::Record<TInput>* inputRecord = inputBlockPtr_->Read<TInput>();
                inCounter_->IncRelaxed();
                if (inputRecord->GetRecordType() != core::Record<TInput>::RecordType::kLatency) {
                    ProcessEvent(inputRecord->GetTimestamp(), inputRecord->GetData(), outputCollectorArr_);
                }
                else {
                    for (auto outputCollector: outputCollectorArr_) {
                        outputCollector->template Collect<TOutput>(core::Record<TOutput>(
                                core::Record<TOutput>::RecordType::kLatency, inputRecord->GetTimestamp()));
                    }
                }
            }
            // Regardless of whether we can write to the output block or not, if we can read it,
            // we should mark it as having output
            if (!static_cast<bool>(operatorStatus & kHasOutput) &&
                    std::ranges::any_of(outputBlockPtrArr_,
                            [&](auto& memBlockPtr) { return memBlockPtr->CanRead(); })) {
                operatorStatus |= kHasOutput;
            }
            // The previous check to outputBlockPtr_->CanWrite() might have changed from true to false due to writing
            // (a) new record(s), or it might have been originally been false. Either way, try to get a new output block
            for (auto i = 0; i < NOuts; i++) {
                if (!outputBlockPtrArr_[i]->CanWrite()) {
                    outputBlockPtrArr_[i] = GetNextOutputBlock<TOutput>(downstreamChannelIdArr_[i],
                            outputBlockPtrArr_[i], pMemoryManager_);
                    outputCollectorArr_[i]->SetOutputBlock(outputBlockPtrArr_[i]);
                }
            }
        }
        if (inputBlockPtr_->IsReadComplete()) {
            // Note that a block that has already been returned can be visible here due to race condition. Consider the
            // events happening in order:
            // 1. Writer thread completes writing to the block, but has not yet exited the processBlock() method
            // 2. Reader thread completes reading all the data written, and returns the block to memory manager.
            // 3. Reader thread cannot obtain a new readable block, therefore the inputBlockPtr still points to the already returned block.
            // Reader thread returns to the task with the status kNoReadableInput, therefore it starts to wait on the upstream operator (writer thread).
            // 4. Writer thread returns to its task, and its status is kHasReadableOutput (even though the output produced is already read).
            // So it will notify the downstream reader thread that data is available to be read even though that data has already being read.
            // 5. Downstream reader thread wakes up, tries to get a new block for reading, but being unable to do so,
            // it will reach here with the inputBlockPtr still pointing to the already returned block.
            if (!inputBlockPtr_->IsBlockReturned()) {
                assert(inputBlockPtr_->IsReadActive());
                pMemoryManager_->ReturnBlock(inputBlockPtr_);
            }
            inputBlockPtr_ = GetNextInputBlock(upstreamChannelId_, inputBlockPtr_, pMemoryManager_);
        }
        if (inputBlockPtr_->CanRead()) {
            operatorStatus |= kHasInput;
        }
        if (std::ranges::any_of(outputBlockPtrArr_, [&](auto& memBlockPtr) { return memBlockPtr->CanRead(); })) {
            if (!static_cast<bool>(operatorStatus & kHasOutput)) {
                operatorStatus |= kHasOutput;
            }
        }
        else if (std::ranges::any_of(outputBlockPtrArr_,
                         [&](auto& memBlockPtr) { return memBlockPtr == reservedMemoryBlockPtr_; })) {
            operatorStatus &= ~kCanOutput;
        }
        return operatorStatus;
    }

    template<typename TInput, typename TOutput, int NOuts>
    uint8_t SingleInputOperator<TInput, TOutput, NOuts>::ProcessBlockInBatches()
    {
        auto operatorStatus = kInitStatus;
        if (inputBlockPtr_->IsReadComplete()) {
            inputBlockPtr_ = GetNextInputBlock(upstreamChannelId_, inputBlockPtr_, pMemoryManager_);
        }
        for (auto i = 0; i < NOuts; i++) {
            if (!outputBlockPtrArr_[i]->CanWrite()) {
                outputBlockPtrArr_[i] =
                        GetNextOutputBlock<TOutput>(downstreamChannelIdArr_[i], outputBlockPtrArr_[i], pMemoryManager_);
                outputCollectorArr_[i]->SetOutputBlock(outputBlockPtrArr_[i]);
            }
        }
        while (inputBlockPtr_->CanRead() && std::ranges::all_of(outputBlockPtrArr_, [&](auto& memBlockPtr) {
            return memBlockPtr != reservedMemoryBlockPtr_;
        })) {
            if (std::ranges::all_of(outputBlockPtrArr_, [&](auto& memBlockPtr) { return memBlockPtr->CanWrite(); })) {
                auto nReadable = inputBlockPtr_->GetNumReadableEvents<TInput>();
                auto minWritableBlock =
                        std::ranges::min(outputBlockPtrArr_, [](auto& memBlockPtrA, auto& memBlockPtrB) {
                            return memBlockPtrA->template GetNumWritableEvents<TOutput>() <
                                   memBlockPtrB->template GetNumWritableEvents<TOutput>();
                        });
                auto nWritable = minWritableBlock->template GetNumWritableEvents<TOutput>();
                auto batchSize = std::min(nReadable, nWritable);
#if ENJIMA_METRICS_LEVEL >= 3
                batchSizeGauge_->UpdateVal(static_cast<double>(batchSize));
#endif
                auto numRecordsRead = inputBlockPtr_->ReadBatch<TInput>(inputRecordBuffer_, batchSize);
                inCounter_->IncRelaxed(numRecordsRead);
                ProcessBatch(inputRecordBuffer_, numRecordsRead, outputRecordBuffer_, outputCollectorArr_);
            }
            if (!static_cast<bool>(operatorStatus & kHasOutput) &&
                    std::ranges::any_of(outputBlockPtrArr_,
                            [&](auto& memBlockPtr) { return memBlockPtr->CanRead(); })) {
                operatorStatus |= kHasOutput;
            }
            for (auto i = 0; i < NOuts; i++) {
                if (!outputBlockPtrArr_[i]->CanWrite()) {
                    outputBlockPtrArr_[i] = GetNextOutputBlock<TOutput>(downstreamChannelIdArr_[i],
                            outputBlockPtrArr_[i], pMemoryManager_);
                    outputCollectorArr_[i]->SetOutputBlock(outputBlockPtrArr_[i]);
                }
            }
        }
        if (inputBlockPtr_->IsReadComplete()) {
            if (!inputBlockPtr_->IsBlockReturned()) {
                pMemoryManager_->ReturnBlock(inputBlockPtr_);
            }
            inputBlockPtr_ = GetNextInputBlock(upstreamChannelId_, inputBlockPtr_, pMemoryManager_);
        }
        if (inputBlockPtr_->CanRead()) {
            operatorStatus |= kHasInput;
        }
        if (std::ranges::any_of(outputBlockPtrArr_, [&](auto& memBlockPtr) { return memBlockPtr->CanRead(); })) {
            if (!static_cast<bool>(operatorStatus & kHasOutput)) {
                operatorStatus |= kHasOutput;
            }
        }
        else if (std::ranges::any_of(outputBlockPtrArr_,
                         [&](auto& memBlockPtr) { return memBlockPtr == reservedMemoryBlockPtr_; })) {
            operatorStatus &= ~kCanOutput;
        }
        return operatorStatus;
    }

    template<typename TInput, typename TOutput, int NOuts>
    ChannelID SingleInputOperator<TInput, TOutput, NOuts>::GetBlockedUpstreamChannelId() const
    {
        return upstreamChannelId_;
    }

    template<typename TInput, typename TOutput, int NOuts>
    void SingleInputOperator<TInput, TOutput, NOuts>::Initialize(runtime::ExecutionEngine* executionEngine,
            memory::MemoryManager* memoryManager, metrics::Profiler* profiler)
    {
        pExecutionEngine_ = executionEngine;
        pMemoryManager_ = memoryManager;

        auto upstreamChannelIdVec = pMemoryManager_->GetUpstreamChannelIds(operatorId_);
        assert(upstreamChannelIdVec.size() == 1);
        upstreamChannelId_ = upstreamChannelIdVec[0];
        inputBlockPtr_ = pMemoryManager_->RequestReadableBlock(upstreamChannelId_);
        assert(inputBlockPtr_ != nullptr);

        auto downstreamChannelIdVec = pMemoryManager_->GetDownstreamChannelIds(operatorId_);
        for (auto i = 0; i < NOuts; i++) {
            downstreamChannelIdArr_[i] = downstreamChannelIdVec[i];
            outputBlockPtrArr_[i] = pMemoryManager_->RequestEmptyBlock<TOutput>(downstreamChannelIdArr_[i]);
            assert(outputBlockPtrArr_[i] != nullptr);
        }
#if ENJIMA_METRICS_LEVEL >= 3
        batchSizeGauge_ =
                profiler->GetOrCreateDoubleAverageGauge(operatorName_ + metrics::kBatchSizeAverageGaugeSuffix);
#endif
        inCounter_ = profiler->GetOrCreateCounter(operatorName_ + metrics::kInCounterSuffix);
        outCounterArr_[0] = profiler->GetOrCreateCounter(operatorName_ + metrics::kOutCounterSuffix);
        profiler->GetOrCreateOperatorCostGauge(operatorName_);
        profiler->GetOrCreateOperatorSelectivityGauge(operatorName_, inCounter_, outCounterArr_[0]);
        outputCollectorArr_[0] = new core::OutputCollector(outputBlockPtrArr_[0], outCounterArr_[0]);
        for (auto i = 1; i < NOuts; i++) {
            outCounterArr_[i] = profiler->GetOrCreateCounter(
                    operatorName_ + metrics::kOutSuffix + std::to_string(i) + metrics::kCounterSuffix);
            outputCollectorArr_[i] = new core::OutputCollector(outputBlockPtrArr_[i], outCounterArr_[i]);
        }
        inputRecordBuffer_ = aligned_alloc(ENJIMA_BUFFER_ALIGNMENT, inputBlockPtr_->GetTotalCapacity());
        outputRecordBuffer_ = aligned_alloc(ENJIMA_BUFFER_ALIGNMENT, outputBlockPtrArr_[0]->GetTotalCapacity());
        reservedMemoryBlockPtr_ = pMemoryManager_->GetReservedMemoryBlock();
    }

    template<typename TInput, typename TOutput, int NOuts>
    void SingleInputOperator<TInput, TOutput, NOuts>::InitializeQueues(
            std::vector<queueing::RecordQueueBase*> inputQueues, size_t outputQueueSize)
    {
        assert(inputQueues.size() == 1);
        this->inputQueue_ = static_cast<queueing::RecordQueueImpl<core::Record<TInput>>*>(inputQueues.at(0));
        for (auto i = 0; i < NOuts; i++) {
            this->outputQueueArr_[i] = new queueing::RecordQueueImpl<core::Record<TOutput>>(outputQueueSize);
        }
    }

    template<typename TInput, typename TOutput, int NOuts>
    std::vector<queueing::RecordQueueBase*> SingleInputOperator<TInput, TOutput, NOuts>::GetOutputQueues()
    {
        return std::vector<queueing::RecordQueueBase*>{outputQueueArr_.begin(), outputQueueArr_.end()};
    }

    template<typename TInput, typename TOutput, int NOuts>
    constexpr size_t SingleInputOperator<TInput, TOutput, NOuts>::GetOutputRecordSize() const
    {
        return sizeof(core::Record<TOutput>);
    }

    template<typename TInput, typename TOutput, int NOuts>
    consteval TOutput SingleInputOperator<TInput, TOutput, NOuts>::GetOutputType()
    {
        return nullptr;
    }

    template<typename TInput, typename TOutput, int NOuts>
    consteval TInput SingleInputOperator<TInput, TOutput, NOuts>::GetInputType()
    {
        return nullptr;
    }
}// namespace enjima::operators