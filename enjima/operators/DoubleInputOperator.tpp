//
// Created by m34ferna on 15/12/23.
//

namespace enjima::operators {
    template<typename TLeft, typename TRight, typename TOutput>
    DoubleInputOperator<TLeft, TRight, TOutput>::DoubleInputOperator(enjima::operators::OperatorID opId,
            const std::string& opName)
        : StreamingOperator(opId, opName)
    {
    }

    template<typename TLeft, typename TRight, typename TOutput>
    DoubleInputOperator<TLeft, TRight, TOutput>::~DoubleInputOperator()
    {
        delete outputCollector_;
        free(leftInputRecordBuffer_);
        free(rightInputRecordBuffer_);
    }

    template<typename TLeft, typename TRight, typename TOutput>
    uint8_t DoubleInputOperator<TLeft, TRight, TOutput>::ProcessBlock()
    {
        auto operatorStatus = kInitStatus;
        if (leftInputBlockPtr_->IsBlockReturned()) {
            leftInputBlockPtr_ = GetNextInputBlock(upstreamLeftChannelId_, leftInputBlockPtr_, pMemoryManager_);
        }
        if (rightInputBlockPtr_->IsBlockReturned()) {
            rightInputBlockPtr_ = GetNextInputBlock(upstreamRightChannelId_, rightInputBlockPtr_, pMemoryManager_);
        }
        if (!outputBlockPtr_->CanWrite()) {
            outputBlockPtr_ = GetNextOutputBlock<TOutput>(downstreamChannelId_, outputBlockPtr_, pMemoryManager_);
            outputCollector_->SetOutputBlock(outputBlockPtr_);
        }
        while ((leftInputBlockPtr_->CanRead() || rightInputBlockPtr_->CanRead()) &&
                outputBlockPtr_ != reservedMemoryBlockPtr_) {
            // If you can write, first write
            if (outputBlockPtr_->CanWrite()) {
                if (this->hasPendingOverflow_) {
                    ProcessPendingOverflow(outputCollector_);
                    operatorStatus |= kHasOutput;
                    // Note that we try to obtain a block at the end of processing pending events.
                    // So we should simply break the loop without trying to obtain a block again.
                    // TODO PotentialOptimization:: But then again, we don't need to have an output block ready all the time.
                    if (!outputBlockPtr_->CanWrite()) {
                        break;
                    }
                }
                if (leftInputBlockPtr_->CanRead()) {
                    core::Record<TLeft>* pLeftInputRecord = leftInputBlockPtr_->Read<TLeft>();
                    leftInCounter_->IncRelaxed();
                    inCounter_->IncRelaxed();
                    if (pLeftInputRecord->GetRecordType() != core::Record<TLeft>::RecordType::kLatency) {
                        ProcessLeftEvent(pLeftInputRecord, outputCollector_);
                    }
                    else {
                        outputCollector_->Collect<TOutput>(core::Record<TOutput>(
                                core::Record<TOutput>::RecordType::kLatency, pLeftInputRecord->GetTimestamp()));
                    }
                }
                if (rightInputBlockPtr_->CanRead()) {
                    core::Record<TRight>* pRightInputRecord = rightInputBlockPtr_->Read<TRight>();
                    rightInCounter_->IncRelaxed();
                    inCounter_->IncRelaxed();
                    if (pRightInputRecord->GetRecordType() != core::Record<TRight>::RecordType::kLatency) {
                        ProcessRightEvent(pRightInputRecord, outputCollector_);
                    }
                    else {
                        outputCollector_->Collect<TOutput>(core::Record<TOutput>(
                                core::Record<TOutput>::RecordType::kLatency, pRightInputRecord->GetTimestamp()));
                    }
                }
            }
            // Regardless of whether we can write to the output block or not, if we can read it,
            // we should mark it as having output
            if (!static_cast<bool>(operatorStatus & kHasOutput) && outputBlockPtr_->CanRead()) {
                operatorStatus |= kHasOutput;
            }
            // The previous check to outputBlockPtr_->CanWrite() might have changed from true to false due to writing
            // (a) new record(s), or it might have been originally been false. Either way, try to get a new output block
            if (!outputBlockPtr_->CanWrite()) {
                outputBlockPtr_ = GetNextOutputBlock<TOutput>(downstreamChannelId_, outputBlockPtr_, pMemoryManager_);
                outputCollector_->SetOutputBlock(outputBlockPtr_);
            }
        }
        if (leftInputBlockPtr_->IsReadComplete()) {
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
            if (!leftInputBlockPtr_->IsBlockReturned()) {
                assert(leftInputBlockPtr_->IsReadActive());
                pMemoryManager_->ReturnBlock(leftInputBlockPtr_);
            }
            leftInputBlockPtr_ = GetNextInputBlock(upstreamLeftChannelId_, leftInputBlockPtr_, pMemoryManager_);
        }
        if (rightInputBlockPtr_->IsReadComplete()) {
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
            if (!rightInputBlockPtr_->IsBlockReturned()) {
                assert(rightInputBlockPtr_->IsReadActive());
                pMemoryManager_->ReturnBlock(rightInputBlockPtr_);
            }
            rightInputBlockPtr_ = GetNextInputBlock(upstreamRightChannelId_, rightInputBlockPtr_, pMemoryManager_);
        }
        if (leftInputBlockPtr_->CanRead() || rightInputBlockPtr_->CanRead()) {
            operatorStatus |= kHasInput;
        }
        else {
            blockedUpstreamChannelId_ =
                    leftInputBlockPtr_->CanRead() ? upstreamRightChannelId_ : upstreamLeftChannelId_;
        }
        if (outputBlockPtr_->CanRead()) {
            if (!static_cast<bool>(operatorStatus & kHasOutput)) {
                operatorStatus |= kHasOutput;
            }
        }
        else if (outputBlockPtr_ == reservedMemoryBlockPtr_) {
            operatorStatus &= ~kCanOutput;
        }
        return operatorStatus;
    }

    template<typename TLeft, typename TRight, typename TOutput>
    uint8_t DoubleInputOperator<TLeft, TRight, TOutput>::ProcessBlockInBatches()
    {
        auto operatorStatus = kInitStatus;
        if (leftInputBlockPtr_->IsReadComplete()) {
            leftInputBlockPtr_ = GetNextInputBlock(upstreamLeftChannelId_, leftInputBlockPtr_, pMemoryManager_);
        }
        if (rightInputBlockPtr_->IsReadComplete()) {
            rightInputBlockPtr_ = GetNextInputBlock(upstreamRightChannelId_, rightInputBlockPtr_, pMemoryManager_);
        }
        SetNextOutputBlockIfNecessary();
        while ((leftInputBlockPtr_->CanRead() || rightInputBlockPtr_->CanRead()) &&
                outputBlockPtr_ != reservedMemoryBlockPtr_) {
            if (outputBlockPtr_->CanWrite()) {
                if (hasPendingOverflow_) {
                    ProcessPendingOverflowBatch(outputCollector_);
                    operatorStatus |= kHasOutput;
                    if (!outputBlockPtr_->CanWrite()) {
                        break;
                    }
                }
                auto nReadableLeft = leftInputBlockPtr_->GetNumReadableEvents<TLeft>();
                auto nReadableRight = rightInputBlockPtr_->GetNumReadableEvents<TRight>();
#if ENJIMA_METRICS_LEVEL >= 3
                batchSizeGauge_->UpdateVal(static_cast<double>(batchSize));
#endif
                auto numRecordsReadLeft = leftInputBlockPtr_->ReadBatch<TLeft>(leftInputRecordBuffer_, nReadableLeft);
                inCounter_->IncRelaxed(numRecordsReadLeft);
                leftInCounter_->IncRelaxed(numRecordsReadLeft);
                auto numRecordsReadRight =
                        rightInputBlockPtr_->ReadBatch<TRight>(rightInputRecordBuffer_, nReadableRight);
                inCounter_->IncRelaxed(numRecordsReadRight);
                rightInCounter_->IncRelaxed(numRecordsReadRight);
                ProcessBatch(leftInputRecordBuffer_, numRecordsReadLeft, rightInputRecordBuffer_, numRecordsReadRight,
                        outputCollector_);
            }
            if (!static_cast<bool>(operatorStatus & kHasOutput) && outputBlockPtr_->CanRead()) {
                operatorStatus |= kHasOutput;
            }
            SetNextOutputBlockIfNecessary();
        }
        if (leftInputBlockPtr_->IsReadComplete()) {
            if (!leftInputBlockPtr_->IsBlockReturned()) {
                pMemoryManager_->ReturnBlock(leftInputBlockPtr_);
            }
            leftInputBlockPtr_ = GetNextInputBlock(upstreamLeftChannelId_, leftInputBlockPtr_, pMemoryManager_);
        }
        if (rightInputBlockPtr_->IsReadComplete()) {
            if (!rightInputBlockPtr_->IsBlockReturned()) {
                pMemoryManager_->ReturnBlock(rightInputBlockPtr_);
            }
            rightInputBlockPtr_ = GetNextInputBlock(upstreamRightChannelId_, rightInputBlockPtr_, pMemoryManager_);
        }

        if (leftInputBlockPtr_->CanRead() || rightInputBlockPtr_->CanRead()) {
            operatorStatus |= kHasInput;
        }
        else {
            blockedUpstreamChannelId_ =
                    leftInputBlockPtr_->CanRead() ? upstreamRightChannelId_ : upstreamLeftChannelId_;
        }

        if (outputBlockPtr_->CanRead()) {
            if (!static_cast<bool>(operatorStatus & kHasOutput)) {
                operatorStatus |= kHasOutput;
            }
        }
        else if (outputBlockPtr_ == reservedMemoryBlockPtr_) {
            operatorStatus &= ~kCanOutput;
        }
        return operatorStatus;
    }

    template<typename TLeft, typename TRight, typename TOutput>
    bool DoubleInputOperator<TLeft, TRight, TOutput>::SetNextOutputBlockIfNecessary()
    {
        if (outputBlockPtr_->CanWrite()) {
            return true;
        }
        else {
            outputBlockPtr_ = GetNextOutputBlock<TOutput>(downstreamChannelId_, outputBlockPtr_, pMemoryManager_);
            outputCollector_->SetOutputBlock(outputBlockPtr_);
            return outputBlockPtr_->CanWrite();
        }
    }

    template<typename TLeft, typename TRight, typename TOutput>
    ChannelID DoubleInputOperator<TLeft, TRight, TOutput>::GetBlockedUpstreamChannelId() const
    {
        return blockedUpstreamChannelId_;
    }

    template<typename TLeft, typename TRight, typename TOutput>
    void DoubleInputOperator<TLeft, TRight, TOutput>::Initialize(runtime::ExecutionEngine* executionEngine,
            memory::MemoryManager* memoryManager, metrics::Profiler* profiler)
    {
        pExecutionEngine_ = executionEngine;
        pMemoryManager_ = memoryManager;

        auto upstreamChannelIdVec = pMemoryManager_->GetUpstreamChannelIds(operatorId_);
        assert(upstreamChannelIdVec.size() == 2);
        upstreamLeftChannelId_ = upstreamChannelIdVec[0];
        leftInputBlockPtr_ = pMemoryManager_->RequestReadableBlock(upstreamLeftChannelId_);
        assert(leftInputBlockPtr_ != nullptr);

        upstreamRightChannelId_ = upstreamChannelIdVec[1];
        rightInputBlockPtr_ = pMemoryManager_->RequestReadableBlock(upstreamRightChannelId_);
        assert(rightInputBlockPtr_ != nullptr);

        auto downstreamChannelIdVec = pMemoryManager_->GetDownstreamChannelIds(operatorId_);
        assert(downstreamChannelIdVec.size() == 1);
        downstreamChannelId_ = downstreamChannelIdVec[0];
        outputBlockPtr_ = pMemoryManager_->RequestEmptyBlock<TOutput>(downstreamChannelId_);
        assert(outputBlockPtr_ != nullptr);
#if ENJIMA_METRICS_LEVEL >= 3
        batchSizeGauge_ =
                profiler->GetOrCreateDoubleAverageGauge(operatorName_ + metrics::kBatchSizeAverageGaugeSuffix);
#endif
        inCounter_ = profiler->GetOrCreateCounter(operatorName_ + metrics::kInCounterSuffix);
        leftInCounter_ = profiler->GetOrCreateCounter(operatorName_ + metrics::kLeftInCounterSuffix);
        rightInCounter_ = profiler->GetOrCreateCounter(operatorName_ + metrics::kRightInCounterSuffix);
        outCounter_ = profiler->GetOrCreateCounter(operatorName_ + metrics::kOutCounterSuffix);
        profiler->GetOrCreateOperatorCostGauge(operatorName_);
        profiler->GetOrCreateOperatorSelectivityGauge(operatorName_, inCounter_, outCounter_);
        outputCollector_ = new core::OutputCollector(outputBlockPtr_, outCounter_);
        leftInputRecordBuffer_ = aligned_alloc(ENJIMA_BUFFER_ALIGNMENT, leftInputBlockPtr_->GetTotalCapacity());
        rightInputRecordBuffer_ = aligned_alloc(ENJIMA_BUFFER_ALIGNMENT, rightInputBlockPtr_->GetTotalCapacity());
        reservedMemoryBlockPtr_ = pMemoryManager_->GetReservedMemoryBlock();
    }

    template<typename TLeft, typename TRight, typename TOutput>
    void DoubleInputOperator<TLeft, TRight, TOutput>::InitializeQueues(
            std::vector<queueing::RecordQueueBase*> inputQueues, size_t outputQueueSize)
    {
        assert(inputQueues.size() == 2);
        leftInputQueue_ = static_cast<queueing::RecordQueueImpl<core::Record<TLeft>>*>(inputQueues.at(0));
        rightInputQueue_ = static_cast<queueing::RecordQueueImpl<core::Record<TRight>>*>(inputQueues.at(1));
        outputQueue_ = new queueing::RecordQueueImpl<core::Record<TOutput>>(outputQueueSize);
    }

    template<typename TLeft, typename TRight, typename TOutput>
    std::vector<queueing::RecordQueueBase*> DoubleInputOperator<TLeft, TRight, TOutput>::GetOutputQueues()
    {
        return std::vector<queueing::RecordQueueBase*>({outputQueue_});
    }

    template<typename TLeft, typename TRight, typename TOutput>
    constexpr size_t DoubleInputOperator<TLeft, TRight, TOutput>::GetOutputRecordSize() const
    {
        return sizeof(core::Record<TOutput>);
    }

    template<typename TLeft, typename TRight, typename TOutput>
    consteval TOutput DoubleInputOperator<TLeft, TRight, TOutput>::GetOutputType()
    {
        return nullptr;
    }

    template<typename TLeft, typename TRight, typename TOutput>
    consteval TLeft DoubleInputOperator<TLeft, TRight, TOutput>::GetLeftInputType()
    {
        return nullptr;
    }

    template<typename TLeft, typename TRight, typename TOutput>
    consteval TRight DoubleInputOperator<TLeft, TRight, TOutput>::GetRightInputType()
    {
        return nullptr;
    }
}// namespace enjima::operators