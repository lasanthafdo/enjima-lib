//
// Created by m34ferna on 15/12/23.
//

namespace enjima::operators {
    template<typename TInput, typename TOutput>
    SingleInputOperator<TInput, TOutput, 1>::SingleInputOperator(enjima::operators::OperatorID opId,
            const std::string& opName)
        : StreamingOperator(opId, opName)
    {
    }

    template<typename TInput, typename TOutput>
    SingleInputOperator<TInput, TOutput, 1>::~SingleInputOperator()
    {
        delete outputCollector_;
        free(inputRecordBuffer_);
        free(outputRecordBuffer_);
    }

    template<typename TInput, typename TOutput>
    uint8_t SingleInputOperator<TInput, TOutput, 1>::ProcessBlock()
    {
        auto operatorStatus = kInitStatus;
        if (inputBlockPtr_->IsBlockReturned()) {
            inputBlockPtr_ = GetNextInputBlock(upstreamChannelId_, inputBlockPtr_, pMemoryManager_);
        }
        if (!outputBlockPtr_->CanWrite()) {
            outputBlockPtr_ = GetNextOutputBlock<TOutput>(downstreamChannelId_, outputBlockPtr_, pMemoryManager_);
            outputCollector_->SetOutputBlock(outputBlockPtr_);
        }
        while (inputBlockPtr_->CanRead() && outputBlockPtr_ != reservedMemoryBlockPtr_) {
            // If you can write, first write
            if (outputBlockPtr_->CanWrite()) {
                if (this->hasPendingOverflow_) {
                    ProcessPendingOverflow(outputCollector_);
                    operatorStatus |= kHasOutput;
                    // Note that we try to obtain a block at the end of processing pending events.
                    // So we should simply break the loop without trying to obtain a block again.
                    if (!outputBlockPtr_->CanWrite()) {
                        break;
                    }
                }
                core::Record<TInput>* inputRecord = inputBlockPtr_->Read<TInput>();
                inCounter_->IncRelaxed();
                if (inputRecord->GetRecordType() != core::Record<TInput>::RecordType::kLatency) {
                    ProcessEvent(inputRecord->GetTimestamp(), inputRecord->GetData(), outputCollector_);
                }
                else {
                    outputCollector_->Collect<TOutput>(core::Record<TOutput>(
                            core::Record<TOutput>::RecordType::kLatency, inputRecord->GetTimestamp()));
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

    template<typename TInput, typename TOutput>
    void SingleInputOperator<TInput, TOutput, 1>::ProcessPendingOverflow(
            [[maybe_unused]] core::OutputCollector* collector)
    {
    }

    template<typename TInput, typename TOutput>
    uint8_t SingleInputOperator<TInput, TOutput, 1>::ProcessBlockInBatches()
    {
        auto operatorStatus = kInitStatus;
        if (inputBlockPtr_->IsReadComplete()) {
            inputBlockPtr_ = GetNextInputBlock(upstreamChannelId_, inputBlockPtr_, pMemoryManager_);
        }
        if (!outputBlockPtr_->CanWrite()) {
            outputBlockPtr_ = GetNextOutputBlock<TOutput>(downstreamChannelId_, outputBlockPtr_, pMemoryManager_);
            outputCollector_->SetOutputBlock(outputBlockPtr_);
        }
        while (inputBlockPtr_->CanRead() && outputBlockPtr_ != reservedMemoryBlockPtr_) {
            if (outputBlockPtr_->CanWrite()) {
                if (this->hasPendingOverflow_) {
                    ProcessPendingOverflowBatch(outputCollector_);
                    operatorStatus |= kHasOutput;
                    // Note that we try to obtain a block at the end of processing pending events.
                    // So we should simply break the loop without trying to obtain a block again.
                    if (!outputBlockPtr_->CanWrite()) {
                        break;
                    }
                }
                auto nReadable = inputBlockPtr_->GetNumReadableEvents<TInput>();
                auto nWritable = outputBlockPtr_->GetNumWritableEvents<TOutput>();
                auto batchSize = std::min(nReadable, nWritable);
#if ENJIMA_METRICS_LEVEL >= 3
                batchSizeGauge_->UpdateVal(static_cast<double>(batchSize));
#endif
                auto numRecordsRead = inputBlockPtr_->ReadBatch<TInput>(inputRecordBuffer_, batchSize);
                assert(numRecordsRead == batchSize);
                inCounter_->IncRelaxed(numRecordsRead);
                ProcessBatch(inputRecordBuffer_, numRecordsRead, outputRecordBuffer_, outputCollector_);
            }
            if (!static_cast<bool>(operatorStatus & kHasOutput) && outputBlockPtr_->CanRead()) {
                operatorStatus |= kHasOutput;
            }
            if (!outputBlockPtr_->CanWrite()) {
                outputBlockPtr_ = GetNextOutputBlock<TOutput>(downstreamChannelId_, outputBlockPtr_, pMemoryManager_);
                outputCollector_->SetOutputBlock(outputBlockPtr_);
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

    template<typename TInput, typename TOutput>
    void SingleInputOperator<TInput, TOutput, 1>::ProcessPendingOverflowBatch(
            [[maybe_unused]] core::OutputCollector* collector)
    {
    }

    template<typename TInput, typename TOutput>
    ChannelID SingleInputOperator<TInput, TOutput, 1>::GetBlockedUpstreamChannelId() const
    {
        return upstreamChannelId_;
    }

    template<typename TInput, typename TOutput>
    void SingleInputOperator<TInput, TOutput, 1>::Initialize(runtime::ExecutionEngine* executionEngine,
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
        assert(downstreamChannelIdVec.size() == 1);
        downstreamChannelId_ = downstreamChannelIdVec[0];
        outputBlockPtr_ = pMemoryManager_->RequestEmptyBlock<TOutput>(downstreamChannelId_);
        assert(outputBlockPtr_ != nullptr);
#if ENJIMA_METRICS_LEVEL >= 3
        batchSizeGauge_ =
                profiler->GetOrCreateDoubleAverageGauge(operatorName_ + metrics::kBatchSizeAverageGaugeSuffix);
#endif
        inCounter_ = profiler->GetOrCreateCounter(operatorName_ + metrics::kInCounterSuffix);
        outCounter_ = profiler->GetOrCreateCounter(operatorName_ + metrics::kOutCounterSuffix);
        profiler->GetOrCreateOperatorCostGauge(operatorName_);
        profiler->GetOrCreateOperatorSelectivityGauge(operatorName_, inCounter_, outCounter_);
        outputCollector_ = new core::OutputCollector(outputBlockPtr_, outCounter_);
        inputRecordBuffer_ = aligned_alloc(ENJIMA_BUFFER_ALIGNMENT, inputBlockPtr_->GetTotalCapacity());
        outputRecordBuffer_ = aligned_alloc(ENJIMA_BUFFER_ALIGNMENT, outputBlockPtr_->GetTotalCapacity());
        reservedMemoryBlockPtr_ = pMemoryManager_->GetReservedMemoryBlock();
    }

    template<typename TInput, typename TOutput>
    void SingleInputOperator<TInput, TOutput, 1>::InitializeQueues(std::vector<queueing::RecordQueueBase*> inputQueues,
            size_t outputQueueSize)
    {
        assert(inputQueues.size() == 1);
        this->inputQueue_ = static_cast<queueing::RecordQueueImpl<core::Record<TInput>>*>(inputQueues.at(0));
        this->outputQueue_ = new queueing::RecordQueueImpl<core::Record<TOutput>>(outputQueueSize);
    }

    template<typename TInput, typename TOutput>
    std::vector<queueing::RecordQueueBase*> SingleInputOperator<TInput, TOutput, 1>::GetOutputQueues()
    {
        return std::vector<queueing::RecordQueueBase*>{outputQueue_};
    }


    template<typename TInput, typename TOutput>
    constexpr size_t SingleInputOperator<TInput, TOutput, 1>::GetOutputRecordSize() const
    {
        return sizeof(core::Record<TOutput>);
    }

    template<typename TInput, typename TOutput>
    consteval TOutput SingleInputOperator<TInput, TOutput, 1>::GetOutputType()
    {
        return nullptr;
    }

    template<typename TInput, typename TOutput>
    consteval TInput SingleInputOperator<TInput, TOutput, 1>::GetInputType()
    {
        return nullptr;
    }
}// namespace enjima::operators