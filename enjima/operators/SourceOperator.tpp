//
// Created by m34ferna on 01/02/24.
//

namespace enjima::operators {
    template<typename TOutput>
    SourceOperator<TOutput>::SourceOperator(enjima::operators::OperatorID opId, const std::string& opName)
        : StreamingOperator(opId, opName)
    {
    }

    template<typename TOutput>
    SourceOperator<TOutput>::~SourceOperator()
    {
        delete outputCollector_;
        free(outputRecordBuffer_);
    }

    template<typename TOutput>
    uint8_t SourceOperator<TOutput>::ProcessBlock()
    {
        auto operatorStatus = kRunThreshold;
        if (!outputBlockPtr_->CanWrite()) {
            outputBlockPtr_ = GetNextOutputBlock<TOutput>(downstreamChannelId_, outputBlockPtr_, pMemoryManager_);
            outputCollector_->SetOutputBlock(outputBlockPtr_);
        }
        while (outputBlockPtr_->CanWrite()) {
            if (!EmitEvent(outputCollector_)) {
                operatorStatus &= ~kHasInput;
                break;
            }
        }
        if (outputBlockPtr_->CanRead()) {
            operatorStatus |= kHasOutput;
        }
        if (!outputBlockPtr_->CanWrite()) {
            outputBlockPtr_ = GetNextOutputBlock<TOutput>(downstreamChannelId_, outputBlockPtr_, pMemoryManager_);
            outputCollector_->SetOutputBlock(outputBlockPtr_);
            if (outputBlockPtr_ == reservedMemoryBlockPtr_) {
                operatorStatus &= ~kCanOutput;
            }
        }
        return operatorStatus;
    }

    template<typename TOutput>
    uint8_t SourceOperator<TOutput>::ProcessBlockInBatches()
    {
        auto operatorStatus = kRunThreshold;
        if (!outputBlockPtr_->CanWrite()) {
            outputBlockPtr_ = GetNextOutputBlock<TOutput>(downstreamChannelId_, outputBlockPtr_, pMemoryManager_);
            outputCollector_->SetOutputBlock(outputBlockPtr_);
        }
        while (outputBlockPtr_->CanWrite()) {
            auto nWritable = outputBlockPtr_->GetNumWritableEvents<TOutput>();
            auto nWritten = EmitBatch(nWritable, outputRecordBuffer_, outputCollector_);
#if ENJIMA_METRICS_LEVEL >= 3
            batchSizeGauge_->UpdateVal(static_cast<double>(nWritten));
#endif
            if (nWritten != nWritable) {
                operatorStatus &= ~kHasInput;
                break;
            }
        }
        if (outputBlockPtr_->CanRead()) {
            operatorStatus |= kHasOutput;
        }
        if (!outputBlockPtr_->CanWrite()) {
            outputBlockPtr_ = GetNextOutputBlock<TOutput>(downstreamChannelId_, outputBlockPtr_, pMemoryManager_);
            outputCollector_->SetOutputBlock(outputBlockPtr_);
            if (outputBlockPtr_ == reservedMemoryBlockPtr_) {
                operatorStatus &= ~kCanOutput;
            }
        }
        return operatorStatus;
    }

    template<typename TOutput>
    uint8_t SourceOperator<TOutput>::ProcessQueue()
    {
        auto operatorStatus = kFlowThreshold;
        auto defaultBlockSize = pMemoryManager_->GetDefaultNumEventsPerBlock();
        for (size_t i = 0; i < defaultBlockSize; i++) {
            if (!this->outputQueue_->full()) {
                core::Record<TOutput> outputRecord;
                if (GenerateQueueRecord(outputRecord)) {
                    this->outputQueue_->push(outputRecord);
                    outCounter_->IncRelaxed();
                }
                else {
                    operatorStatus &= ~kHasInput;
                    break;
                }
            }
            else {
                operatorStatus &= ~kCanOutput;
                break;
            }
        }
        if (this->outputQueue_->empty()) {
            operatorStatus &= ~kHasOutput;
        }
        return operatorStatus;
    }

    template<typename TOutput>
    ChannelID SourceOperator<TOutput>::GetBlockedUpstreamChannelId() const
    {
        return 0;
    }

    template<typename TOutput>
    void SourceOperator<TOutput>::Initialize(runtime::ExecutionEngine* executionEngine,
            memory::MemoryManager* memoryManager, metrics::Profiler* profiler)
    {
        pExecutionEngine_ = executionEngine;
        pMemoryManager_ = memoryManager;
#if ENJIMA_METRICS_LEVEL >= 3
        batchSizeGauge_ =
                profiler->GetOrCreateDoubleAverageGauge(operatorName_ + metrics::kBatchSizeAverageGaugeSuffix);
#endif
        outCounter_ = profiler->GetOrCreateCounter(operatorName_ + metrics::kOutCounterSuffix);
        profiler->GetOrCreateThroughputGauge(operatorName_ + metrics::kOutThroughputGaugeSuffix, outCounter_);
        profiler->GetOrCreateOperatorCostGauge(operatorName_);
        profiler->GetOrCreateOperatorSelectivityGauge(operatorName_, outCounter_, outCounter_);

        auto downstreamChannelIdVec = pMemoryManager_->GetDownstreamChannelIds(operatorId_);
        assert(downstreamChannelIdVec.size() == 1);
        downstreamChannelId_ = downstreamChannelIdVec[0];
        outputBlockPtr_ = pMemoryManager_->RequestEmptyBlock<TOutput>(downstreamChannelId_);
        outputCollector_ = new core::OutputCollector(outputBlockPtr_, outCounter_);
        outputRecordBuffer_ = aligned_alloc(ENJIMA_BUFFER_ALIGNMENT, outputBlockPtr_->GetTotalCapacity());
        reservedMemoryBlockPtr_ = pMemoryManager_->GetReservedMemoryBlock();
    }

    template<typename TOutput>
    void SourceOperator<TOutput>::InitializeQueues(std::vector<queueing::RecordQueueBase*> inputQueues,
            size_t outputQueueSize)
    {
        assert(inputQueues.empty());
        outputQueue_ = new queueing::RecordQueueImpl<core::Record<TOutput>>(outputQueueSize);
    }

    template<typename TOutput>
    std::vector<queueing::RecordQueueBase*> SourceOperator<TOutput>::GetOutputQueues()
    {
        return std::vector<queueing::RecordQueueBase*>{outputQueue_};
    }

    template<typename TOutput>
    constexpr size_t SourceOperator<TOutput>::GetOutputRecordSize() const
    {
        return sizeof(core::Record<TOutput>);
    }

    template<typename TOutput>
    bool SourceOperator<TOutput>::IsSourceOperator() const
    {
        return true;
    }

    template<typename TOutput>
    consteval TOutput SourceOperator<TOutput>::GetOutputType()
    {
        return nullptr;
    }
}// namespace enjima::operators
