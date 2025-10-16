//
// Created by m34ferna on 03/02/24.
//

namespace enjima::operators {
    template<typename TInput, typename Duration>
    SinkOperator<TInput, Duration>::SinkOperator(OperatorID opId, const std::string& opName)
        : StreamingOperator(opId, opName)
    {
    }

    template<typename TInput, typename Duration>
    SinkOperator<TInput, Duration>::~SinkOperator()
    {
        free(inputRecordBuffer_);
    }

    template<typename TInput, typename Duration>
    uint8_t SinkOperator<TInput, Duration>::ProcessBlock()
    {
        auto operatorStatus = kInitStatus;
        if (inputBlockPtr_->IsReadComplete()) {
            inputBlockPtr_ = GetNextInputBlock(upstreamChannelId_, inputBlockPtr_, pMemoryManager_);
        }
        while (inputBlockPtr_->CanRead()) {
            core::Record<TInput>* inputRec = inputBlockPtr_->template Read<TInput>();
            auto recTimestamp = inputRec->GetTimestamp();
            inCounter_->IncRelaxed();
            if (inputRec->GetRecordType() != core::Record<TInput>::RecordType::kLatency) {
                ProcessSinkEvent(recTimestamp, inputRec->GetData());
            }
            else {
                auto currentTimestamp = enjima::runtime::GetSystemTime<Duration>();
                this->latencyHistogram_->Update(currentTimestamp - recTimestamp);
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
        return operatorStatus;
    }

    template<typename TInput, typename Duration>
    uint8_t SinkOperator<TInput, Duration>::ProcessBlockInBatches()
    {
        auto operatorStatus = kInitStatus;
        if (inputBlockPtr_->IsReadComplete()) {
            inputBlockPtr_ = GetNextInputBlock(upstreamChannelId_, inputBlockPtr_, pMemoryManager_);
        }
        while (inputBlockPtr_->CanRead()) {
            auto nReadable = inputBlockPtr_->GetNumReadableEvents<TInput>();
            auto numRecordsRead = inputBlockPtr_->ReadBatch<TInput>(inputRecordBuffer_, nReadable);
#if ENJIMA_METRICS_LEVEL >= 3
            batchSizeGauge_->UpdateVal(static_cast<double>(numRecordsRead));
#endif
            inCounter_->IncRelaxed(numRecordsRead);
            ProcessSinkBatch(inputRecordBuffer_, numRecordsRead);
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
        return operatorStatus;
    }

    template<typename TInput, typename Duration>
    uint8_t SinkOperator<TInput, Duration>::ProcessQueue()
    {
        auto operatorStatus = kInitStatus;
        size_t numProcessed = 0;
        while (!this->inputQueue_->empty() && numProcessed <= this->pMemoryManager_->GetDefaultNumEventsPerBlock()) {
            core::Record<TInput> inputRec;
            if (inputQueue_->try_pop(inputRec)) {
                inCounter_->Inc();
                numProcessed++;
                auto recTimestamp = inputRec.GetTimestamp();
                if (inputRec.GetRecordType() != core::Record<TInput>::RecordType::kLatency) {
                    ProcessSinkEvent(recTimestamp, inputRec.GetData());
                }
                else {
                    auto currentTimestamp = enjima::runtime::GetSystemTime<Duration>();
                    this->latencyHistogram_->Update(currentTimestamp - recTimestamp);
                }
            }
        }
        if (!this->inputQueue_->empty()) {
            operatorStatus |= kHasInput;
        }
        return operatorStatus;
    }

    template<typename TInput, typename Duration>
    ChannelID SinkOperator<TInput, Duration>::GetBlockedUpstreamChannelId() const
    {
        return upstreamChannelId_;
    }

    template<typename TInput, typename Duration>
    void SinkOperator<TInput, Duration>::Initialize(runtime::ExecutionEngine* executionEngine,
            memory::MemoryManager* memoryManager, metrics::Profiler* profiler)
    {
        pExecutionEngine_ = executionEngine;
        pMemoryManager_ = memoryManager;
#if ENJIMA_METRICS_LEVEL >= 3
        batchSizeGauge_ =
                profiler->GetOrCreateDoubleAverageGauge(operatorName_ + metrics::kBatchSizeAverageGaugeSuffix);
#endif
        inCounter_ = profiler->GetOrCreateCounter(operatorName_ + metrics::kInCounterSuffix);
        profiler->GetOrCreateThroughputGauge(operatorName_ + metrics::kInThroughputGaugeSuffix, inCounter_);
        latencyHistogram_ = profiler->GetOrCreateHistogram(operatorName_ + metrics::kLatencyHistogramSuffix, 200);
        profiler->GetOrCreateLatencyGradientGauge(operatorName_ + metrics::kLatencyGradGaugeSuffix, latencyHistogram_);
        profiler->GetOrCreateOperatorCostGauge(operatorName_);
        profiler->GetOrCreateOperatorSelectivityGauge(operatorName_, inCounter_, inCounter_);

        auto upstreamChannelIdVec = pMemoryManager_->GetUpstreamChannelIds(operatorId_);
        assert(upstreamChannelIdVec.size() == 1);
        upstreamChannelId_ = upstreamChannelIdVec[0];
        inputBlockPtr_ = pMemoryManager_->RequestReadableBlock(upstreamChannelId_);
        assert(inputBlockPtr_ != nullptr);
        inputRecordBuffer_ = aligned_alloc(ENJIMA_BUFFER_ALIGNMENT, inputBlockPtr_->GetTotalCapacity());
    }


    template<typename TInput, typename Duration>
    void SinkOperator<TInput, Duration>::InitializeQueues(std::vector<queueing::RecordQueueBase*> inputQueues,
            size_t outputQueueSize)
    {
        assert(inputQueues.size() == 1);
        inputQueue_ = static_cast<queueing::RecordQueueImpl<core::Record<TInput>>*>(inputQueues.at(0));
    }

    template<typename TInput, typename Duration>
    std::vector<queueing::RecordQueueBase*> SinkOperator<TInput, Duration>::GetOutputQueues()
    {
        return std::vector<queueing::RecordQueueBase*>{};
    }

    template<typename TInput, typename Duration>
    bool SinkOperator<TInput, Duration>::IsSinkOperator() const
    {
        return true;
    }

    template<typename TInput, typename Duration>
    consteval TInput SinkOperator<TInput, Duration>::GetInputType()
    {
        return nullptr;
    }
}// namespace enjima::operators
