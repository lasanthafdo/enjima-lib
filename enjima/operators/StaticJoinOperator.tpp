//
// Created by m34ferna on 01/03/24.
//

namespace enjima::operators {
    template<typename TLeft, typename TRight, typename TOutput, enjima::api::JoinPredT<TLeft, TRight> TPred,
            enjima::api::JoinFuncT<TLeft, TRight, TOutput> TFunc>
    StaticJoinOperator<TLeft, TRight, TOutput, TPred, TFunc>::StaticJoinOperator(OperatorID opId,
            const std::string& opName, TPred joinPred, TFunc joinFunc, std::vector<TRight>& staticData)
        : SingleInputOperator<TLeft, TOutput>(opId, opName), joinPred_(joinPred), joinFunc_(joinFunc)
    {
        rightSide_.swap(staticData);
    }

    template<typename TLeft, typename TRight, typename TOutput, enjima::api::JoinPredT<TLeft, TRight> TPred,
            enjima::api::JoinFuncT<TLeft, TRight, TOutput> TFunc>
    void StaticJoinOperator<TLeft, TRight, TOutput, TPred, TFunc>::ProcessEvent(uint64_t timestamp, TLeft inputEvent,
            enjima::core::OutputCollector* collector)
    {
        for (const auto& rightInputEvent: rightSide_) {
            if (joinPred_(inputEvent, rightInputEvent)) {
                collector->CollectWithTimestamp<TOutput>(timestamp, joinFunc_(inputEvent, rightInputEvent));
            }
        }
    }

    template<typename TLeft, typename TRight, typename TOutput, enjima::api::JoinPredT<TLeft, TRight> TPred,
            enjima::api::JoinFuncT<TLeft, TRight, TOutput> TFunc>
    void StaticJoinOperator<TLeft, TRight, TOutput, TPred, TFunc>::ProcessBatch(void* inputBuffer,
            uint32_t numRecordsToRead, void* outputBuffer, core::OutputCollector* collector)
    {
        auto numEmitted = 0u;
        auto nextOutRecordPtr = static_cast<core::Record<TOutput>*>(outputBuffer);
        for (uint32_t i = 0; i < numRecordsToRead; i++) {
            auto inputRecord = static_cast<core::Record<TLeft>*>(inputBuffer) + i;
            for (const auto& rightInputEvent: rightSide_) {
                if (joinPred_(inputRecord->GetData(), rightInputEvent)) {
                    new (nextOutRecordPtr) core::Record<TOutput>(inputRecord->GetTimestamp(),
                            joinFunc_(inputRecord->GetData(), rightInputEvent));
                    nextOutRecordPtr += 1;
                    numEmitted++;
                }
            }
        }

        collector->CollectBatch<TOutput>(outputBuffer, numEmitted);
    }

    template<typename TLeft, typename TRight, typename TOutput, enjima::api::JoinPredT<TLeft, TRight> TPred,
            enjima::api::JoinFuncT<TLeft, TRight, TOutput> TFunc>
    uint8_t StaticJoinOperator<TLeft, TRight, TOutput, TPred, TFunc>::ProcessQueue()
    {
        auto operatorStatus = StreamingOperator::kQueueInitStatus;
        while (this->inputQueue_->size() > 0) {
            core::Record<TLeft> inputRec;
            this->inputQueue_->pop(inputRec);
            this->inCounter_->Inc();

            if (inputRec.GetRecordType() != core::Record<TLeft>::RecordType::kLatency) {
                for (const auto& rightInputEvent: rightSide_) {
                    if (joinPred_(inputRec.GetData(), rightInputEvent)) {
                        auto currentSysTime = enjima::runtime::GetSystemTimeMillis();
                        core::Record<TOutput> record(currentSysTime, joinFunc_(inputRec.GetData(), rightInputEvent));
                        this->outputQueue_->push(record);
                        this->outCounter_->Inc();
                    }
                }
            }
            else {
                core::Record<TOutput> record(core::Record<TOutput>::RecordType::kLatency, inputRec.GetTimestamp());
                this->outputQueue_->push(record);
                this->outCounter_->Inc();
            }
        }
        if (this->outputQueue_->size() == 0) {
            operatorStatus &= ~StreamingOperator::kHasOutput;
        }
        return operatorStatus;
    }
}// namespace enjima::operators
