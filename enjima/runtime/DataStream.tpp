//
// Created by m34ferna on 15/03/24.
//

namespace enjima::runtime {
    template<OutStreamOpT T, typename O>
    DataStream<T, O>::DataStream(std::unique_ptr<T> opPtr, StreamingJob& streamingJob) : streamingJob_(streamingJob)
    {
        static_assert(std::is_base_of_v<operators::SourceOperator<decltype(opPtr->GetOutputType())>, T>,
                "Must pass a source operator of matching type to create a data stream using this constructor!");
        currentOpId_ = opPtr->GetOperatorId();
        streamingJob_.AddOperator(std::move(opPtr));
    }

    template<OutStreamOpT T, typename O>
    DataStream<T, O>::DataStream(std::unique_ptr<T> opPtr, operators::OperatorID prevOpId, StreamingJob& streamingJob)
        : streamingJob_(streamingJob)
    {
        static_assert(is_single_input_op_t<T>,
                "Must pass a single input operator with matching types to create a data stream using this "
                "constructor!");
        currentOpId_ = opPtr->GetOperatorId();
        streamingJob_.AddOperator(std::move(opPtr), prevOpId);
    }

    template<OutStreamOpT T, typename O>
    DataStream<T, O>::DataStream(std::unique_ptr<T> opPtr, operators::OperatorID leftOpId,
            operators::OperatorID rightOpId, StreamingJob& streamingJob)
        : streamingJob_(streamingJob)
    {
        static_assert(std::is_base_of_v<operators::DoubleInputOperator<decltype(opPtr->GetLeftInputType()),
                                                decltype(opPtr->GetRightInputType()), decltype(opPtr->GetOutputType())>,
                              T>,
                "Must pass a double input operator with matching types to create a data stream using this "
                "constructor!");
        currentOpId_ = opPtr->GetOperatorId();
        streamingJob_.AddOperator(std::move(opPtr), std::make_pair(leftOpId, rightOpId));
    }

    template<OutStreamOpT T, typename O>
    template<SingleInputOpT U>
    DataStream<U> DataStream<T, O>::AddOperator(std::unique_ptr<U> opPtr)
    {
        static_assert(std::is_same_v<O, decltype(opPtr->GetInputType())>,
                "Input type of operator does not match the output type of previous operator!");
        return DataStream<U>(std::move(opPtr), currentOpId_, streamingJob_);
    }

    template<OutStreamOpT T, typename O>
    template<DoubleInputOpT U, typename V>
    DataStream<U> DataStream<T, O>::AddJoinOperator(std::unique_ptr<U> opPtr, DataStream<V> rightStream)
    {
        static_assert(std::is_same_v<O, decltype(opPtr->GetLeftInputType())>,
                "Left input type of operator does not match the output type of previous left operator!");
        static_assert(std::is_same_v<decltype(std::declval<V>().GetOutputType()), decltype(opPtr->GetRightInputType())>,
                "Right input type of operator does not match the output type of previous right operator!");
        return DataStream<U>(std::move(opPtr), currentOpId_, rightStream.GetCurrentOperatorId(), streamingJob_);
    }

    template<OutStreamOpT T, typename O>
    template<typename Duration, SinkOpT<Duration> U>
    void DataStream<T, O>::AddSinkOperator(std::unique_ptr<U> opPtr)
    {
        static_assert(std::is_same_v<O, decltype(opPtr->GetInputType())>,
                "Input type of operator does not match the output type of previous operator!");
        streamingJob_.AddOperator(std::move(opPtr), currentOpId_);
    }

    template<OutStreamOpT T, typename O>
    template<SinkOpT<std::chrono::milliseconds> U>
    void DataStream<T, O>::AddSinkOperator(std::unique_ptr<U> opPtr)
    {
        static_assert(std::is_same_v<O, decltype(opPtr->GetInputType())>,
                "Input type of operator does not match the output type of previous operator!");
        streamingJob_.AddOperator(std::move(opPtr), currentOpId_);
    }

    template<OutStreamOpT T, typename O>
    operators::OperatorID DataStream<T, O>::GetCurrentOperatorId() const
    {
        return currentOpId_;
    }
}// namespace enjima::runtime
