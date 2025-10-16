//
// Created by m34ferna on 15/03/24.
//

#ifndef ENJIMA_DATA_STREAM_H
#define ENJIMA_DATA_STREAM_H


#include "StreamingJob.h"
#include "enjima/operators/DoubleInputOperator.h"
#include "enjima/operators/SingleInputOperator.h"
#include "enjima/operators/SinkOperator.h"
#include "enjima/operators/SourceOperator.h"

namespace enjima::runtime {
    template<typename T, typename I = decltype(std::declval<T>().GetInputType()),
            typename O = decltype(std::declval<T>().GetOutputType())>
    concept SingleInputOpT = std::is_base_of_v<operators::SingleInputOperator<I, O>, T> ||
                             std::is_base_of_v<operators::SingleInputOperator<I, O, 2>, T>;

    template<typename T, typename L = decltype(std::declval<T>().GetLeftInputType()),
            typename R = decltype(std::declval<T>().GetRightInputType()),
            typename O = decltype(std::declval<T>().GetOutputType())>
    concept DoubleInputOpT = std::is_base_of_v<operators::DoubleInputOperator<L, R, O>, T>;

    template<typename T>
    inline constexpr bool is_single_input_op_t =
            std::is_base_of_v<operators::SingleInputOperator<decltype(std::declval<T>().GetInputType()),
                                      decltype(std::declval<T>().GetOutputType())>,
                    T> ||
            std::is_base_of_v<operators::SingleInputOperator<decltype(std::declval<T>().GetInputType()),
                                      decltype(std::declval<T>().GetOutputType()), 2>,
                    T>;

    template<typename T, typename Duration, typename I = decltype(std::declval<T>().GetInputType())>
    concept SinkOpT = std::is_base_of_v<operators::SinkOperator<I, Duration>, T>;

    template<typename T>
    concept OutStreamOpT =
            std::is_base_of_v<operators::StreamingOperator, T> && std::is_invocable_v<decltype(&T::GetOutputType), T>;

    template<OutStreamOpT T, typename O = decltype(std::declval<T>().GetOutputType())>
    class DataStream {
    public:
        DataStream(std::unique_ptr<T> opPtr, StreamingJob& streamingJob);
        DataStream(std::unique_ptr<T> opPtr, operators::OperatorID prevOpId, StreamingJob& streamingJob);
        DataStream(std::unique_ptr<T> opPtr, operators::OperatorID leftOpId, operators::OperatorID rightOpId,
                StreamingJob& streamingJob);

        template<SingleInputOpT U>
        DataStream<U> AddOperator(std::unique_ptr<U> opPtr);

        template<DoubleInputOpT U, typename V>
        DataStream<U> AddJoinOperator(std::unique_ptr<U> opPtr, DataStream<V> rightStream);

        template<typename Duration, SinkOpT<Duration> U>
        void AddSinkOperator(std::unique_ptr<U> opPtr);

        template<SinkOpT<std::chrono::milliseconds> U>
        void AddSinkOperator(std::unique_ptr<U> opPtr);

        [[nodiscard]] operators::OperatorID GetCurrentOperatorId() const;

    private:
        operators::OperatorID currentOpId_;
        StreamingJob& streamingJob_;
    };
}// namespace enjima::runtime

#include "DataStream.tpp"

#endif//ENJIMA_DATA_STREAM_H
