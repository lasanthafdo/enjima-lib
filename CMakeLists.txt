cmake_minimum_required(VERSION 3.16)
project(Enjima)

set(CMAKE_CXX_STANDARD 20)

include_directories(
        ./
        ./include
)

add_subdirectory(google-tests)

set(TBB_TEST CACHE INTERNAL OFF)

include(FetchContent)
FetchContent_Declare(
        tbb
        GIT_REPOSITORY https://github.com/oneapi-src/oneTBB.git
        GIT_TAG v2021.12.0
        GIT_SHALLOW ON
)
FetchContent_MakeAvailable(tbb)

FetchContent_Declare(
        yaml-cpp
        GIT_REPOSITORY https://github.com/jbeder/yaml-cpp.git
        GIT_TAG 0.8.0 # Can be a tag (yaml-cpp-x.x.x), a commit hash, or a branch name (master)
        GIT_SHALLOW ON
)
FetchContent_MakeAvailable(yaml-cpp)

# For GCC
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror -Wno-error=stringop-overflow -fPIE -pie -pthread -march=native")

if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    if (CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL "12.0")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --param destructive-interference-size=64")
    endif()
endif()

# For Clang
#set(CMAKE_CXX_COMPILER "clang++-18")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror -fPIE -pthread")

#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread -O2")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=undefined -O2")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=shift -O2")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fno-omit-frame-pointer")

# set(CMAKE_CXX_FLAGS_DEBUG "-g")
# set(CMAKE_CXX_FLAGS_RELEASE "-O3")

# When you want your debug build program to go faster
#set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O2")

set(CMAKE_DEBUG_POSTFIX Debug)
set(CMAKE_RELEASE_POSTFIX Release)
set(CMAKE_RELWITHDEBINFO_POSTFIX RelWithDebInfo)

# Please note that this compile definition needs to be specified for the entire build correctly.
# Otherwise, there can be weird runtime issues (e.g., sysmalloc assert failure in std::vector::push_back())
add_compile_definitions(ENJIMA_METRICS_LEVEL=0)
add_compile_definitions(ENJIMA_QUEUE_BASED_Q_TYPE=0)
add_compile_definitions(ENJIMA_MEM_CPY_METHOD=0)
add_compile_definitions(ENJIMA_LATENCY_GRAD_TYPE=0)

add_compile_definitions(ENJIMA_BUFFER_ALIGNMENT=128)
# add_compile_definitions(ENJIMA_PRINT_SCHED_INFO)
add_compile_definitions(ENJIMA_ADAPT_PARAMETERS)
# add_compile_definitions(ENJIMA_USE_NORMALIZED_OUTPUT_COST)

set(HEADER_FILES
        enjima/core/Record.h
        enjima/core/Record.tpp
        enjima/core/InstanceID.h
        enjima/core/StreamingPipeline.h
        enjima/core/ExecutionPlan.h
        enjima/core/JobHandler.h
        enjima/core/OutputCollector.h
        enjima/core/CoreInternals.fwd.h
        enjima/core/CoreTypeAliases.h

        enjima/memory/MemoryBlock.h
        enjima/memory/MemoryManager.h
        enjima/memory/MemoryAllocator.h
        enjima/memory/AdaptiveMemoryAllocator.h
        enjima/memory/BasicMemoryAllocator.h
        enjima/memory/MemoryChunk.h
        enjima/memory/BadMemoryOperation.h
        enjima/memory/MemoryUtil.h
        enjima/memory/MemoryManager.tpp
        enjima/memory/MemoryBlock.tpp
        enjima/memory/MemoryAllocationCoordinator.h
        enjima/memory/MemoryTypeAliases.h
        enjima/memory/MemoryInternals.fwd.h
        enjima/memory/MemoryManager.fwd.h

        enjima/operators/StreamingOperator.h
        enjima/operators/SingleInputOperator.h
        enjima/operators/SourceOperator.h
        enjima/operators/NoOpSinkOperator.h
        enjima/operators/OperatorsTypeAliases.h
        enjima/operators/FilterOperator.tpp
        enjima/operators/FilterOperator.h
        enjima/operators/OperatorUtil.h
        enjima/operators/LatencyTrackingSourceOperator.h
        enjima/operators/LatencyTrackingSourceOperator.tpp
        enjima/operators/MapOperator.tpp
        enjima/operators/MapOperator.h
        enjima/operators/FixedEventTimeWindowOperator.tpp
        enjima/operators/FixedEventTimeWindowOperator.h
        enjima/operators/StaticJoinOperator.h
        enjima/operators/StaticJoinOperator.tpp
        enjima/operators/StaticEquiJoinOperator.h
        enjima/operators/StaticEquiJoinOperator.tpp
        enjima/operators/OperatorsInternals.fwd.h
        enjima/operators/GenericSinkOperator.h
        enjima/operators/GenericSinkOperator.tpp
        enjima/queueing/RecordQueueBase.h
        enjima/queueing/RecordQueueImpl.h
        enjima/queueing/RecordQueueImpl.tpp

        enjima/api/SinkFunction.h
        enjima/api/data_types/LinearRoadEvent.h
        enjima/api/data_types/YSBAdEvent.h
        enjima/api/MapFunction.h
        enjima/api/AggregateFunction.h
        enjima/api/KeyedAggregateFunction.h
        enjima/api/NonKeyedAggregateFunction.h
        enjima/api/JoinPredicate.h
        enjima/api/JoinFunction.h
        enjima/api/KeyExtractionFunction.h

        enjima/metrics/Profiler.h
        enjima/metrics/types/Counter.h
        enjima/metrics/types/Gauge.h
        enjima/metrics/types/Gauge.tpp
        enjima/metrics/MetricNames.h
        enjima/metrics/types/Counter.tpp
        enjima/metrics/PeriodicMetricsLogger.h
        enjima/metrics/types/ThroughputGauge.h
        enjima/metrics/MetricsInternals.fwd.h
        enjima/metrics/types/Histogram.h
        enjima/metrics/types/Histogram.tpp
        enjima/metrics/types/HistogramValue.h
        enjima/metrics/SystemMetricsProfiler.h
        enjima/metrics/types/SystemGauge.h
        enjima/metrics/types/SystemGauge.tpp
        enjima/metrics/types/PendingInputEventsGauge.h
        enjima/metrics/types/OperatorCostGauge.h
        enjima/metrics/types/OperatorSelectivityGauge.h
        enjima/metrics/types/MetricTypes.fwd.h
        enjima/metrics/types/CpuGauge.h
        enjima/metrics/types/OperatorCpuTimeGauge.h
        enjima/metrics/types/OperatorScheduledCountGauge.h
        enjima/metrics/types/DoubleAverageGauge.h

        enjima/common/TypeAliases.h

        enjima/runtime/ExecutionEngine.h
        enjima/runtime/StreamingJob.h
        enjima/runtime/scheduling/StateBasedTask.h
        enjima/runtime/scheduling/Scheduler.h
        enjima/runtime/RuntimeUtil.h
        enjima/runtime/scheduling/ThreadBasedTask.h
        enjima/runtime/StreamingTask.h
        enjima/runtime/DataStream.tpp
        enjima/runtime/DataStream.h
        enjima/runtime/scheduling/PriorityPrecedenceTracker.h
        enjima/runtime/scheduling/OperatorContext.h
        enjima/runtime/scheduling/StateBasedTaskRunner.h
        enjima/runtime/scheduling/StateBasedTaskRunner.tpp
        enjima/runtime/JobValidationException.h
        enjima/runtime/scheduling/PriorityScheduler.h
        enjima/runtime/InitializationException.h
        enjima/runtime/scheduling/SchedulingContext.h
        enjima/runtime/scheduling/OperatorPrecedenceTracker.h
        enjima/runtime/scheduling/OperatorPrecedenceTracker.tpp
        enjima/runtime/scheduling/PriorityPrecedenceTracker.tpp
        enjima/runtime/IllegalStateException.h
        enjima/runtime/AlreadyExistsException.h
        enjima/runtime/RuntimeInternals.fwd.h
        enjima/runtime/scheduling/SchedulingDecisionContext.h
        enjima/runtime/scheduling/SchedulingTypes.h
        enjima/runtime/RuntimeUtil.tpp
        enjima/runtime/scheduling/SchedulingInternals.fwd.h
        enjima/runtime/scheduling/policy/SchedulingPolicy.h
        enjima/runtime/scheduling/policy/AdaptivePriorityCalculator.h
        enjima/runtime/scheduling/policy/InputSizeBasedPriorityCalculator.h
        enjima/runtime/scheduling/policy/LatencyOptimizedPriorityCalculator.h
        enjima/runtime/scheduling/ThreadBasedTaskRunner.tpp
        enjima/runtime/scheduling/ThreadBasedTaskRunner.h
        enjima/runtime/scheduling/policy/SchedulingPreemptMode.h
        enjima/runtime/scheduling/policy/NonPreemptiveMode.h
        enjima/runtime/scheduling/policy/PreemptiveMode.h
        enjima/runtime/RuntimeConfiguration.h
        enjima/runtime/ExecutionEngine.fwd.h
        enjima/runtime/RuntimeTypes.h
        enjima/runtime/scheduling/policy/NonPreemptiveBasicMode.h
        enjima/runtime/scheduling/InternalSchedulingTypes.fwd.h
        enjima/runtime/scheduling/policy/ThroughputOptimizedPriorityCalculator.h
        enjima/runtime/scheduling/policy/NonPreemptiveThroughputOptimizedMode.h
        enjima/runtime/scheduling/policy/SimpleThroughputPriorityCalculator.h
        enjima/runtime/scheduling/PreemptivePriorityScheduler.tpp
        enjima/runtime/scheduling/policy/LeastRecentOperatorPriorityCalculator.h
        enjima/runtime/scheduling/policy/MetricsBasedPriorityCalculator.h
        enjima/runtime/IllegalArgumentException.h
        enjima/runtime/scheduling/policy/SPLatencyOptimizedPriorityCalculator.h
        enjima/runtime/scheduling/SchedulingQueue.h
        enjima/runtime/ConfigurationException.h
        enjima/operators/FixedEventTimeWindowJoinOperator.h
        enjima/operators/FixedEventTimeWindowJoinOperator.tpp
        enjima/operators/DoubleInputOperator.h
        enjima/operators/DoubleInputOperator.tpp
        enjima/operators/SingleInputDoubleOutOperator.tpp
        enjima/operators/StaticEquiJoinDoubleOutOperator.tpp
        enjima/operators/MapDoubleOutOperator.tpp
        enjima/operators/SlidingEventTimeWindowOperator.h
        enjima/operators/SlidingEventTimeWindowOperator.tpp
        enjima/api/KeyedOrNonKeyedAggregateFunction.h
        enjima/api/MergeableKeyedAggregateFunction.h
        enjima/operators/FixedEventTimeWindowCoGroupOperator.tpp
        enjima/operators/FixedEventTimeWindowCoGroupOperator.h
        enjima/api/CoGroupFunction.h
        enjima/operators/SingleInputSingleOutOperator.tpp
        enjima/operators/MapSingleOutOperator.tpp
        enjima/core/InstanceID.tpp
        enjima/runtime/scheduling/policy/HighestRatePriorityCalculator.cpp
        enjima/runtime/scheduling/policy/MinLatencyPriorityCalculator.h
        enjima/runtime/scheduling/policy/MinCostPriorityCalculator.h
        enjima/runtime/scheduling/policy/RoundRobinPriorityCalculator.h
        enjima/runtime/scheduling/policy/FCFSPriorityCalculator.h
        enjima/runtime/CancellationException.h
        enjima/queueing/VyukovUnboundedSPSCQueue.h
        enjima/metrics/types/LatencyGradientGauge.h
        enjima/runtime/scheduling/policy/AdaptivePipelineContext.h
)

set(SOURCE_FILES
        enjima/core/StreamingPipeline.cpp
        enjima/core/ExecutionPlan.cpp
        enjima/core/JobHandler.cpp
        enjima/core/OutputCollector.cpp

        enjima/memory/MemoryBlock.cpp
        enjima/memory/MemoryManager.cpp
        enjima/memory/AdaptiveMemoryAllocator.cpp
        enjima/memory/BasicMemoryAllocator.cpp
        enjima/memory/MemoryChunk.cpp
        enjima/memory/MemoryAllocationCoordinator.cpp
        enjima/memory/MemoryUtil.cpp

        enjima/operators/StreamingOperator.cpp

        enjima/metrics/Profiler.cpp
        enjima/metrics/types/Counter.cpp
        enjima/metrics/PeriodicMetricsLogger.cpp
        enjima/metrics/types/ThroughputGauge.cpp
        enjima/metrics/SystemMetricsProfiler.cpp
        enjima/metrics/types/PendingInputEventsGauge.cpp
        enjima/metrics/types/OperatorCostGauge.cpp
        enjima/metrics/types/OperatorSelectivityGauge.cpp
        enjima/metrics/types/CpuGauge.cpp
        enjima/metrics/types/OperatorCpuTimeGauge.cpp
        enjima/metrics/types/OperatorScheduledCountGauge.cpp
        enjima/metrics/types/DoubleAverageGauge.cpp

        enjima/runtime/ExecutionEngine.cpp
        enjima/runtime/StreamingJob.cpp
        enjima/runtime/scheduling/StateBasedTask.cpp
        enjima/runtime/scheduling/Scheduler.cpp
        enjima/runtime/scheduling/ThreadBasedTask.cpp
        enjima/runtime/RuntimeUtil.cpp
        enjima/runtime/scheduling/OperatorContext.cpp
        enjima/runtime/scheduling/PriorityScheduler.tpp
        enjima/runtime/scheduling/SchedulingContext.cpp
        enjima/runtime/scheduling/SchedulingDecisionContext.cpp
        enjima/runtime/StreamingTask.cpp
        enjima/runtime/scheduling/PriorityPrecedenceTracker.cpp
        enjima/runtime/scheduling/policy/AdaptivePriorityCalculator.cpp
        enjima/runtime/scheduling/policy/InputSizeBasedPriorityCalculator.cpp
        enjima/runtime/scheduling/policy/LatencyOptimizedPriorityCalculator.cpp
        enjima/runtime/scheduling/policy/NonPreemptiveMode.cpp
        enjima/runtime/scheduling/policy/PreemptiveMode.cpp
        enjima/runtime/RuntimeConfiguration.cpp
        enjima/runtime/scheduling/policy/NonPreemptiveBasicMode.cpp
        enjima/runtime/scheduling/policy/ThroughputOptimizedPriorityCalculator.cpp
        enjima/runtime/scheduling/policy/NonPreemptiveThroughputOptimizedMode.cpp
        enjima/runtime/scheduling/policy/SimpleThroughputPriorityCalculator.cpp
        enjima/runtime/scheduling/policy/LeastRecentOperatorPriorityCalculator.cpp
        enjima/runtime/scheduling/policy/SPLatencyOptimizedPriorityCalculator.cpp
        enjima/runtime/scheduling/policy/SchedulingPolicy.cpp
        enjima/runtime/scheduling/SchedulingQueue.cpp
        enjima/runtime/scheduling/policy/HighestRatePriorityCalculator.cpp
        enjima/runtime/scheduling/policy/MinLatencyPriorityCalculator.cpp
        enjima/runtime/scheduling/policy/MinCostPriorityCalculator.cpp
        enjima/runtime/scheduling/policy/RoundRobinPriorityCalculator.cpp
        enjima/runtime/scheduling/policy/FCFSPriorityCalculator.cpp
        enjima/metrics/types/LatencyGradientGauge.cpp
        enjima/runtime/scheduling/policy/AdaptivePipelineContext.cpp
)

add_library(Enjima STATIC ${SOURCE_FILES} ${HEADER_FILES})

# target_compile_definitions(Enjima PUBLIC ENJIMA_WORKER_PRIORITY_UPDATES_DISABLED)
target_compile_definitions(Enjima PUBLIC ENABLE_THREAD_PARKING)

set_target_properties(Enjima PROPERTIES DEBUG_POSTFIX ${CMAKE_DEBUG_POSTFIX}
        RELWITHDEBINFO_POSTFIX ${CMAKE_RELWITHDEBINFO_POSTFIX} RELEASE_POSTFIX ${CMAKE_RELEASE_POSTFIX})

target_link_libraries(Enjima TBB::tbb yaml-cpp::yaml-cpp)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
