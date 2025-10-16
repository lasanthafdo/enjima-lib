//
// Created by m34ferna on 03/05/24.
//

#ifndef ENJIMA_TEST_SETUP_HELPER_FUNCTIONS_H
#define ENJIMA_TEST_SETUP_HELPER_FUNCTIONS_H

#include "HelperFunctions.h"
#include "InputGenerationHelper.h"
#include "enjima/operators/FilterOperator.h"
#include "enjima/operators/FixedEventTimeWindowCoGroupOperator.h"
#include "enjima/operators/FixedEventTimeWindowJoinOperator.h"
#include "enjima/operators/FixedEventTimeWindowOperator.h"
#include "enjima/operators/GenericSinkOperator.h"
#include "enjima/operators/MapOperator.h"
#include "enjima/operators/SlidingEventTimeWindowOperator.h"
#include "enjima/operators/StaticEquiJoinOperator.h"
#include "enjima/runtime/DataStream.h"
#include "enjima/runtime/ExecutionEngine.h"
#include <fstream>
#include <gtest/gtest.h>
#include <iomanip>

// Multi query related functions

using JobT = enjima::runtime::StreamingJob;
using OpIDT = enjima::operators::OperatorID;
using namespace enjima::operators;

void PrintAveragedLatencyResultsForMultiQueryRun(enjima::metrics::Profiler* pProf, const std::string& sinkOpName,
        int numQueries = 1);
void PrintAveragedThroughputResultsForMultiQueryRun(enjima::metrics::Profiler* pProf, const std::string& srcOpName,
        int numQueries = 1);

JobT SetUpWindowlessPipelineForMultiQueryRun(enjima::runtime::ExecutionEngine* pExecEngine,
        const std::string& srcOpName, const std::string& projectOpName, const std::string& statEqJoinOpName,
        const std::string& sinkOpName, const std::string& jobIdSuffix = "1",
        enjima::runtime::StreamingTask::ProcessingMode processingMode =
                enjima::runtime::StreamingTask::ProcessingMode::kUnassigned);

void ValidatePipelineResultsForMultiQueryRun(bool hasWindow, bool isWindowJoin, enjima::metrics::Profiler* pProf,
        enjima::runtime::ExecutionEngine* pExecEngine, const std::string& srcOpName, const std::string& projectOpName,
        const std::string& statEqJoinOpName, const std::string& windowOpName, const std::string& windowJoinOpName,
        const std::string& rightWindowOpName, const std::string& sinkOpName, int numQueries = 1);

// Single query related functions
JobT SetUpBasicWindowlessPipelineNoAsserts(enjima::runtime::ExecutionEngine* pExecEngine, const std::string& srcOpName,
        const std::string& projectOpName, const std::string& statEqJoinOpName, const std::string& sinkOpName,
        enjima::runtime::StreamingTask::ProcessingMode processingMode =
                enjima::runtime::StreamingTask::ProcessingMode::kUnassigned);

template<typename Duration = std::chrono::milliseconds>
JobT SetUpBasicWindowlessPipeline(enjima::runtime::ExecutionEngine* pExecEngine, const std::string& srcOpName,
        const std::string& projectOpName, const std::string& statEqJoinOpName, const std::string& sinkOpName,
        enjima::runtime::StreamingTask::ProcessingMode processingMode =
                enjima::runtime::StreamingTask::ProcessingMode::kUnassigned);

template<typename Duration = std::chrono::milliseconds>
JobT SetUpBasicWindowedPipeline(enjima::runtime::ExecutionEngine* pExecEngine, const std::string& srcOpName,
        const std::string& projectOpName, const std::string& statEqJoinOpName, const std::string& windowOpName,
        const std::string& sinkOpName,
        enjima::runtime::StreamingTask::ProcessingMode processingMode =
                enjima::runtime::StreamingTask::ProcessingMode::kUnassigned);

template<typename Duration = std::chrono::milliseconds>
JobT SetUpFixedRateWindowedPipeline(enjima::runtime::ExecutionEngine* pExecEngine, const std::string& srcOpName,
        const std::string& projectOpName, const std::string& statEqJoinOpName, const std::string& windowOpName,
        const std::string& sinkOpName, uint64_t inputRate,
        enjima::runtime::StreamingTask::ProcessingMode processingMode =
                enjima::runtime::StreamingTask::ProcessingMode::kUnassigned);

template<typename Duration = std::chrono::milliseconds>
JobT SetUpFixedRateWindowedPipelineForMultiQueryRun(enjima::runtime::ExecutionEngine* pExecEngine,
        const std::string& srcOpName, const std::string& projectOpName, const std::string& statEqJoinOpName,
        const std::string& windowOpName, const std::string& sinkOpName, uint64_t inputRate,
        const std::string& jobIdSuffix = "1",
        enjima::runtime::StreamingTask::ProcessingMode processingMode =
                enjima::runtime::StreamingTask::ProcessingMode::kUnassigned,
        uint64_t reservoirCapacity = 10'000'000);

template<typename Duration = std::chrono::milliseconds>
JobT SetUpFixedRateYSBWindowedPipelineForMultiQueryRun(enjima::runtime::ExecutionEngine* pExecEngine,
        const std::string& srcOpName, const std::string& filterOpName, const std::string& projectOpName,
        const std::string& statEqJoinOpName, const std::string& windowOpName, const std::string& sinkOpName,
        uint64_t inputRate, const std::string& jobIdSuffix = "1",
        enjima::runtime::StreamingTask::ProcessingMode processingMode =
                enjima::runtime::StreamingTask::ProcessingMode::kUnassigned,
        uint64_t reservoirCapacity = 10'000'000);

template<typename Duration = std::chrono::milliseconds>
JobT SetUpFixedRateWindowJoinedPipelineForMultiQueryRun(enjima::runtime::ExecutionEngine* pExecEngine,
        const std::string& srcOpName, const std::string& projectOpName, const std::string& statEqJoinOpName,
        const std::string& windowOpName, const std::string& windowJoinOpName, const std::string& filterOpName,
        const std::string& filteredStatEqJoinOpName, const std::string& filteredWindowOpName,
        const std::string& sinkOpName, uint64_t inputRate, const std::string& jobIdSuffix = "1",
        enjima::runtime::StreamingTask::ProcessingMode processingMode =
                enjima::runtime::StreamingTask::ProcessingMode::kUnassigned,
        uint64_t reservoirCapacity = 10'000'000);

template<typename Duration = std::chrono::milliseconds>
JobT SetUpFixedRateWindowCoGroupedPipelineForMultiQueryRun(enjima::runtime::ExecutionEngine* pExecEngine,
        const std::string& srcOpName, const std::string& projectOpName, const std::string& statEqJoinOpName,
        const std::string& windowOpName, const std::string& windowCoGroupOpName, const std::string& filterOpName,
        const std::string& filteredStatEqJoinOpName, const std::string& filteredWindowOpName,
        const std::string& sinkOpName, uint64_t inputRate, const std::string& jobIdSuffix = "1",
        enjima::runtime::StreamingTask::ProcessingMode processingMode =
                enjima::runtime::StreamingTask::ProcessingMode::kUnassigned,
        uint64_t reservoirCapacity = 10'000'000);

template<typename Duration = std::chrono::milliseconds>
JobT SetUpFixedRateSlidingWindowedPipelineForMultiQueryRun(enjima::runtime::ExecutionEngine* pExecEngine,
        const std::string& srcOpName, const std::string& projectOpName, const std::string& statEqJoinOpName,
        const std::string& windowOpName, const std::string& sinkOpName, uint64_t inputRate,
        const std::string& jobIdSuffix = "1",
        enjima::runtime::StreamingTask::ProcessingMode processingMode =
                enjima::runtime::StreamingTask::ProcessingMode::kUnassigned,
        uint64_t reservoirCapacity = 10'000'000);

void ValidatePipelineResults(bool hasWindow, enjima::metrics::Profiler* pProf,
        enjima::runtime::ExecutionEngine* pExecEngine, const std::string& srcOpName, const std::string& projectOpName,
        const std::string& statEqJoinOpName, const std::string& windowOpName, const std::string& sinkOpName);

std::vector<std::string> GetLastLineAsTokenizedVectorFromFile(const std::string& filename,
        enjima::metrics::Profiler* pProf);

std::vector<std::string> GetTargetValuesAsTokenizedVectorFromFile(const std::string& filename,
        enjima::metrics::Profiler* pProf, const std::string& targetVal, size_t targetIdx, size_t metricNameIdx);

static inline std::string GetSuffixedOperatorName(const std::string& opName, const std::string& jobIdSuffix)
{
    return std::string(opName).append("_").append(jobIdSuffix);
}

#include "TestSetupHelperFunctions.tpp"

#endif//ENJIMA_TEST_SETUP_HELPER_FUNCTIONS_H
