//
// Created by m34ferna on 03/05/24.
//

#include "enjima/memory/MemoryManager.h"
#include "enjima/memory/MemoryUtil.h"
#include "enjima/runtime/ExecutionEngine.h"
#include "enjima/runtime/StreamingJob.h"
#include "google-tests/common/TestSetupHelperFunctions.h"

#include <fstream>
#include <gtest/gtest.h>

using MemManT = enjima::memory::MemoryManager;
using EngineT = enjima::runtime::ExecutionEngine;
using ProflierT = enjima::metrics::Profiler;
using JobT = enjima::runtime::StreamingJob;

class MetricLoggingTest : public testing::Test {
protected:
    void SetUp() override
    {
        memoryManager_ = new MemManT(maxMemory_, numBlocksPerChunk, numEventsPerBlock_, MemManT::AllocatorType::kBasic);
        profiler_ = new ProflierT(10000, true, "EnjimaMetricLoggingTest");
        executionEngine_ = new EngineT;
        executionEngine_->Init(memoryManager_, profiler_);
        executionEngine_->Start();
    }

    void TearDown() override
    {
        executionEngine_->Shutdown();
    }

    MemManT* memoryManager_ = nullptr;
    EngineT* executionEngine_ = nullptr;
    ProflierT* profiler_ = nullptr;
    const size_t maxMemory_ = enjima::memory::MegaBytes(512);
    const size_t numBlocksPerChunk = 48;
    const int32_t numEventsPerBlock_ = 1000;

    const std::string srcOpName = "src";
    const std::string filterOpName = "filter";
    const std::string projectOpName = "project";
    const std::string statEqJoinOpName = "staticEqJoin";
    const std::string windowOpName = "timeWindow";
    const std::string sinkOpName = "genericSink";
};

TEST_F(MetricLoggingTest, MetricFileValidityMillis)
{
    enjima::runtime::StreamingJob streamingJob =
            SetUpBasicWindowlessPipeline(executionEngine_, srcOpName, projectOpName, statEqJoinOpName, sinkOpName,
                    enjima::runtime::StreamingTask::ProcessingMode::kBlockBasedBatch);
    auto jobId = executionEngine_->Submit(streamingJob);
    std::this_thread::sleep_for(std::chrono::seconds(30));
    executionEngine_->Cancel(jobId);
    ValidatePipelineResults(false, profiler_, executionEngine_, srcOpName, projectOpName, statEqJoinOpName,
            windowOpName, sinkOpName);
    auto tokenizedStrVec = GetLastLineAsTokenizedVectorFromFile("metrics/latency.csv", profiler_);
    EXPECT_STREQ(tokenizedStrVec[3].c_str(),
            std::string(sinkOpName).append(enjima::metrics::kLatencyHistogramSuffix).c_str());
    EXPECT_LT(std::stod(tokenizedStrVec[4]), 10'000);
    EXPECT_LT(std::stoul(tokenizedStrVec[5]), 10'000);

    tokenizedStrVec = GetLastLineAsTokenizedVectorFromFile("metrics/throughput.csv", profiler_);
    EXPECT_STREQ(tokenizedStrVec[3].c_str(),
            std::string(sinkOpName).append(enjima::metrics::kInThroughputGaugeSuffix).c_str());
    EXPECT_GT(std::stod(tokenizedStrVec[4]), 1'000'000);

    tokenizedStrVec = GetLastLineAsTokenizedVectorFromFile("metrics/event_count.csv", profiler_);
    EXPECT_STREQ(tokenizedStrVec[3].c_str(),
            std::string(statEqJoinOpName).append(enjima::metrics::kOutCounterSuffix).c_str());
    EXPECT_GT(std::stod(tokenizedStrVec[4]), 20'000'000);
}

TEST_F(MetricLoggingTest, MetricFileValidityMicros)
{
    enjima::runtime::StreamingJob streamingJob =
            SetUpBasicWindowlessPipeline<std::chrono::microseconds>(executionEngine_, srcOpName, projectOpName,
                    statEqJoinOpName, sinkOpName, enjima::runtime::StreamingTask::ProcessingMode::kBlockBasedBatch);
    auto jobId = executionEngine_->Submit(streamingJob);
    std::this_thread::sleep_for(std::chrono::seconds(30));
    executionEngine_->Cancel(jobId);
    ValidatePipelineResults(false, profiler_, executionEngine_, srcOpName, projectOpName, statEqJoinOpName,
            windowOpName, sinkOpName);
    auto tokenizedStrVec = GetLastLineAsTokenizedVectorFromFile("metrics/latency.csv", profiler_);
    EXPECT_STREQ(tokenizedStrVec[3].c_str(),
            std::string(sinkOpName).append(enjima::metrics::kLatencyHistogramSuffix).c_str());
    EXPECT_LT(std::stod(tokenizedStrVec[4]), 10'000'000);
    EXPECT_LT(std::stoul(tokenizedStrVec[5]), 10'000'000);

    tokenizedStrVec = GetLastLineAsTokenizedVectorFromFile("metrics/throughput.csv", profiler_);
    EXPECT_STREQ(tokenizedStrVec[3].c_str(),
            std::string(sinkOpName).append(enjima::metrics::kInThroughputGaugeSuffix).c_str());
    EXPECT_GT(std::stod(tokenizedStrVec[4]), 1'000'000);

    tokenizedStrVec = GetLastLineAsTokenizedVectorFromFile("metrics/event_count.csv", profiler_);
    EXPECT_STREQ(tokenizedStrVec[3].c_str(),
            std::string(statEqJoinOpName).append(enjima::metrics::kOutCounterSuffix).c_str());
    EXPECT_GT(std::stod(tokenizedStrVec[4]), 20'000'000);
}

TEST_F(MetricLoggingTest, MetricFileValidityMicrosBlockBasedSingle)
{
    enjima::runtime::StreamingJob streamingJob =
            SetUpBasicWindowlessPipeline<std::chrono::microseconds>(executionEngine_, srcOpName, projectOpName,
                    statEqJoinOpName, sinkOpName, enjima::runtime::StreamingTask::ProcessingMode::kBlockBasedSingle);
    auto jobId = executionEngine_->Submit(streamingJob);
    std::this_thread::sleep_for(std::chrono::seconds(30));
    executionEngine_->Cancel(jobId);
    ValidatePipelineResults(false, profiler_, executionEngine_, srcOpName, projectOpName, statEqJoinOpName,
            windowOpName, sinkOpName);
    auto tokenizedStrVec = GetLastLineAsTokenizedVectorFromFile("metrics/latency.csv", profiler_);
    EXPECT_STREQ(tokenizedStrVec[3].c_str(),
            std::string(sinkOpName).append(enjima::metrics::kLatencyHistogramSuffix).c_str());
    EXPECT_LT(std::stod(tokenizedStrVec[4]), 10'000'000);
    EXPECT_LT(std::stoul(tokenizedStrVec[5]), 10'000'000);

    tokenizedStrVec = GetLastLineAsTokenizedVectorFromFile("metrics/throughput.csv", profiler_);
    EXPECT_STREQ(tokenizedStrVec[3].c_str(),
            std::string(sinkOpName).append(enjima::metrics::kInThroughputGaugeSuffix).c_str());
    EXPECT_GT(std::stod(tokenizedStrVec[4]), 1'000'000);

    tokenizedStrVec = GetLastLineAsTokenizedVectorFromFile("metrics/event_count.csv", profiler_);
    EXPECT_STREQ(tokenizedStrVec[3].c_str(),
            std::string(statEqJoinOpName).append(enjima::metrics::kOutCounterSuffix).c_str());
    EXPECT_GT(std::stod(tokenizedStrVec[4]), 20'000'000);
}