//
// Created by m34ferna on 12/01/24.
//

#include "enjima/api/data_types/LinearRoadEvent.h"
#include "enjima/memory/MemoryUtil.h"
#include "enjima/operators/FilterOperator.h"
#include "enjima/runtime/DataStream.h"
#include "google-tests/common/TestSetupHelperFunctions.h"
#include "gtest/gtest.h"
#include <random>

using LinearRoadT = enjima::api::data_types::LinearRoadEvent;
using MemManT = enjima::memory::MemoryManager;
using EngineT = enjima::runtime::ExecutionEngine;
using ProflierT = enjima::metrics::Profiler;
using JobT = enjima::runtime::StreamingJob;
using LRRecordT = enjima::core::Record<LinearRoadT>;
using UniformIntDistParamT = std::uniform_int_distribution<int>::param_type;

class RoundRobinSchedulingTest : public testing::Test {
protected:
    void SetUp() override
    {
        memoryManager_ = new MemManT(maxMemory_, numBlocksPerChunk, numEventsPerBlock_, MemManT::AllocatorType::kBasic);
        profiler_ = new ProflierT(10000, true, "EnjimaRRSchedulingTest");
        executionEngine_ = new EngineT;
        executionEngine_->SetSchedulingPeriodMs(50);
        executionEngine_->SetPriorityType(enjima::runtime::PriorityType::kRoundRobin);
        executionEngine_->Init(memoryManager_, profiler_, enjima::runtime::SchedulingMode::kStateBasedPriority, 2,
                enjima::runtime::StreamingTask::ProcessingMode::kBlockBasedSingle,
                enjima::runtime::PreemptMode::kPreemptive);
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
    const int32_t numEventsPerBlock_ = 1024;

    const std::string srcOpName = "src";
    const std::string filterOpName = "filter";
    const std::string projectOpName = "project";
    const std::string statEqJoinOpName = "staticEqJoin";
    const std::string windowOpName = "timeWindow";
    const std::string sinkOpName = "genericSink";
};

TEST_F(RoundRobinSchedulingTest, WindowlessLinearPipelineNoAsserts)
{
    enjima::runtime::StreamingJob streamingJob = SetUpBasicWindowlessPipelineNoAsserts(executionEngine_, srcOpName,
            projectOpName, statEqJoinOpName, sinkOpName);
    auto jobId = executionEngine_->Submit(streamingJob);
    std::this_thread::sleep_for(std::chrono::seconds(10));
    executionEngine_->Cancel(jobId, std::chrono::seconds(10));

    ValidatePipelineResults(false, profiler_, executionEngine_, srcOpName, projectOpName, statEqJoinOpName,
            windowOpName, sinkOpName);
}

TEST_F(RoundRobinSchedulingTest, WindowlessLinearPipeline)
{
    enjima::runtime::StreamingJob streamingJob =
            SetUpBasicWindowlessPipeline(executionEngine_, srcOpName, projectOpName, statEqJoinOpName, sinkOpName);
    auto jobId = executionEngine_->Submit(streamingJob);
    std::this_thread::sleep_for(std::chrono::seconds(10));
    executionEngine_->Cancel(jobId, std::chrono::seconds(10));

    ValidatePipelineResults(false, profiler_, executionEngine_, srcOpName, projectOpName, statEqJoinOpName,
            windowOpName, sinkOpName);
}

TEST_F(RoundRobinSchedulingTest, WindowedLinearPipeline)
{
    enjima::runtime::StreamingJob streamingJob = SetUpBasicWindowedPipeline(executionEngine_, srcOpName, projectOpName,
            statEqJoinOpName, windowOpName, sinkOpName);
    auto jobId = executionEngine_->Submit(streamingJob);
    std::this_thread::sleep_for(std::chrono::seconds(30));
    executionEngine_->Cancel(jobId, std::chrono::seconds(10));

    ValidatePipelineResults(true, profiler_, executionEngine_, srcOpName, projectOpName, statEqJoinOpName, windowOpName,
            sinkOpName);
}