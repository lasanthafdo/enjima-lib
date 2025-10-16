//
// Created by m34ferna on 12/01/24.
//

#include "enjima/api/data_types/LinearRoadEvent.h"
#include "enjima/core/StreamingPipeline.h"
#include "enjima/memory/MemoryUtil.h"
#include "enjima/operators/FilterOperator.h"
#include "enjima/runtime/DataStream.h"
#include "enjima/runtime/scheduling/PriorityPrecedenceTracker.h"
#include "google-tests/common/TestSetupHelperFunctions.h"
#include "gtest/gtest.h"

using MemManT = enjima::memory::MemoryManager;
using EngineT = enjima::runtime::ExecutionEngine;
using ProflierT = enjima::metrics::Profiler;
using PreemptModeT = enjima::runtime::PreemptMode;
using PriorityModeT = enjima::runtime::PriorityType;
using SchedModeT = enjima::runtime::SchedulingMode;
using ProcModeT = enjima::runtime::StreamingTask::ProcessingMode;

class FixedRateGenerationTest : public testing::Test {
protected:
    void SetUp() override
    {
        memoryManager_ = new MemManT(maxMemory_, numBlocksPerChunk, numEventsPerBlock_, MemManT::AllocatorType::kBasic);
        memoryManager_->SetMaxActiveChunksPerOperator(100);
        profiler_ = new ProflierT(10000, true, "EnjimaFixedRateGenerationTest");
        executionEngine_ = new EngineT;
    }

    void TearDown() override
    {
        executionEngine_->Shutdown();
    }

    void StartEngineWithParams(uint64_t schedPeriodMs, SchedModeT schedMode, uint32_t numWorkers, ProcModeT procMode,
            PreemptModeT preemptMode, PriorityModeT priorityMode, uint64_t eventThreshold, uint64_t idleThresholdMs)
    {
        auto runtimeConfig = new enjima::runtime::RuntimeConfiguration("conf/enjima-config.yaml");
        std::string cpuListStr;
        switch (numWorkers) {
            case 2:
                cpuListStr = "2,3";
                break;
            case 3:
                cpuListStr = "2,3,4";
                break;
            case 4:
            default:
                cpuListStr = "2,3,4,5";
        }
        runtimeConfig->SetConfigAsString({"runtime", "workerCpuList"}, cpuListStr);
        runtimeConfig->SaveConfigToFile();

        executionEngine_->SetSchedulingPeriodMs(schedPeriodMs);
        executionEngine_->SetEventThreshold(eventThreshold);
        executionEngine_->SetIdleThresholdMs(idleThresholdMs);
        executionEngine_->SetPriorityType(priorityMode);
        executionEngine_->Init(memoryManager_, profiler_, schedMode, numWorkers, procMode, preemptMode);
        executionEngine_->Start();
    }

    MemManT* memoryManager_ = nullptr;
    EngineT* executionEngine_ = nullptr;
    ProflierT* profiler_ = nullptr;
    const size_t maxMemory_ = enjima::memory::MegaBytes(2048);
    const size_t numBlocksPerChunk = 48;
    const int32_t numEventsPerBlock_ = 1000;
    const uint64_t defaultMaxIdleThresholdMs_ = 1;
    const uint64_t defaultEventThreshold_ = 1000;
    const uint32_t defaultNumWorkers_ = 2;
    const uint64_t defaultInputRate_ = 30'000'000;

    const std::string srcOpName = "src";
    const std::string filterOpName = "filter";
    const std::string projectOpName = "project";
    const std::string statEqJoinOpName = "staticEqJoin";
    const std::string windowOpName = "timeWindow";
    const std::string sinkOpName = "genericSink";
};

TEST_F(FixedRateGenerationTest, AssertedWindowedLinearPipelineTB)
{
    StartEngineWithParams(10, enjima::runtime::SchedulingMode::kThreadBased, defaultNumWorkers_,
            enjima::runtime::StreamingTask::ProcessingMode::kUnassigned, enjima::runtime::PreemptMode::kNonPreemptive,
            PriorityModeT::kLatencyOptimized, defaultEventThreshold_, defaultMaxIdleThresholdMs_);
    enjima::runtime::StreamingJob streamingJob =
            SetUpFixedRateWindowedPipeline(executionEngine_, srcOpName, projectOpName, statEqJoinOpName, windowOpName,
                    sinkOpName, defaultInputRate_, enjima::runtime::StreamingTask::ProcessingMode::kBlockBasedSingle);
    auto jobId = executionEngine_->Submit(streamingJob);
    std::this_thread::sleep_for(std::chrono::seconds(30));
    executionEngine_->Cancel(jobId);

    ValidatePipelineResults(true, profiler_, executionEngine_, srcOpName, projectOpName, statEqJoinOpName, windowOpName,
            sinkOpName);
}

TEST_F(FixedRateGenerationTest, AssertedBatchedWindowedLinearPipelineTB)
{
    StartEngineWithParams(10, enjima::runtime::SchedulingMode::kThreadBased, defaultNumWorkers_,
            enjima::runtime::StreamingTask::ProcessingMode::kUnassigned, enjima::runtime::PreemptMode::kNonPreemptive,
            PriorityModeT::kLatencyOptimized, defaultEventThreshold_, defaultMaxIdleThresholdMs_);
    enjima::runtime::StreamingJob streamingJob =
            SetUpFixedRateWindowedPipeline(executionEngine_, srcOpName, projectOpName, statEqJoinOpName, windowOpName,
                    sinkOpName, defaultInputRate_, enjima::runtime::StreamingTask::ProcessingMode::kBlockBasedBatch);
    auto jobId = executionEngine_->Submit(streamingJob);
    std::this_thread::sleep_for(std::chrono::seconds(30));
    executionEngine_->Cancel(jobId);

    ValidatePipelineResults(true, profiler_, executionEngine_, srcOpName, projectOpName, statEqJoinOpName, windowOpName,
            sinkOpName);
}

TEST_F(FixedRateGenerationTest, AssertedBatchedWindowedLinearPipelineSBL)
{
    StartEngineWithParams(10, enjima::runtime::SchedulingMode::kStateBasedPriority, defaultNumWorkers_,
            enjima::runtime::StreamingTask::ProcessingMode::kBlockBasedBatch,
            enjima::runtime::PreemptMode::kNonPreemptive, PriorityModeT::kLatencyOptimized, defaultEventThreshold_,
            defaultMaxIdleThresholdMs_);
    enjima::runtime::StreamingJob streamingJob = SetUpFixedRateWindowedPipeline(executionEngine_, srcOpName,
            projectOpName, statEqJoinOpName, windowOpName, sinkOpName, defaultInputRate_);
    auto jobId = executionEngine_->Submit(streamingJob);
    std::this_thread::sleep_for(std::chrono::seconds(30));
    executionEngine_->Cancel(jobId);

    ValidatePipelineResults(true, profiler_, executionEngine_, srcOpName, projectOpName, statEqJoinOpName, windowOpName,
            sinkOpName);
}

TEST_F(FixedRateGenerationTest, AssertedBatchedWindowedLinearPipelineSBA)
{
    StartEngineWithParams(10, enjima::runtime::SchedulingMode::kStateBasedPriority, defaultNumWorkers_,
            enjima::runtime::StreamingTask::ProcessingMode::kBlockBasedBatch,
            enjima::runtime::PreemptMode::kNonPreemptive, PriorityModeT::kAdaptive, defaultEventThreshold_,
            defaultMaxIdleThresholdMs_);
    enjima::runtime::StreamingJob streamingJob = SetUpFixedRateWindowedPipeline(executionEngine_, srcOpName,
            projectOpName, statEqJoinOpName, windowOpName, sinkOpName, defaultInputRate_);
    auto jobId = executionEngine_->Submit(streamingJob);
    std::this_thread::sleep_for(std::chrono::seconds(30));
    executionEngine_->Cancel(jobId);

    ValidatePipelineResults(true, profiler_, executionEngine_, srcOpName, projectOpName, statEqJoinOpName, windowOpName,
            sinkOpName);
}