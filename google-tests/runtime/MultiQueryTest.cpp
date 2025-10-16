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
#include <random>

using LinearRoadT = enjima::api::data_types::LinearRoadEvent;
using MemManT = enjima::memory::MemoryManager;
using EngineT = enjima::runtime::ExecutionEngine;
using ProflierT = enjima::metrics::Profiler;
using JobT = enjima::runtime::StreamingJob;
using LRRecordT = enjima::core::Record<LinearRoadT>;
using UniformIntDistParamT = std::uniform_int_distribution<int>::param_type;
using PreemptModeT = enjima::runtime::PreemptMode;
using PriorityModeT = enjima::runtime::PriorityType;
using SchedModeT = enjima::runtime::SchedulingMode;
using ProcModeT = enjima::runtime::StreamingTask::ProcessingMode;

class MultiQueryTest : public testing::Test {
protected:
    void SetUp() override
    {
        memoryManager_ =
                new MemManT(maxMemory_, numBlocksPerChunk_, numEventsPerBlock_, MemManT::AllocatorType::kBasic);
        memoryManager_->SetMaxActiveChunksPerOperator(100);
        profiler_ = new ProflierT(1000, true, "EnjimaPRSchedulingTest");
        executionEngine_ = new EngineT;
    }

    void TearDown() override
    {
        executionEngine_->Shutdown();
        delete executionEngine_;
    }

    void StartEngineWithParams(uint64_t schedPeriodMs, SchedModeT schedMode, uint32_t numWorkers = 3,
            ProcModeT procMode = ProcModeT::kBlockBasedSingle, PreemptModeT preemptMode = PreemptModeT::kNonPreemptive,
            PriorityModeT priorityMode = PriorityModeT::kAdaptive, uint64_t eventThreshold = 1000,
            uint64_t idleThresholdMs = 1)
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
        delete runtimeConfig;

        executionEngine_->SetSchedulingPeriodMs(schedPeriodMs);
        executionEngine_->SetEventThreshold(eventThreshold);
        executionEngine_->SetIdleThresholdMs(idleThresholdMs);
        executionEngine_->SetPriorityType(priorityMode);
        executionEngine_->Init(memoryManager_, profiler_, schedMode, numWorkers, procMode, preemptMode);
        executionEngine_->Start();
    }

    static void SetSchedulingMode(SchedModeT* schedMode)
    {
        std::string inputStr;
        std::cout << "Scheduling Mode (TB, SBRoundRobin, SBPriority) [SBPriority]: ";
        std::getline(std::cin, inputStr);
        if (inputStr == "SBPriority" || inputStr.empty()) {
            *schedMode = SchedModeT::kStateBasedPriority;
        }
        else if (inputStr == "TB") {
            *schedMode = SchedModeT::kThreadBased;
        }
        else {
            std::cout << "Scheduling mode invalid!" << std::endl;
        }
    }

    static void SetProcessingMode(ProcModeT* procMode)
    {
        std::string inputStr;
        std::cout << "Processing Mode (BlockBasedSingle, BlockBasedBatch, QueueBasedSingle) [BlockBasedBatch]: ";
        std::getline(std::cin, inputStr);
        if (inputStr == "QueueBasedSingle") {
            *procMode = ProcModeT::kQueueBasedSingle;
        }
        else if (inputStr == "BlockBasedSingle") {
            *procMode = ProcModeT::kBlockBasedSingle;
        }
        else if (inputStr == "BlockBasedBatch" || inputStr.empty()) {
            *procMode = ProcModeT::kBlockBasedBatch;
        }
        else {
            std::cout << "Processing mode invalid!" << std::endl;
        }
    }

    void SetNumQueries(int* numQueries) const
    {
        std::string inputStr;
        std::cout << "Num queries (int) [" << defaultNumQueries_ << "]: ";
        std::getline(std::cin, inputStr);
        if (!inputStr.empty()) {
            *numQueries = std::stoi(inputStr);
        }
    }

    void SetNumWorkers(uint32_t* numWorkers) const
    {
        std::string inputStr;
        std::cout << "Num workers (unsigned int) [" << defaultNumWorkers_ << "]: ";
        std::getline(std::cin, inputStr);
        if (!inputStr.empty()) {
            *numWorkers = std::stoi(inputStr);
        }
    }

    void SetInputRate(uint64_t* inputRate) const
    {
        std::string inputStr;
        std::cout << "Input rate (unsigned long) [" << defaultInputRate_ << "]: ";
        std::getline(std::cin, inputStr);
        if (!inputStr.empty()) {
            *inputRate = std::stoul(inputStr);
        }
    }

    MemManT* memoryManager_ = nullptr;
    EngineT* executionEngine_ = nullptr;
    ProflierT* profiler_ = nullptr;
    const size_t maxMemory_ = enjima::memory::MegaBytes(4096);
    const size_t numBlocksPerChunk_ = 48;
    const int32_t numEventsPerBlock_ = 1024;
    const uint64_t defaultMaxIdleThresholdMs_ = 1;
    const uint64_t defaultEventThreshold_ = 1000;
    const uint32_t defaultNumWorkers_ = 4;
    const uint64_t defaultInputRate_ = 30'000'000;
    const int defaultNumQueries_ = 2;

    const std::string srcOpName = "src";
    const std::string filterOpName = "filter";
    const std::string projectOpName = "project";
    const std::string statEqJoinOpName = "staticEqJoin";
    const std::string windowOpName = "timeWindow";
    const std::string slidingWindowOpName = "slidingTimeWindow";
    const std::string windowJoinOpName = "timeWindowJoin";
    const std::string windowCoGroupOpName = "timeWindowCoGroup";
    const std::string filteredStatEqJoinOpName = "filteredStaticEqJoin";
    const std::string filteredWindowOpName = "filteredTimeWindow";
    const std::string sinkOpName = "genericSink";
};

TEST_F(MultiQueryTest, MultiQueryWindowlessLinearPipelineTB)
{
    StartEngineWithParams(50, SchedModeT::kThreadBased, 3);
    auto numJobs = 2;
    std::vector<enjima::core::JobID> jobIds;
    for (int i = 0; i < numJobs; i++) {
        enjima::runtime::StreamingJob streamingJob = SetUpWindowlessPipelineForMultiQueryRun(executionEngine_,
                srcOpName, projectOpName, statEqJoinOpName, sinkOpName, std::to_string(i + 1));
        auto jobId = executionEngine_->Submit(streamingJob);
        jobIds.emplace_back(jobId);
    }
    std::this_thread::sleep_for(std::chrono::seconds(10));
    for (const auto& jobId: jobIds) {
        executionEngine_->Cancel(jobId, std::chrono::seconds(10));
    }

    ValidatePipelineResultsForMultiQueryRun(false, false, profiler_, executionEngine_, srcOpName, projectOpName,
            statEqJoinOpName, windowOpName, windowJoinOpName, filteredWindowOpName, sinkOpName, numJobs);
}

TEST_F(MultiQueryTest, MultiQueryWindowlessLinearPipelineSBA)
{
    StartEngineWithParams(50, SchedModeT::kStateBasedPriority, 4, ProcModeT::kBlockBasedSingle,
            PreemptModeT::kNonPreemptive, PriorityModeT::kAdaptive, defaultEventThreshold_);
    auto numJobs = 2;
    std::vector<enjima::core::JobID> jobIds;
    for (int i = 0; i < numJobs; i++) {
        enjima::runtime::StreamingJob streamingJob = SetUpWindowlessPipelineForMultiQueryRun(executionEngine_,
                srcOpName, projectOpName, statEqJoinOpName, sinkOpName, std::to_string(i + 1));
        auto jobId = executionEngine_->Submit(streamingJob);
        jobIds.emplace_back(jobId);
    }
    std::this_thread::sleep_for(std::chrono::seconds(10));
    for (const auto& jobId: jobIds) {
        executionEngine_->Cancel(jobId, std::chrono::seconds(10));
    }

    ValidatePipelineResultsForMultiQueryRun(false, false, profiler_, executionEngine_, srcOpName, projectOpName,
            statEqJoinOpName, windowOpName, windowJoinOpName, filteredWindowOpName, sinkOpName, numJobs);
}

TEST_F(MultiQueryTest, MultiQueryWindowlessLinearPipelinePreemptiveSBA)
{
    StartEngineWithParams(50, SchedModeT::kStateBasedPriority, 4, ProcModeT::kBlockBasedSingle,
            PreemptModeT::kPreemptive, PriorityModeT::kAdaptive, defaultEventThreshold_);
    auto numJobs = 2;
    std::vector<enjima::core::JobID> jobIds;
    for (int i = 0; i < numJobs; i++) {
        enjima::runtime::StreamingJob streamingJob = SetUpWindowlessPipelineForMultiQueryRun(executionEngine_,
                srcOpName, projectOpName, statEqJoinOpName, sinkOpName, std::to_string(i + 1));
        auto jobId = executionEngine_->Submit(streamingJob);
        jobIds.emplace_back(jobId);
    }
    std::this_thread::sleep_for(std::chrono::seconds(10));
    for (const auto& jobId: jobIds) {
        executionEngine_->Cancel(jobId, std::chrono::seconds(10));
    }

    ValidatePipelineResultsForMultiQueryRun(false, false, profiler_, executionEngine_, srcOpName, projectOpName,
            statEqJoinOpName, windowOpName, windowJoinOpName, filteredWindowOpName, sinkOpName, numJobs);
}

TEST_F(MultiQueryTest, MultiQueryAssertedWindowedLinearPipelineTB)
{
    StartEngineWithParams(10, SchedModeT::kThreadBased, defaultNumWorkers_, ProcModeT::kUnassigned);

    std::vector<enjima::core::JobID> jobIds;
    for (int i = 0; i < defaultNumQueries_; i++) {
        enjima::runtime::StreamingJob streamingJob = SetUpFixedRateWindowedPipelineForMultiQueryRun(executionEngine_,
                srcOpName, projectOpName, statEqJoinOpName, windowOpName, sinkOpName, defaultInputRate_,
                std::to_string(i + 1), ProcModeT::kBlockBasedBatch);
        auto jobId = executionEngine_->Submit(streamingJob);
        jobIds.emplace_back(jobId);
    }
    std::this_thread::sleep_for(std::chrono::seconds(30));
    for (const auto& jobId: jobIds) {
        executionEngine_->Cancel(jobId, std::chrono::seconds(10));
    }

    ValidatePipelineResultsForMultiQueryRun(true, false, profiler_, executionEngine_, srcOpName, projectOpName,
            statEqJoinOpName, windowOpName, windowJoinOpName, filteredWindowOpName, sinkOpName, defaultNumQueries_);
}

TEST_F(MultiQueryTest, MultiQueryAssertedWindowedLinearPipelineSBL)
{
    StartEngineWithParams(10, SchedModeT::kStateBasedPriority, defaultNumWorkers_, ProcModeT::kBlockBasedSingle,
            PreemptModeT::kNonPreemptive, PriorityModeT::kLatencyOptimized, defaultEventThreshold_,
            defaultMaxIdleThresholdMs_);

    std::vector<enjima::core::JobID> jobIds;
    for (int i = 0; i < defaultNumQueries_; i++) {
        enjima::runtime::StreamingJob streamingJob =
                SetUpFixedRateWindowedPipelineForMultiQueryRun(executionEngine_, srcOpName, projectOpName,
                        statEqJoinOpName, windowOpName, sinkOpName, defaultInputRate_, std::to_string(i + 1));
        auto jobId = executionEngine_->Submit(streamingJob);
        jobIds.emplace_back(jobId);
    }
    std::this_thread::sleep_for(std::chrono::seconds(30));
    for (const auto& jobId: jobIds) {
        executionEngine_->Cancel(jobId, std::chrono::seconds(10));
    }

    ValidatePipelineResultsForMultiQueryRun(true, false, profiler_, executionEngine_, srcOpName, projectOpName,
            statEqJoinOpName, windowOpName, windowJoinOpName, filteredWindowOpName, sinkOpName, defaultNumQueries_);
}

TEST_F(MultiQueryTest, MultiQueryAssertedBatchedWindowedLinearPipelineTB)
{
    StartEngineWithParams(10, SchedModeT::kThreadBased, defaultNumWorkers_, ProcModeT::kUnassigned);

    std::vector<enjima::core::JobID> jobIds;
    for (int i = 0; i < defaultNumQueries_; i++) {
        enjima::runtime::StreamingJob streamingJob = SetUpFixedRateWindowedPipelineForMultiQueryRun(executionEngine_,
                srcOpName, projectOpName, statEqJoinOpName, windowOpName, sinkOpName, defaultInputRate_,
                std::to_string(i + 1), ProcModeT::kBlockBasedBatch);
        auto jobId = executionEngine_->Submit(streamingJob);
        jobIds.emplace_back(jobId);
    }
    std::this_thread::sleep_for(std::chrono::seconds(30));
    for (const auto& jobId: jobIds) {
        executionEngine_->Cancel(jobId, std::chrono::seconds(10));
    }

    ValidatePipelineResultsForMultiQueryRun(true, false, profiler_, executionEngine_, srcOpName, projectOpName,
            statEqJoinOpName, windowOpName, windowJoinOpName, filteredWindowOpName, sinkOpName, defaultNumQueries_);
}

TEST_F(MultiQueryTest, MultiQueryAssertedBatchedWindowedLinearPipelineSBL)
{
    StartEngineWithParams(10, SchedModeT::kStateBasedPriority, defaultNumWorkers_, ProcModeT::kBlockBasedBatch,
            PreemptModeT::kNonPreemptive, PriorityModeT::kLatencyOptimized, defaultEventThreshold_,
            defaultMaxIdleThresholdMs_);

    std::vector<enjima::core::JobID> jobIds;
    for (int i = 0; i < defaultNumQueries_; i++) {
        enjima::runtime::StreamingJob streamingJob =
                SetUpFixedRateWindowedPipelineForMultiQueryRun(executionEngine_, srcOpName, projectOpName,
                        statEqJoinOpName, windowOpName, sinkOpName, defaultInputRate_, std::to_string(i + 1));
        auto jobId = executionEngine_->Submit(streamingJob);
        jobIds.emplace_back(jobId);
    }
    std::this_thread::sleep_for(std::chrono::seconds(30));
    for (const auto& jobId: jobIds) {
        executionEngine_->Cancel(jobId, std::chrono::seconds(10));
    }

    ValidatePipelineResultsForMultiQueryRun(true, false, profiler_, executionEngine_, srcOpName, projectOpName,
            statEqJoinOpName, windowOpName, windowJoinOpName, filteredWindowOpName, sinkOpName, defaultNumQueries_);
}

TEST_F(MultiQueryTest, MultiQueryAssertedBatchedWindowedLinearPipelineSBHighestRate)
{
    StartEngineWithParams(1, SchedModeT::kStateBasedPriority, defaultNumWorkers_, ProcModeT::kBlockBasedBatch,
            PreemptModeT::kNonPreemptive, PriorityModeT::kHighestRate, defaultEventThreshold_,
            defaultMaxIdleThresholdMs_);

    std::vector<enjima::core::JobID> jobIds;
    for (int i = 0; i < 4; i++) {
        enjima::runtime::StreamingJob streamingJob =
                SetUpFixedRateWindowedPipelineForMultiQueryRun(executionEngine_, srcOpName, projectOpName,
                        statEqJoinOpName, windowOpName, sinkOpName, defaultInputRate_, std::to_string(i + 1));
        auto jobId = executionEngine_->Submit(streamingJob);
        jobIds.emplace_back(jobId);
    }
    std::this_thread::sleep_for(std::chrono::seconds(30));
    for (const auto& jobId: jobIds) {
        executionEngine_->Cancel(jobId, std::chrono::seconds(10));
    }

    ValidatePipelineResultsForMultiQueryRun(true, false, profiler_, executionEngine_, srcOpName, projectOpName,
            statEqJoinOpName, windowOpName, windowJoinOpName, filteredWindowOpName, sinkOpName, defaultNumQueries_);
}

TEST_F(MultiQueryTest, MultiQueryAssertedBatchedWindowedLinearPipelineSBMinLatency)
{
    StartEngineWithParams(10, SchedModeT::kStateBasedPriority, defaultNumWorkers_, ProcModeT::kBlockBasedBatch,
            PreemptModeT::kNonPreemptive, PriorityModeT::kMinLatency, defaultEventThreshold_,
            defaultMaxIdleThresholdMs_);

    std::vector<enjima::core::JobID> jobIds;
    for (int i = 0; i < defaultNumQueries_; i++) {
        enjima::runtime::StreamingJob streamingJob =
                SetUpFixedRateWindowedPipelineForMultiQueryRun(executionEngine_, srcOpName, projectOpName,
                        statEqJoinOpName, windowOpName, sinkOpName, defaultInputRate_, std::to_string(i + 1));
        auto jobId = executionEngine_->Submit(streamingJob);
        jobIds.emplace_back(jobId);
    }
    std::this_thread::sleep_for(std::chrono::seconds(30));
    for (const auto& jobId: jobIds) {
        executionEngine_->Cancel(jobId, std::chrono::seconds(10));
    }

    ValidatePipelineResultsForMultiQueryRun(true, false, profiler_, executionEngine_, srcOpName, projectOpName,
            statEqJoinOpName, windowOpName, windowJoinOpName, filteredWindowOpName, sinkOpName, defaultNumQueries_);
}

TEST_F(MultiQueryTest, MultiQueryAssertedBatchedWindowedLinearPipelineSBMC)
{
    StartEngineWithParams(10, SchedModeT::kStateBasedPriority, defaultNumWorkers_, ProcModeT::kBlockBasedBatch,
            PreemptModeT::kNonPreemptive, PriorityModeT::kMinCost, defaultEventThreshold_, defaultMaxIdleThresholdMs_);

    std::vector<enjima::core::JobID> jobIds;
    for (int i = 0; i < defaultNumQueries_; i++) {
        enjima::runtime::StreamingJob streamingJob =
                SetUpFixedRateWindowedPipelineForMultiQueryRun(executionEngine_, srcOpName, projectOpName,
                        statEqJoinOpName, windowOpName, sinkOpName, defaultInputRate_, std::to_string(i + 1));
        auto jobId = executionEngine_->Submit(streamingJob);
        jobIds.emplace_back(jobId);
    }
    std::this_thread::sleep_for(std::chrono::seconds(30));
    for (const auto& jobId: jobIds) {
        executionEngine_->Cancel(jobId, std::chrono::seconds(10));
    }

    ValidatePipelineResultsForMultiQueryRun(true, false, profiler_, executionEngine_, srcOpName, projectOpName,
            statEqJoinOpName, windowOpName, windowJoinOpName, filteredWindowOpName, sinkOpName, defaultNumQueries_);
}

TEST_F(MultiQueryTest, MultiQueryAssertedBatchedWindowedLinearPipelineSBFCFS)
{
    StartEngineWithParams(10, SchedModeT::kStateBasedPriority, defaultNumWorkers_, ProcModeT::kBlockBasedBatch,
            PreemptModeT::kNonPreemptive, PriorityModeT::kFirstComeFirstServed, defaultEventThreshold_,
            defaultMaxIdleThresholdMs_);

    std::vector<enjima::core::JobID> jobIds;
    for (int i = 0; i < defaultNumQueries_; i++) {
        enjima::runtime::StreamingJob streamingJob =
                SetUpFixedRateWindowedPipelineForMultiQueryRun(executionEngine_, srcOpName, projectOpName,
                        statEqJoinOpName, windowOpName, sinkOpName, defaultInputRate_, std::to_string(i + 1));
        auto jobId = executionEngine_->Submit(streamingJob);
        jobIds.emplace_back(jobId);
    }
    std::this_thread::sleep_for(std::chrono::seconds(30));
    for (const auto& jobId: jobIds) {
        executionEngine_->Cancel(jobId, std::chrono::seconds(10));
    }

    ValidatePipelineResultsForMultiQueryRun(true, false, profiler_, executionEngine_, srcOpName, projectOpName,
            statEqJoinOpName, windowOpName, windowJoinOpName, filteredWindowOpName, sinkOpName, defaultNumQueries_);
}

TEST_F(MultiQueryTest, MultiQueryAssertedBatchedWindowedLinearPipelineSBRoundRobin)
{
    StartEngineWithParams(10, SchedModeT::kStateBasedPriority, defaultNumWorkers_, ProcModeT::kBlockBasedBatch,
            PreemptModeT::kNonPreemptive, PriorityModeT::kRoundRobin, defaultEventThreshold_,
            defaultMaxIdleThresholdMs_);

    std::vector<enjima::core::JobID> jobIds;
    for (int i = 0; i < defaultNumQueries_; i++) {
        enjima::runtime::StreamingJob streamingJob =
                SetUpFixedRateWindowedPipelineForMultiQueryRun(executionEngine_, srcOpName, projectOpName,
                        statEqJoinOpName, windowOpName, sinkOpName, defaultInputRate_, std::to_string(i + 1));
        auto jobId = executionEngine_->Submit(streamingJob);
        jobIds.emplace_back(jobId);
    }
    std::this_thread::sleep_for(std::chrono::seconds(30));
    for (const auto& jobId: jobIds) {
        executionEngine_->Cancel(jobId, std::chrono::seconds(10));
    }

    ValidatePipelineResultsForMultiQueryRun(true, false, profiler_, executionEngine_, srcOpName, projectOpName,
            statEqJoinOpName, windowOpName, windowJoinOpName, filteredWindowOpName, sinkOpName, defaultNumQueries_);
}

TEST_F(MultiQueryTest, MultiQueryAssertedBatchedWindowJoinedPipelineTB)
{
    StartEngineWithParams(10, SchedModeT::kThreadBased, defaultNumWorkers_, ProcModeT::kUnassigned);

    std::vector<enjima::core::JobID> jobIds;
    for (int i = 0; i < defaultNumQueries_; i++) {
        enjima::runtime::StreamingJob streamingJob = SetUpFixedRateWindowJoinedPipelineForMultiQueryRun(
                executionEngine_, srcOpName, projectOpName, statEqJoinOpName, windowOpName, windowJoinOpName,
                filterOpName, filteredStatEqJoinOpName, filteredWindowOpName, sinkOpName, defaultInputRate_,
                std::to_string(i + 1), ProcModeT::kBlockBasedBatch);
        auto jobId = executionEngine_->Submit(streamingJob);
        jobIds.emplace_back(jobId);
    }
    std::this_thread::sleep_for(std::chrono::seconds(30));
    for (const auto& jobId: jobIds) {
        executionEngine_->Cancel(jobId, std::chrono::seconds(10));
    }

    ValidatePipelineResultsForMultiQueryRun(true, true, profiler_, executionEngine_, srcOpName, projectOpName,
            statEqJoinOpName, windowOpName, windowJoinOpName, filteredWindowOpName, sinkOpName, defaultNumQueries_);
}

TEST_F(MultiQueryTest, MultiQueryAssertedBatchedWindowJoinedPipelineSBL)
{
    StartEngineWithParams(10, SchedModeT::kStateBasedPriority, defaultNumWorkers_, ProcModeT::kBlockBasedBatch,
            PreemptModeT::kNonPreemptive, PriorityModeT::kLatencyOptimized, defaultEventThreshold_,
            defaultMaxIdleThresholdMs_);

    std::vector<enjima::core::JobID> jobIds;
    for (int i = 0; i < defaultNumQueries_; i++) {
        enjima::runtime::StreamingJob streamingJob =
                SetUpFixedRateWindowJoinedPipelineForMultiQueryRun(executionEngine_, srcOpName, projectOpName,
                        statEqJoinOpName, windowOpName, windowJoinOpName, filterOpName, filteredStatEqJoinOpName,
                        filteredWindowOpName, sinkOpName, defaultInputRate_, std::to_string(i + 1));
        auto jobId = executionEngine_->Submit(streamingJob);
        jobIds.emplace_back(jobId);
    }
    std::this_thread::sleep_for(std::chrono::seconds(30));
    for (const auto& jobId: jobIds) {
        executionEngine_->Cancel(jobId, std::chrono::seconds(10));
    }

    ValidatePipelineResultsForMultiQueryRun(true, true, profiler_, executionEngine_, srcOpName, projectOpName,
            statEqJoinOpName, windowOpName, windowJoinOpName, filteredWindowOpName, sinkOpName, defaultNumQueries_);
}

TEST_F(MultiQueryTest, MultiQueryAssertedBatchedWindowJoinedPipelineSBHighestRate)
{
    StartEngineWithParams(1, SchedModeT::kStateBasedPriority, defaultNumWorkers_, ProcModeT::kBlockBasedBatch,
            PreemptModeT::kNonPreemptive, PriorityModeT::kHighestRate, defaultEventThreshold_,
            defaultMaxIdleThresholdMs_);

    std::vector<enjima::core::JobID> jobIds;
    for (int i = 0; i < defaultNumQueries_; i++) {
        enjima::runtime::StreamingJob streamingJob =
                SetUpFixedRateWindowJoinedPipelineForMultiQueryRun(executionEngine_, srcOpName, projectOpName,
                        statEqJoinOpName, windowOpName, windowJoinOpName, filterOpName, filteredStatEqJoinOpName,
                        filteredWindowOpName, sinkOpName, defaultInputRate_, std::to_string(i + 1));
        auto jobId = executionEngine_->Submit(streamingJob);
        jobIds.emplace_back(jobId);
    }
    std::this_thread::sleep_for(std::chrono::seconds(60));
    for (const auto& jobId: jobIds) {
        executionEngine_->Cancel(jobId, std::chrono::seconds(10));
    }

    ValidatePipelineResultsForMultiQueryRun(true, true, profiler_, executionEngine_, srcOpName, projectOpName,
            statEqJoinOpName, windowOpName, windowJoinOpName, filteredWindowOpName, sinkOpName, defaultNumQueries_);
}

TEST_F(MultiQueryTest, MultiQueryAssertedBatchedSlidingWindowedLinearPipelineTB)
{
    StartEngineWithParams(10, SchedModeT::kThreadBased, defaultNumWorkers_, ProcModeT::kUnassigned);

    std::vector<enjima::core::JobID> jobIds;
    for (int i = 0; i < defaultNumQueries_; i++) {
        enjima::runtime::StreamingJob streamingJob = SetUpFixedRateSlidingWindowedPipelineForMultiQueryRun(
                executionEngine_, srcOpName, projectOpName, statEqJoinOpName, slidingWindowOpName, sinkOpName,
                defaultInputRate_, std::to_string(i + 1), ProcModeT::kBlockBasedBatch);
        auto jobId = executionEngine_->Submit(streamingJob);
        jobIds.emplace_back(jobId);
    }
    std::this_thread::sleep_for(std::chrono::seconds(30));
    for (const auto& jobId: jobIds) {
        executionEngine_->Cancel(jobId, std::chrono::seconds(10));
    }

    ValidatePipelineResultsForMultiQueryRun(true, false, profiler_, executionEngine_, srcOpName, projectOpName,
            statEqJoinOpName, slidingWindowOpName, windowJoinOpName, filteredWindowOpName, sinkOpName,
            defaultNumQueries_);
}

TEST_F(MultiQueryTest, MultiQueryAssertedBatchedSlidingWindowedLinearPipelineSBL)
{
    StartEngineWithParams(10, SchedModeT::kStateBasedPriority, defaultNumWorkers_, ProcModeT::kBlockBasedBatch,
            PreemptModeT::kNonPreemptive, PriorityModeT::kLatencyOptimized, defaultEventThreshold_,
            defaultMaxIdleThresholdMs_);

    std::vector<enjima::core::JobID> jobIds;
    for (int i = 0; i < defaultNumQueries_; i++) {
        enjima::runtime::StreamingJob streamingJob =
                SetUpFixedRateSlidingWindowedPipelineForMultiQueryRun(executionEngine_, srcOpName, projectOpName,
                        statEqJoinOpName, slidingWindowOpName, sinkOpName, defaultInputRate_, std::to_string(i + 1));
        auto jobId = executionEngine_->Submit(streamingJob);
        jobIds.emplace_back(jobId);
    }
    std::this_thread::sleep_for(std::chrono::seconds(30));
    for (const auto& jobId: jobIds) {
        executionEngine_->Cancel(jobId, std::chrono::seconds(10));
    }

    ValidatePipelineResultsForMultiQueryRun(true, false, profiler_, executionEngine_, srcOpName, projectOpName,
            statEqJoinOpName, slidingWindowOpName, windowJoinOpName, filteredWindowOpName, sinkOpName,
            defaultNumQueries_);
}

TEST_F(MultiQueryTest, MultiQueryAssertedBatchedWindowGoGroupedPipelineSBL)
{
    StartEngineWithParams(10, SchedModeT::kStateBasedPriority, defaultNumWorkers_, ProcModeT::kBlockBasedBatch,
            PreemptModeT::kNonPreemptive, PriorityModeT::kLatencyOptimized, defaultEventThreshold_,
            defaultMaxIdleThresholdMs_);

    std::vector<enjima::core::JobID> jobIds;
    for (int i = 0; i < defaultNumQueries_; i++) {
        enjima::runtime::StreamingJob streamingJob =
                SetUpFixedRateWindowCoGroupedPipelineForMultiQueryRun(executionEngine_, srcOpName, projectOpName,
                        statEqJoinOpName, windowOpName, windowCoGroupOpName, filterOpName, filteredStatEqJoinOpName,
                        filteredWindowOpName, sinkOpName, defaultInputRate_, std::to_string(i + 1));
        auto jobId = executionEngine_->Submit(streamingJob);
        jobIds.emplace_back(jobId);
    }
    std::this_thread::sleep_for(std::chrono::seconds(30));
    for (const auto& jobId: jobIds) {
        executionEngine_->Cancel(jobId, std::chrono::seconds(10));
    }

    ValidatePipelineResultsForMultiQueryRun(true, true, profiler_, executionEngine_, srcOpName, projectOpName,
            statEqJoinOpName, windowOpName, windowCoGroupOpName, filteredWindowOpName, sinkOpName, defaultNumQueries_);
}

TEST_F(MultiQueryTest, MultiQueryAssertedLongRunningBatchedWindowedLinearPipelineSBL)
{
    StartEngineWithParams(10, SchedModeT::kStateBasedPriority, defaultNumWorkers_, ProcModeT::kBlockBasedBatch,
            PreemptModeT::kNonPreemptive, PriorityModeT::kLatencyOptimized, defaultEventThreshold_,
            defaultMaxIdleThresholdMs_);

    std::vector<enjima::core::JobID> jobIds;
    for (int i = 0; i < defaultNumQueries_; i++) {
        enjima::runtime::StreamingJob streamingJob =
                SetUpFixedRateWindowedPipelineForMultiQueryRun(executionEngine_, srcOpName, projectOpName,
                        statEqJoinOpName, windowOpName, sinkOpName, defaultInputRate_, std::to_string(i + 1));
        auto jobId = executionEngine_->Submit(streamingJob);
        jobIds.emplace_back(jobId);
    }
    std::this_thread::sleep_for(std::chrono::seconds(300));
    for (const auto& jobId: jobIds) {
        executionEngine_->Cancel(jobId, std::chrono::seconds(10));
    }

    ValidatePipelineResultsForMultiQueryRun(true, false, profiler_, executionEngine_, srcOpName, projectOpName,
            statEqJoinOpName, windowOpName, windowJoinOpName, filteredWindowOpName, sinkOpName, defaultNumQueries_);
}

TEST_F(MultiQueryTest, MultiQueryAssertedQueuedSlidingWindowedLinearPipelineTB)
{
    StartEngineWithParams(10, SchedModeT::kThreadBased, defaultNumWorkers_, ProcModeT::kQueueBasedSingle);

    std::vector<enjima::core::JobID> jobIds;
    for (int i = 0; i < defaultNumQueries_; i++) {
        enjima::runtime::StreamingJob streamingJob = SetUpFixedRateSlidingWindowedPipelineForMultiQueryRun(
                executionEngine_, srcOpName, projectOpName, statEqJoinOpName, slidingWindowOpName, sinkOpName,
                defaultInputRate_, std::to_string(i + 1), ProcModeT::kBlockBasedBatch);
        streamingJob.SetProcessingMode(ProcModeT::kQueueBasedSingle);
        auto jobId = executionEngine_->Submit(streamingJob);
        jobIds.emplace_back(jobId);
    }
    std::this_thread::sleep_for(std::chrono::seconds(30));
    for (const auto& jobId: jobIds) {
        executionEngine_->Cancel(jobId, std::chrono::seconds(10));
    }

    ValidatePipelineResultsForMultiQueryRun(true, false, profiler_, executionEngine_, srcOpName, projectOpName,
            statEqJoinOpName, slidingWindowOpName, windowJoinOpName, filteredWindowOpName, sinkOpName,
            defaultNumQueries_);
}

TEST_F(MultiQueryTest, MultiQueryAssertedQueuedWindowJoinedPipelineTB)
{
    StartEngineWithParams(10, SchedModeT::kThreadBased, defaultNumWorkers_, ProcModeT::kQueueBasedSingle,
            PreemptModeT::kUnassigned, PriorityModeT::kUnassigned, defaultEventThreshold_, defaultMaxIdleThresholdMs_);

    std::vector<enjima::core::JobID> jobIds;
    for (int i = 0; i < defaultNumQueries_; i++) {
        enjima::runtime::StreamingJob streamingJob =
                SetUpFixedRateWindowJoinedPipelineForMultiQueryRun(executionEngine_, srcOpName, projectOpName,
                        statEqJoinOpName, windowOpName, windowJoinOpName, filterOpName, filteredStatEqJoinOpName,
                        filteredWindowOpName, sinkOpName, defaultInputRate_, std::to_string(i + 1));
        streamingJob.SetProcessingMode(ProcModeT::kQueueBasedSingle);
        auto jobId = executionEngine_->Submit(streamingJob);
        jobIds.emplace_back(jobId);
    }
    std::this_thread::sleep_for(std::chrono::seconds(30));
    for (const auto& jobId: jobIds) {
        executionEngine_->Cancel(jobId, std::chrono::seconds(10));
    }

    ValidatePipelineResultsForMultiQueryRun(true, true, profiler_, executionEngine_, srcOpName, projectOpName,
            statEqJoinOpName, windowOpName, windowJoinOpName, filteredWindowOpName, sinkOpName, defaultNumQueries_);
}

TEST_F(MultiQueryTest, MultiQueryAssertedQueuedWindowJoinedPipelineSBMC)
{
    StartEngineWithParams(1, SchedModeT::kStateBasedPriority, defaultNumWorkers_, ProcModeT::kBlockBasedBatch,
            PreemptModeT::kNonPreemptive, PriorityModeT::kMinCost, defaultEventThreshold_, defaultMaxIdleThresholdMs_);

    std::vector<enjima::core::JobID> jobIds;
    for (int i = 0; i < defaultNumQueries_; i++) {
        enjima::runtime::StreamingJob streamingJob =
                SetUpFixedRateWindowJoinedPipelineForMultiQueryRun(executionEngine_, srcOpName, projectOpName,
                        statEqJoinOpName, windowOpName, windowJoinOpName, filterOpName, filteredStatEqJoinOpName,
                        filteredWindowOpName, sinkOpName, defaultInputRate_, std::to_string(i + 1));
        auto jobId = executionEngine_->Submit(streamingJob);
        jobIds.emplace_back(jobId);
    }
    std::this_thread::sleep_for(std::chrono::seconds(30));
    for (const auto& jobId: jobIds) {
        executionEngine_->Cancel(jobId, std::chrono::seconds(10));
    }

    ValidatePipelineResultsForMultiQueryRun(true, true, profiler_, executionEngine_, srcOpName, projectOpName,
            statEqJoinOpName, windowOpName, windowJoinOpName, filteredWindowOpName, sinkOpName, defaultNumQueries_);

    PrintAveragedLatencyResultsForMultiQueryRun(profiler_, sinkOpName, defaultNumQueries_);
    PrintAveragedThroughputResultsForMultiQueryRun(profiler_, srcOpName, defaultNumQueries_);
}

TEST_F(MultiQueryTest, MultiQueryAssertedQueuedWindowJoinedPipelineRR)
{
    StartEngineWithParams(1, SchedModeT::kStateBasedPriority, defaultNumWorkers_, ProcModeT::kBlockBasedBatch,
            PreemptModeT::kPreemptive, PriorityModeT::kRoundRobin, defaultEventThreshold_, defaultMaxIdleThresholdMs_);

    std::vector<enjima::core::JobID> jobIds;
    for (int i = 0; i < defaultNumQueries_; i++) {
        enjima::runtime::StreamingJob streamingJob =
                SetUpFixedRateWindowJoinedPipelineForMultiQueryRun(executionEngine_, srcOpName, projectOpName,
                        statEqJoinOpName, windowOpName, windowJoinOpName, filterOpName, filteredStatEqJoinOpName,
                        filteredWindowOpName, sinkOpName, defaultInputRate_, std::to_string(i + 1));
        auto jobId = executionEngine_->Submit(streamingJob);
        jobIds.emplace_back(jobId);
    }
    std::this_thread::sleep_for(std::chrono::seconds(30));
    for (const auto& jobId: jobIds) {
        executionEngine_->Cancel(jobId, std::chrono::seconds(100));
    }

    ValidatePipelineResultsForMultiQueryRun(true, true, profiler_, executionEngine_, srcOpName, projectOpName,
            statEqJoinOpName, windowOpName, windowJoinOpName, filteredWindowOpName, sinkOpName, defaultNumQueries_);

    PrintAveragedLatencyResultsForMultiQueryRun(profiler_, sinkOpName, defaultNumQueries_);
    PrintAveragedThroughputResultsForMultiQueryRun(profiler_, srcOpName, defaultNumQueries_);
}

TEST_F(MultiQueryTest, MultiQueryAssertedQueuedWindowJoinedPipelineNonPreemptiveRR)
{
    StartEngineWithParams(1, SchedModeT::kStateBasedPriority, defaultNumWorkers_, ProcModeT::kBlockBasedBatch,
            PreemptModeT::kNonPreemptive, PriorityModeT::kRoundRobin, defaultEventThreshold_,
            defaultMaxIdleThresholdMs_);

    std::vector<enjima::core::JobID> jobIds;
    for (int i = 0; i < defaultNumQueries_; i++) {
        enjima::runtime::StreamingJob streamingJob =
                SetUpFixedRateWindowJoinedPipelineForMultiQueryRun(executionEngine_, srcOpName, projectOpName,
                        statEqJoinOpName, windowOpName, windowJoinOpName, filterOpName, filteredStatEqJoinOpName,
                        filteredWindowOpName, sinkOpName, defaultInputRate_, std::to_string(i + 1));
        auto jobId = executionEngine_->Submit(streamingJob);
        jobIds.emplace_back(jobId);
    }
    std::this_thread::sleep_for(std::chrono::seconds(30));
    for (const auto& jobId: jobIds) {
        executionEngine_->Cancel(jobId, std::chrono::seconds(10));
    }

    ValidatePipelineResultsForMultiQueryRun(true, true, profiler_, executionEngine_, srcOpName, projectOpName,
            statEqJoinOpName, windowOpName, windowJoinOpName, filteredWindowOpName, sinkOpName, defaultNumQueries_);

    PrintAveragedLatencyResultsForMultiQueryRun(profiler_, sinkOpName, defaultNumQueries_);
    PrintAveragedThroughputResultsForMultiQueryRun(profiler_, srcOpName, defaultNumQueries_);
}

TEST_F(MultiQueryTest, MultiQueryAssertedQueuedWindowCoGroupedPipelineTB)
{
    StartEngineWithParams(10, SchedModeT::kThreadBased, defaultNumWorkers_, ProcModeT::kQueueBasedSingle,
            PreemptModeT::kUnassigned, PriorityModeT::kUnassigned, defaultEventThreshold_, defaultMaxIdleThresholdMs_);

    std::vector<enjima::core::JobID> jobIds;
    for (int i = 0; i < defaultNumQueries_; i++) {
        enjima::runtime::StreamingJob streamingJob =
                SetUpFixedRateWindowCoGroupedPipelineForMultiQueryRun(executionEngine_, srcOpName, projectOpName,
                        statEqJoinOpName, windowOpName, windowCoGroupOpName, filterOpName, filteredStatEqJoinOpName,
                        filteredWindowOpName, sinkOpName, defaultInputRate_, std::to_string(i + 1));
        streamingJob.SetProcessingMode(ProcModeT::kQueueBasedSingle);
        auto jobId = executionEngine_->Submit(streamingJob);
        jobIds.emplace_back(jobId);
    }
    std::this_thread::sleep_for(std::chrono::seconds(30));
    for (const auto& jobId: jobIds) {
        executionEngine_->Cancel(jobId, std::chrono::seconds(10));
    }

    ValidatePipelineResultsForMultiQueryRun(true, true, profiler_, executionEngine_, srcOpName, projectOpName,
            statEqJoinOpName, windowOpName, windowCoGroupOpName, filteredWindowOpName, sinkOpName, defaultNumQueries_);
}

// Please do not make a commit with this test enabled as this would break the automated test pipeline
TEST_F(MultiQueryTest, DISABLED_InteractiveAssertedWindowedLinearPipeline)
{
    auto schedMode = SchedModeT::kStateBasedPriority;
    auto procMode = ProcModeT::kBlockBasedBatch;
    auto numQueries = defaultNumQueries_;
    auto numWorkers = defaultNumWorkers_;
    auto inputRate = defaultInputRate_;

    std::string interactiveInputCommand;
    std::string interactiveMode;
    while (interactiveInputCommand != "done") {
        if (interactiveMode.empty()) {
            std::cout << "Set interactive mode {Linear (L), Interactive (I)} [L]: ";
            std::getline(std::cin, interactiveMode);
        }
        if (interactiveMode == "I" || interactiveMode == "Interactive") {
            std::cout << "Set argument for: ";
            std::cin >> interactiveInputCommand;
            if (interactiveInputCommand == "schedulingMode") {
                SetSchedulingMode(&schedMode);
            }
            else if (interactiveInputCommand == "processingMode") {
                SetProcessingMode(&procMode);
            }
            else if (interactiveInputCommand == "numQueries") {
                SetNumQueries(&numQueries);
            }
            else if (interactiveInputCommand == "numWorkers") {
                SetNumWorkers(&numWorkers);
            }
            else if (interactiveInputCommand == "inputRate") {
                SetInputRate(&inputRate);
            }
        }
        else if (interactiveMode == "L" || interactiveMode == "Linear" || interactiveMode.empty()) {
            SetSchedulingMode(&schedMode);
            SetProcessingMode(&procMode);
            SetNumQueries(&numQueries);
            SetNumWorkers(&numWorkers);
            SetInputRate(&inputRate);
            interactiveInputCommand = "done";
        }
        else {
            interactiveMode = "";
        }
    }
    std::cout << "Finished collecting parameters. Running..." << std::endl;

    StartEngineWithParams(10, schedMode, numWorkers, procMode, PreemptModeT::kNonPreemptive,
            PriorityModeT::kLatencyOptimized, defaultEventThreshold_, defaultMaxIdleThresholdMs_);

    std::vector<enjima::core::JobID> jobIds;
    for (int i = 0; i < numQueries; i++) {
        enjima::runtime::StreamingJob streamingJob = SetUpFixedRateWindowedPipelineForMultiQueryRun(executionEngine_,
                srcOpName, projectOpName, statEqJoinOpName, windowOpName, sinkOpName, inputRate, std::to_string(i + 1));
        auto jobId = executionEngine_->Submit(streamingJob);
        jobIds.emplace_back(jobId);
    }
    std::this_thread::sleep_for(std::chrono::seconds(30));
    for (const auto& jobId: jobIds) {
        executionEngine_->Cancel(jobId, std::chrono::seconds(10));
    }

    ValidatePipelineResultsForMultiQueryRun(true, false, profiler_, executionEngine_, srcOpName, projectOpName,
            statEqJoinOpName, windowOpName, windowJoinOpName, filteredWindowOpName, sinkOpName, numQueries);
}

TEST_F(MultiQueryTest, FlexibleAssertedWindowedLinearPipeline)
{
    auto schedMode = SchedModeT::kStateBasedPriority;
    auto procMode = ProcModeT::kBlockBasedBatch;
    auto preemptMode = PreemptModeT::kNonPreemptive;
    auto priorityMode = PriorityModeT::kMinCost;
    // Please set jobProcMode to kUnassigned if schedMode is not kThreadBased
    auto jobProcMode = ProcModeT::kUnassigned;
    auto numQueries = 2;
    auto numWorkers = 4;
    auto inputRate = 40'000'000;

    StartEngineWithParams(2, schedMode, numWorkers, procMode, preemptMode, priorityMode, defaultEventThreshold_, 2);

    std::vector<enjima::core::JobID> jobIds;
    for (int i = 0; i < numQueries; i++) {
        enjima::runtime::StreamingJob streamingJob = SetUpFixedRateWindowedPipelineForMultiQueryRun(executionEngine_,
                srcOpName, projectOpName, statEqJoinOpName, windowOpName, sinkOpName, inputRate, std::to_string(i + 1),
                jobProcMode, 5'000'000);
        auto jobId = executionEngine_->Submit(streamingJob);
        jobIds.emplace_back(jobId);
    }
    std::this_thread::sleep_for(std::chrono::seconds(60));
    try {
        for (const auto& jobId: jobIds) {
            executionEngine_->Cancel(jobId, std::chrono::seconds(10));
        }

        ValidatePipelineResultsForMultiQueryRun(true, false, profiler_, executionEngine_, srcOpName, projectOpName,
                statEqJoinOpName, windowOpName, windowJoinOpName, filteredWindowOpName, sinkOpName, numQueries);

        PrintAveragedLatencyResultsForMultiQueryRun(profiler_, sinkOpName, numQueries);
        PrintAveragedThroughputResultsForMultiQueryRun(profiler_, srcOpName, numQueries);
    }
    catch (const enjima::runtime::CancellationException& e) {
        std::cout << "Exception: " << e.what() << std::endl;
    }
}

TEST_F(MultiQueryTest, YSBFlexibleAssertedWindowedLinearPipeline)
{
    auto schedMode = SchedModeT::kStateBasedPriority;
    auto procMode = ProcModeT::kBlockBasedBatch;
    auto preemptMode = PreemptModeT::kNonPreemptive;
    auto priorityMode = PriorityModeT::kAdaptive;
    // Please set jobProcMode to kUnassigned if schedMode is not kThreadBased
    auto jobProcMode = ProcModeT::kUnassigned;
    auto numQueries = 2;
    auto numWorkers = 4;
    auto inputRate = 40'000'000;

    StartEngineWithParams(2, schedMode, numWorkers, procMode, preemptMode, priorityMode, defaultEventThreshold_, 2);

    std::vector<enjima::core::JobID> jobIds;
    for (int i = 0; i < numQueries; i++) {
        enjima::runtime::StreamingJob streamingJob = SetUpFixedRateYSBWindowedPipelineForMultiQueryRun(executionEngine_,
                srcOpName, filterOpName, projectOpName, statEqJoinOpName, windowOpName, sinkOpName, inputRate,
                std::to_string(i + 1), jobProcMode, 5'000'000);
        auto jobId = executionEngine_->Submit(streamingJob);
        jobIds.emplace_back(jobId);
    }
    std::this_thread::sleep_for(std::chrono::seconds(60));
    try {
        for (const auto& jobId: jobIds) {
            executionEngine_->Cancel(jobId, std::chrono::seconds(10));
        }

        ValidatePipelineResultsForMultiQueryRun(true, false, profiler_, executionEngine_, srcOpName, projectOpName,
                statEqJoinOpName, windowOpName, windowJoinOpName, filteredWindowOpName, sinkOpName, numQueries);

        PrintAveragedLatencyResultsForMultiQueryRun(profiler_, sinkOpName, numQueries);
        PrintAveragedThroughputResultsForMultiQueryRun(profiler_, srcOpName, numQueries);
    }
    catch (const enjima::runtime::CancellationException& e) {
        std::cout << "Exception: " << e.what() << std::endl;
    }
}