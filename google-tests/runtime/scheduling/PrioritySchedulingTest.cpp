//
// Created by m34ferna on 12/01/24.
//

#include "enjima/api/data_types/LinearRoadEvent.h"
#include "enjima/core/StreamingPipeline.h"
#include "enjima/memory/MemoryUtil.h"
#include "enjima/operators/FilterOperator.h"
#include "enjima/runtime/DataStream.h"
#include "enjima/runtime/scheduling/PriorityPrecedenceTracker.h"
#include "enjima/runtime/scheduling/policy/InputSizeBasedPriorityCalculator.h"
#include "google-tests/common/TestSetupHelperFunctions.h"
#include "gtest/gtest.h"

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
using PriorityTrackerT = enjima::runtime::PriorityPrecedenceTracker<enjima::runtime::NonPreemptiveMode,
        enjima::runtime::InputSizeBasedPriorityCalculator>;

class PrioritySchedulingTest : public testing::Test {
protected:
    void SetUp() override
    {
        memoryManager_ = new MemManT(maxMemory_, numBlocksPerChunk, numEventsPerBlock_, MemManT::AllocatorType::kBasic);
        memoryManager_->SetMaxActiveChunksPerOperator(100);
        profiler_ = new ProflierT(10000, true, "EnjimaPRSchedulingTest");
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
        std::string cpuListStr = "2,3";
        if (numWorkers == 3) {
            cpuListStr = "2,3,4";
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
    const uint64_t defaultIdleThreshold_ = 10;
    const uint64_t defaultEventThreshold_ = 1000;
    const uint64_t defaultInputRate_ = 12'000'000;

    const std::string srcOpName = "src";
    const std::string filterOpName = "filter";
    const std::string projectOpName = "project";
    const std::string statEqJoinOpName = "staticEqJoin";
    const std::string windowOpName = "timeWindow";
    const std::string sinkOpName = "genericSink";
};

TEST(PrioritySchedulingTestSuite, EvaluateSchedulingTracker)
{
    auto* pProfiler = new ProflierT(10000, true, "EnjimaPRSchedulingTest");
    auto* pMemoryManager = new MemManT(enjima::memory::MegaBytes(16), 48, 1000, MemManT::AllocatorType::kBasic);
    pMemoryManager->Init();
    pMemoryManager->StartMemoryPreAllocator();

    PriorityTrackerT precedenceTracker(pProfiler, 50, 1000, 50, 1000);
    auto* pSrcOp = new InMemoryYSBSourceOperator(1, "testSrcOp");
    auto* pProjOp = new enjima::operators::MapOperator<YSBAdT, YSBProjT, YSBProjectFunction>(2, "testProjOp",
            YSBProjectFunction{});
    auto* pSinkOp = new enjima::operators::GenericSinkOperator<YSBProjT, NoOpYSBSinkFunction<YSBProjT>>(3, "testSinkOp",
            NoOpYSBSinkFunction<YSBProjT>{});
    auto pStreamingPipeline = new enjima::core::StreamingPipeline;
    pStreamingPipeline->AddOperatorAtLevel(pSrcOp, 1);
    pStreamingPipeline->UpdateUpstreamAndDownstreamOperators(pSrcOp->GetOperatorId(), std::vector<OperatorID>{},
            std::vector<OperatorID>{pProjOp->GetOperatorId()});

    pStreamingPipeline->AddOperatorAtLevel(pProjOp, 2);
    pStreamingPipeline->UpdateUpstreamAndDownstreamOperators(pProjOp->GetOperatorId(),
            std::vector<OperatorID>{pSrcOp->GetOperatorId()}, std::vector<OperatorID>{pSinkOp->GetOperatorId()});

    pStreamingPipeline->AddOperatorAtLevel(pSinkOp, 3);
    pStreamingPipeline->UpdateUpstreamAndDownstreamOperators(pSinkOp->GetOperatorId(),
            std::vector<OperatorID>{pProjOp->GetOperatorId()}, std::vector<OperatorID>{});

    pMemoryManager->PreAllocatePipeline(pStreamingPipeline);
    pStreamingPipeline->Initialize(nullptr, pMemoryManager, pProfiler, 0);

    precedenceTracker.TrackPipeline(pStreamingPipeline);
    precedenceTracker.ActivateOperator(pSrcOp);
    precedenceTracker.ActivateOperator(pProjOp);
    precedenceTracker.ActivateOperator(pSinkOp);

    auto prevSchedCtxtPtr = const_cast<enjima::runtime::SchedulingContext*>(&PriorityTrackerT::kIneligibleSchedCtxt);
    auto schedCtxtPtr = precedenceTracker.GetNextInPrecedence(prevSchedCtxtPtr);
    auto* firstSchedDecision = schedCtxtPtr->GetDecisionCtxtPtr();
    auto nextOpPtr = firstSchedDecision->GetOperatorPtr();
    EXPECT_EQ(nextOpPtr, nullptr);

    pSrcOp->PopulateEventCache(100'000, 100, 10);
    pSrcOp->ProcessBlock();
    pSrcOp->ProcessBlock();
    pProjOp->ProcessBlock();
    precedenceTracker.UpdatePriority(enjima::runtime::GetSystemTimeMillis() + 50);

    prevSchedCtxtPtr = schedCtxtPtr;
    schedCtxtPtr = precedenceTracker.GetNextInPrecedence(prevSchedCtxtPtr);
    const auto& secondSchedDecision = schedCtxtPtr->GetDecisionCtxtPtr();
    nextOpPtr = secondSchedDecision->GetOperatorPtr();
    EXPECT_NE(nextOpPtr, nullptr);
    EXPECT_EQ(nextOpPtr, pSrcOp);

    prevSchedCtxtPtr = schedCtxtPtr;
    schedCtxtPtr = precedenceTracker.GetNextInPrecedence(prevSchedCtxtPtr);
    const auto& thirdSchedDecision = schedCtxtPtr->GetDecisionCtxtPtr();
    nextOpPtr = thirdSchedDecision->GetOperatorPtr();
    EXPECT_EQ(nextOpPtr, pProjOp);

    prevSchedCtxtPtr = schedCtxtPtr;
    schedCtxtPtr = precedenceTracker.GetNextInPrecedence(prevSchedCtxtPtr);
    const auto& fourthSchedDecision = schedCtxtPtr->GetDecisionCtxtPtr();
    nextOpPtr = fourthSchedDecision->GetOperatorPtr();
    EXPECT_EQ(nextOpPtr, pSinkOp);

    pMemoryManager->ReleaseAllResources();
    delete pMemoryManager;
    delete pProfiler;
    delete pSrcOp;
    delete pProjOp;
    delete pSinkOp;
}

TEST_F(PrioritySchedulingTest, BasicWindowlessPipeline)
{
    StartEngineWithParams(50, enjima::runtime::SchedulingMode::kStateBasedPriority, 3, ProcModeT ::kBlockBasedSingle,
            PreemptModeT::kNonPreemptive, PriorityModeT::kInputQueueSize, defaultEventThreshold_,
            defaultIdleThreshold_);
    enjima::runtime::StreamingJob streamingJob = SetUpBasicWindowlessPipelineNoAsserts(executionEngine_, srcOpName,
            projectOpName, statEqJoinOpName, sinkOpName);
    auto jobId = executionEngine_->Submit(streamingJob);
    std::this_thread::sleep_for(std::chrono::seconds(30));
    executionEngine_->Cancel(jobId, std::chrono::seconds(10));

    ValidatePipelineResults(false, profiler_, executionEngine_, srcOpName, projectOpName, statEqJoinOpName,
            windowOpName, sinkOpName);
}

TEST_F(PrioritySchedulingTest, BasicBatchedWindowlessPipelineLatencyOptimized)
{
    StartEngineWithParams(10, enjima::runtime::SchedulingMode::kStateBasedPriority, 2, ProcModeT ::kBlockBasedBatch,
            PreemptModeT::kNonPreemptive, PriorityModeT::kLatencyOptimized, defaultEventThreshold_, 0);
    enjima::runtime::StreamingJob streamingJob = SetUpBasicWindowlessPipeline<std::chrono::microseconds>(
            executionEngine_, srcOpName, projectOpName, statEqJoinOpName, sinkOpName);
    auto jobId = executionEngine_->Submit(streamingJob);
    std::this_thread::sleep_for(std::chrono::seconds(30));
    executionEngine_->Cancel(jobId, std::chrono::seconds(10));

    ValidatePipelineResults(false, profiler_, executionEngine_, srcOpName, projectOpName, statEqJoinOpName,
            windowOpName, sinkOpName);
}

TEST_F(PrioritySchedulingTest, BasicBatchedWindowlessPipelineLatencyOptimizedTB)
{
    StartEngineWithParams(10, enjima::runtime::SchedulingMode::kThreadBased, 2, ProcModeT ::kBlockBasedBatch,
            PreemptModeT::kPreemptive, PriorityModeT::kLatencyOptimized, defaultEventThreshold_, defaultIdleThreshold_);
    enjima::runtime::StreamingJob streamingJob = SetUpBasicWindowlessPipeline<std::chrono::microseconds>(
            executionEngine_, srcOpName, projectOpName, statEqJoinOpName, sinkOpName, ProcModeT::kBlockBasedBatch);
    auto jobId = executionEngine_->Submit(streamingJob);
    std::this_thread::sleep_for(std::chrono::seconds(30));
    executionEngine_->Cancel(jobId, std::chrono::seconds(10));

    ValidatePipelineResults(false, profiler_, executionEngine_, srcOpName, projectOpName, statEqJoinOpName,
            windowOpName, sinkOpName);
}

TEST_F(PrioritySchedulingTest, BasicWindowlessPipelinePreemptive)
{
    StartEngineWithParams(50, enjima::runtime::SchedulingMode::kStateBasedPriority, 3,
            enjima::runtime::StreamingTask::ProcessingMode::kBlockBasedSingle,
            enjima::runtime::PreemptMode::kPreemptive, PriorityModeT::kInputQueueSize, defaultEventThreshold_,
            defaultIdleThreshold_);
    enjima::runtime::StreamingJob streamingJob = SetUpBasicWindowlessPipelineNoAsserts(executionEngine_, srcOpName,
            projectOpName, statEqJoinOpName, sinkOpName);
    auto jobId = executionEngine_->Submit(streamingJob);
    std::this_thread::sleep_for(std::chrono::seconds(30));
    executionEngine_->Cancel(jobId, std::chrono::seconds(10));

    ValidatePipelineResults(false, profiler_, executionEngine_, srcOpName, projectOpName, statEqJoinOpName,
            windowOpName, sinkOpName);
}

TEST_F(PrioritySchedulingTest, BasicWindowlessPipelineAdaptive)
{
    StartEngineWithParams(50, enjima::runtime::SchedulingMode::kStateBasedPriority, 3, ProcModeT ::kBlockBasedSingle,
            PreemptModeT::kNonPreemptive, PriorityModeT::kAdaptive, defaultEventThreshold_, defaultIdleThreshold_);
    enjima::runtime::StreamingJob streamingJob = SetUpBasicWindowlessPipelineNoAsserts(executionEngine_, srcOpName,
            projectOpName, statEqJoinOpName, sinkOpName);
    auto jobId = executionEngine_->Submit(streamingJob);
    std::this_thread::sleep_for(std::chrono::seconds(30));
    executionEngine_->Cancel(jobId, std::chrono::seconds(10));

    ValidatePipelineResults(false, profiler_, executionEngine_, srcOpName, projectOpName, statEqJoinOpName,
            windowOpName, sinkOpName);
}

TEST_F(PrioritySchedulingTest, BasicWindowlessPipelineLatencyOptimized)
{
    StartEngineWithParams(50, enjima::runtime::SchedulingMode::kStateBasedPriority, 3, ProcModeT ::kBlockBasedSingle,
            PreemptModeT::kNonPreemptive, PriorityModeT::kLatencyOptimized, defaultEventThreshold_,
            defaultIdleThreshold_);
    enjima::runtime::StreamingJob streamingJob = SetUpBasicWindowlessPipelineNoAsserts(executionEngine_, srcOpName,
            projectOpName, statEqJoinOpName, sinkOpName);
    auto jobId = executionEngine_->Submit(streamingJob);
    std::this_thread::sleep_for(std::chrono::seconds(30));
    executionEngine_->Cancel(jobId, std::chrono::seconds(10));

    ValidatePipelineResults(false, profiler_, executionEngine_, srcOpName, projectOpName, statEqJoinOpName,
            windowOpName, sinkOpName);
}

TEST_F(PrioritySchedulingTest, BasicWindowlessPipelinePreemptiveAdaptive)
{
    StartEngineWithParams(50, enjima::runtime::SchedulingMode::kStateBasedPriority, 3,
            enjima::runtime::StreamingTask::ProcessingMode::kBlockBasedSingle,
            enjima::runtime::PreemptMode::kPreemptive, PriorityModeT::kAdaptive, defaultEventThreshold_,
            defaultIdleThreshold_);
    enjima::runtime::StreamingJob streamingJob = SetUpBasicWindowlessPipelineNoAsserts(executionEngine_, srcOpName,
            projectOpName, statEqJoinOpName, sinkOpName);
    auto jobId = executionEngine_->Submit(streamingJob);
    std::this_thread::sleep_for(std::chrono::seconds(30));
    executionEngine_->Cancel(jobId, std::chrono::seconds(10));

    ValidatePipelineResults(false, profiler_, executionEngine_, srcOpName, projectOpName, statEqJoinOpName,
            windowOpName, sinkOpName);
}

TEST_F(PrioritySchedulingTest, BasicWindowlessPipelinePreemptiveLatencyOptimized)
{
    StartEngineWithParams(50, enjima::runtime::SchedulingMode::kStateBasedPriority, 3, ProcModeT ::kBlockBasedSingle,
            PreemptModeT::kPreemptive, PriorityModeT::kLatencyOptimized, defaultEventThreshold_, defaultIdleThreshold_);
    enjima::runtime::StreamingJob streamingJob = SetUpBasicWindowlessPipelineNoAsserts(executionEngine_, srcOpName,
            projectOpName, statEqJoinOpName, sinkOpName);
    auto jobId = executionEngine_->Submit(streamingJob);
    std::this_thread::sleep_for(std::chrono::seconds(30));
    executionEngine_->Cancel(jobId, std::chrono::seconds(10));

    ValidatePipelineResults(false, profiler_, executionEngine_, srcOpName, projectOpName, statEqJoinOpName,
            windowOpName, sinkOpName);
}

TEST_F(PrioritySchedulingTest, AssertedWindowlessLinearPipeline)
{
    StartEngineWithParams(50, enjima::runtime::SchedulingMode::kStateBasedPriority, 3,
            enjima::runtime::StreamingTask::ProcessingMode::kBlockBasedSingle,
            enjima::runtime::PreemptMode::kNonPreemptive, PriorityModeT::kInputQueueSize, defaultEventThreshold_,
            defaultIdleThreshold_);
    enjima::runtime::StreamingJob streamingJob =
            SetUpBasicWindowlessPipeline(executionEngine_, srcOpName, projectOpName, statEqJoinOpName, sinkOpName);
    auto jobId = executionEngine_->Submit(streamingJob);
    std::this_thread::sleep_for(std::chrono::seconds(30));
    executionEngine_->Cancel(jobId, std::chrono::seconds(10));

    ValidatePipelineResults(false, profiler_, executionEngine_, srcOpName, projectOpName, statEqJoinOpName,
            windowOpName, sinkOpName);
}

TEST_F(PrioritySchedulingTest, AssertedWindowedLinearPipelineLatencyOptimized)
{
    StartEngineWithParams(10, enjima::runtime::SchedulingMode::kStateBasedPriority, 2,
            enjima::runtime::StreamingTask::ProcessingMode::kBlockBasedSingle,
            enjima::runtime::PreemptMode::kNonPreemptive, PriorityModeT::kLatencyOptimized, defaultEventThreshold_,
            defaultIdleThreshold_);
    enjima::runtime::StreamingJob streamingJob = SetUpBasicWindowedPipeline(executionEngine_, srcOpName, projectOpName,
            statEqJoinOpName, windowOpName, sinkOpName);
    auto jobId = executionEngine_->Submit(streamingJob);
    std::this_thread::sleep_for(std::chrono::seconds(30));
    executionEngine_->Cancel(jobId, std::chrono::seconds(10));

    ValidatePipelineResults(true, profiler_, executionEngine_, srcOpName, projectOpName, statEqJoinOpName, windowOpName,
            sinkOpName);
}

TEST_F(PrioritySchedulingTest, AssertedBatchedWindowedLinearPipelineLatencyOptimized)
{
    StartEngineWithParams(10, enjima::runtime::SchedulingMode::kStateBasedPriority, 2,
            enjima::runtime::StreamingTask::ProcessingMode::kBlockBasedBatch,
            enjima::runtime::PreemptMode::kNonPreemptive, PriorityModeT::kLatencyOptimized, defaultEventThreshold_, 1);
    enjima::runtime::StreamingJob streamingJob = SetUpFixedRateWindowedPipeline(executionEngine_, srcOpName,
            projectOpName, statEqJoinOpName, windowOpName, sinkOpName, defaultInputRate_);
    auto jobId = executionEngine_->Submit(streamingJob);
    std::this_thread::sleep_for(std::chrono::seconds(30));
    executionEngine_->Cancel(jobId, std::chrono::seconds(10));

    ValidatePipelineResults(true, profiler_, executionEngine_, srcOpName, projectOpName, statEqJoinOpName, windowOpName,
            sinkOpName);
}

TEST_F(PrioritySchedulingTest, AssertedBatchedWindowedLinearPipelineThroughputOptimized)
{
    StartEngineWithParams(10, enjima::runtime::SchedulingMode::kStateBasedPriority, 2,
            enjima::runtime::StreamingTask::ProcessingMode::kBlockBasedBatch,
            enjima::runtime::PreemptMode::kNonPreemptive, PriorityModeT::kThroughputOptimized, defaultEventThreshold_,
            1);
    enjima::runtime::StreamingJob streamingJob = SetUpFixedRateWindowedPipeline(executionEngine_, srcOpName,
            projectOpName, statEqJoinOpName, windowOpName, sinkOpName, defaultInputRate_);
    auto jobId = executionEngine_->Submit(streamingJob);
    std::this_thread::sleep_for(std::chrono::seconds(30));
    executionEngine_->Cancel(jobId, std::chrono::seconds(10));

    ValidatePipelineResults(true, profiler_, executionEngine_, srcOpName, projectOpName, statEqJoinOpName, windowOpName,
            sinkOpName);
}

TEST_F(PrioritySchedulingTest, AssertedBatchedWindowedLinearPipelineAdaptive)
{
    StartEngineWithParams(10, enjima::runtime::SchedulingMode::kStateBasedPriority, 2,
            enjima::runtime::StreamingTask::ProcessingMode::kBlockBasedBatch,
            enjima::runtime::PreemptMode::kNonPreemptive, PriorityModeT::kAdaptive, defaultEventThreshold_, 1);
    enjima::runtime::StreamingJob streamingJob = SetUpFixedRateWindowedPipeline(executionEngine_, srcOpName,
            projectOpName, statEqJoinOpName, windowOpName, sinkOpName, defaultInputRate_);
    auto jobId = executionEngine_->Submit(streamingJob);
    std::this_thread::sleep_for(std::chrono::seconds(30));
    executionEngine_->Cancel(jobId, std::chrono::seconds(10));

    ValidatePipelineResults(true, profiler_, executionEngine_, srcOpName, projectOpName, statEqJoinOpName, windowOpName,
            sinkOpName);
}

TEST_F(PrioritySchedulingTest, AssertedBatchedWindowedLinearPipelineTB)
{
    StartEngineWithParams(10, enjima::runtime::SchedulingMode::kThreadBased, 2,
            enjima::runtime::StreamingTask::ProcessingMode::kBlockBasedBatch,
            enjima::runtime::PreemptMode::kNonPreemptive, PriorityModeT::kLatencyOptimized, defaultEventThreshold_, 2);
    enjima::runtime::StreamingJob streamingJob =
            SetUpFixedRateWindowedPipeline(executionEngine_, srcOpName, projectOpName, statEqJoinOpName, windowOpName,
                    sinkOpName, defaultInputRate_, enjima::runtime::StreamingTask::ProcessingMode::kBlockBasedBatch);
    auto jobId = executionEngine_->Submit(streamingJob);
    std::this_thread::sleep_for(std::chrono::seconds(30));
    executionEngine_->Cancel(jobId, std::chrono::seconds(10));

    ValidatePipelineResults(true, profiler_, executionEngine_, srcOpName, projectOpName, statEqJoinOpName, windowOpName,
            sinkOpName);
}

TEST_F(PrioritySchedulingTest, AssertedBatchedWindowedLinearPipelineSimpleLatency)
{
    StartEngineWithParams(10, enjima::runtime::SchedulingMode::kStateBasedPriority, 2,
            enjima::runtime::StreamingTask::ProcessingMode::kBlockBasedBatch,
            enjima::runtime::PreemptMode::kNonPreemptive, PriorityModeT::kMinLatency, defaultEventThreshold_,
            defaultIdleThreshold_);
    enjima::runtime::StreamingJob streamingJob = SetUpBasicWindowedPipeline(executionEngine_, srcOpName, projectOpName,
            statEqJoinOpName, windowOpName, sinkOpName);
    auto jobId = executionEngine_->Submit(streamingJob);
    std::this_thread::sleep_for(std::chrono::seconds(30));
    executionEngine_->Cancel(jobId, std::chrono::seconds(10));

    ValidatePipelineResults(true, profiler_, executionEngine_, srcOpName, projectOpName, statEqJoinOpName, windowOpName,
            sinkOpName);
}

TEST_F(PrioritySchedulingTest, AssertedBatchedWindowedLinearPipelineFCFS)
{
    StartEngineWithParams(10, enjima::runtime::SchedulingMode::kStateBasedPriority, 2,
            enjima::runtime::StreamingTask::ProcessingMode::kBlockBasedBatch,
            enjima::runtime::PreemptMode::kNonPreemptive, PriorityModeT::kFirstComeFirstServed, defaultEventThreshold_,
            defaultIdleThreshold_);
    enjima::runtime::StreamingJob streamingJob = SetUpBasicWindowedPipeline(executionEngine_, srcOpName, projectOpName,
            statEqJoinOpName, windowOpName, sinkOpName);
    auto jobId = executionEngine_->Submit(streamingJob);
    std::this_thread::sleep_for(std::chrono::seconds(30));
    executionEngine_->Cancel(jobId, std::chrono::seconds(10));

    ValidatePipelineResults(true, profiler_, executionEngine_, srcOpName, projectOpName, statEqJoinOpName, windowOpName,
            sinkOpName);
}
