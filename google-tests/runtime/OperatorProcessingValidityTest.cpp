//
// Created by m34ferna on 12/01/24.
//

#include "enjima/api/data_types/LinearRoadEvent.h"
#include "enjima/memory/MemoryUtil.h"
#include "enjima/operators/FilterOperator.h"
#include "enjima/operators/GenericSinkOperator.h"
#include "enjima/operators/SourceOperator.h"
#include "enjima/runtime/DataStream.h"
#include "google-tests/common/HelperFunctions.h"
#include "google-tests/common/TestSetupHelperFunctions.h"
#include "gtest/gtest.h"

using LinearRoadT = enjima::api::data_types::LinearRoadEvent;
using MemManT = enjima::memory::MemoryManager;
using EngineT = enjima::runtime::ExecutionEngine;
using ProflierT = enjima::metrics::Profiler;
using JobT = enjima::runtime::StreamingJob;
using LRRecordT = enjima::core::Record<LinearRoadT>;
using UniformIntDistParamT = std::uniform_int_distribution<int>::param_type;

class OperatorProcessingValidityTest : public testing::Test {
protected:
    void SetUp() override
    {
        memoryManager_ = new MemManT(maxMemory_, numBlocksPerChunk, numEventsPerBlock_, MemManT::AllocatorType::kBasic);
        profiler_ = new ProflierT(10000, true, "EnjimaOperatorValidityTest");
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

TEST_F(OperatorProcessingValidityTest, InMemoryBatchRunWithSinkAsserts)
{
    enjima::runtime::StreamingJob streamingJob;

    enjima::operators::OperatorID srcOpId = executionEngine_->GetNextOperatorId();
    auto uPtrSrcOp = std::make_unique<InMemoryLinearRoadSourceOperator>(srcOpId, srcOpName);
    uPtrSrcOp->PopulateEventCache(10500);
    streamingJob.AddOperator(std::move(uPtrSrcOp), 0);

    enjima::operators::OperatorID filterOpId = executionEngine_->GetNextOperatorId();
    auto filterNothingFn = [](const LinearRoadT& lrTypeEvent) { return lrTypeEvent.GetDow() > 0; };
    auto uPtrFilterOp = std::make_unique<enjima::operators::FilterOperator<LinearRoadT, decltype(filterNothingFn)>>(
            filterOpId, filterOpName, filterNothingFn);
    streamingJob.AddOperator(std::move(uPtrFilterOp), 0);

    enjima::operators::OperatorID sinkOpId = executionEngine_->GetNextOperatorId();
    auto sinkFn = ValidatingSinkFunction{};
    auto uPtrSinkOp = std::make_unique<enjima::operators::GenericSinkOperator<LinearRoadT, ValidatingSinkFunction>>(
            sinkOpId, sinkOpName, sinkFn);
    streamingJob.AddOperator(std::move(uPtrSinkOp), 0);
    streamingJob.SetProcessingMode(enjima::runtime::StreamingTask::ProcessingMode::kBlockBasedBatch);
    auto jobId = executionEngine_->Submit(streamingJob);
    std::this_thread::sleep_for(std::chrono::seconds(10));

    auto sinkInCounter = profiler_->GetOrCreateCounter(sinkOpName + enjima::metrics::kInCounterSuffix);
    auto filterOutCounter = profiler_->GetOrCreateCounter(filterOpName + enjima::metrics::kOutCounterSuffix);
    auto filterInCounter = profiler_->GetOrCreateCounter(filterOpName + enjima::metrics::kInCounterSuffix);
    auto srcOutCounter = profiler_->GetOrCreateCounter(srcOpName + enjima::metrics::kOutCounterSuffix);

    EXPECT_GE(filterOutCounter->GetCount(), sinkInCounter->GetCount());
    EXPECT_GE(filterInCounter->GetCount(), filterOutCounter->GetCount());
    EXPECT_GE(srcOutCounter->GetCount(), filterInCounter->GetCount());
    EXPECT_GT(srcOutCounter->GetCount(), 0);

    executionEngine_->Cancel(jobId);
    EXPECT_EQ(executionEngine_->GetNumActiveJobs(), 0);

    EXPECT_GT(srcOutCounter->GetCount(), 0);
    EXPECT_EQ(srcOutCounter->GetCount(), filterInCounter->GetCount());
    EXPECT_GE(filterInCounter->GetCount(), filterOutCounter->GetCount());
    EXPECT_EQ(filterOutCounter->GetCount(), sinkInCounter->GetCount());

    std::cout << "Count for src(out) : " << srcOutCounter->GetCount() << std::endl;
    std::cout << "Count for filter(in) : " << filterInCounter->GetCount() << std::endl;
    std::cout << "Count for filter(out) : " << filterOutCounter->GetCount() << std::endl;
    std::cout << "Count for sink(in) : " << sinkInCounter->GetCount() << std::endl;
    std::cout << std::endl;
}

TEST_F(OperatorProcessingValidityTest, WindowlessLinearPipeline)
{
    enjima::runtime::StreamingJob streamingJob =
            SetUpBasicWindowlessPipeline(executionEngine_, srcOpName, projectOpName, statEqJoinOpName, sinkOpName,
                    enjima::runtime::StreamingTask::ProcessingMode::kBlockBasedSingle);
    auto jobId = executionEngine_->Submit(streamingJob);
    std::this_thread::sleep_for(std::chrono::seconds(300));
    executionEngine_->Cancel(jobId);

    ValidatePipelineResults(false, profiler_, executionEngine_, srcOpName, projectOpName, statEqJoinOpName,
            windowOpName, sinkOpName);
}

TEST_F(OperatorProcessingValidityTest, BatchedWindowlessLinearPipeline)
{
    enjima::runtime::StreamingJob streamingJob =
            SetUpBasicWindowlessPipeline(executionEngine_, srcOpName, projectOpName, statEqJoinOpName, sinkOpName,
                    enjima::runtime::StreamingTask::ProcessingMode::kBlockBasedBatch);
    auto jobId = executionEngine_->Submit(streamingJob);
    std::this_thread::sleep_for(std::chrono::seconds(300));
    executionEngine_->Cancel(jobId);

    ValidatePipelineResults(false, profiler_, executionEngine_, srcOpName, projectOpName, statEqJoinOpName,
            windowOpName, sinkOpName);
}

TEST_F(OperatorProcessingValidityTest, WindowedLinearPipeline)
{
    enjima::runtime::StreamingJob streamingJob =
            SetUpBasicWindowedPipeline(executionEngine_, srcOpName, projectOpName, statEqJoinOpName, windowOpName,
                    sinkOpName, enjima::runtime::StreamingTask::ProcessingMode::kBlockBasedSingle);
    auto jobId = executionEngine_->Submit(streamingJob);
    std::this_thread::sleep_for(std::chrono::seconds(30));
    executionEngine_->Cancel(jobId);

    ValidatePipelineResults(true, profiler_, executionEngine_, srcOpName, projectOpName, statEqJoinOpName, windowOpName,
            sinkOpName);
}

TEST_F(OperatorProcessingValidityTest, BatchedWindowedLinearPipeline)
{
    enjima::runtime::StreamingJob streamingJob =
            SetUpBasicWindowedPipeline(executionEngine_, srcOpName, projectOpName, statEqJoinOpName, windowOpName,
                    sinkOpName, enjima::runtime::StreamingTask::ProcessingMode::kBlockBasedBatch);
    auto jobId = executionEngine_->Submit(streamingJob);
    std::this_thread::sleep_for(std::chrono::seconds(30));
    executionEngine_->Cancel(jobId);

    ValidatePipelineResults(true, profiler_, executionEngine_, srcOpName, projectOpName, statEqJoinOpName, windowOpName,
            sinkOpName);
}