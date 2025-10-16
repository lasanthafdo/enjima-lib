//
// Created by m34ferna on 12/01/24.
//

#include "enjima/memory/MemoryUtil.h"
#include "enjima/operators/FilterOperator.h"
#include "enjima/operators/GenericSinkOperator.h"
#include "enjima/operators/NoOpSinkOperator.h"
#include "enjima/operators/SourceOperator.h"
#include "enjima/runtime/ExecutionEngine.h"
#include "enjima/runtime/StreamingJob.h"
#include "google-tests/common/InputGenerationHelper.h"
#include "gtest/gtest.h"

using MemManT = enjima::memory::MemoryManager;
using EngineT = enjima::runtime::ExecutionEngine;
using ProflierT = enjima::metrics::Profiler;
using JobT = enjima::runtime::StreamingJob;
using UniformIntDistParamT = std::uniform_int_distribution<int>::param_type;

class GenericOperatorTest : public testing::Test {
protected:
    void SetUp() override
    {
        memoryManager_ =
                new MemManT(maxMemory_, numBlocksPerChunk, numEventsPerBlock_, MemManT ::AllocatorType::kBasic);
        profiler_ = new ProflierT(10000, true, "EnjimaOperatorTest");
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
    const size_t maxMemory_ = enjima::memory::MegaBytes(64);
    const size_t numBlocksPerChunk = 16;
    const int32_t numEventsPerBlock_ = 1000;
};

class ValidatingSinkFunction : public enjima::api::SinkFunction<LinearRoadT> {
public:
    void Execute(uint64_t timestamp, enjima::api::data_types::LinearRoadEvent inputEvent) override
    {
        assert(inputEvent.GetTimestamp() < 11000 && inputEvent.GetDow() <= 7);
    }
};

TEST_F(GenericOperatorTest, Init)
{
    EXPECT_EQ(memoryManager_->GetMaxMemory(), maxMemory_);
    EXPECT_EQ(memoryManager_->GetDefaultNumEventsPerBlock(), numEventsPerBlock_);
}

TEST_F(GenericOperatorTest, BasicPipeline)
{
    enjima::runtime::StreamingJob streamingJob;

    enjima::operators::OperatorID srcOpId = executionEngine_->GetNextOperatorId();
    auto uPtrSrcOp = std::make_unique<RateLimitedGeneratingLRSource>(srcOpId, "src", 10'000'000);
    streamingJob.AddOperator(std::move(uPtrSrcOp), 0);

    enjima::operators::OperatorID filterOpId = executionEngine_->GetNextOperatorId();
    auto filterFn = [](const LinearRoadT& lrTypeEvent) { return lrTypeEvent.GetDow() > 5; };
    auto uPtrFilterOp = std::make_unique<enjima::operators::FilterOperator<LinearRoadT, decltype(filterFn)>>(filterOpId,
            "dowFilter", filterFn);
    streamingJob.AddOperator(std::move(uPtrFilterOp), 0);

    enjima::operators::OperatorID sinkOpId = executionEngine_->GetNextOperatorId();
    auto uPtrSinkOp = std::make_unique<enjima::operators::NoOpSinkOperator<LinearRoadT>>(sinkOpId, "noOpSink");
    streamingJob.AddOperator(std::move(uPtrSinkOp), 0);

    auto jobId = executionEngine_->Submit(streamingJob);
    EXPECT_EQ(executionEngine_->GetNumActiveJobs(), 1);

    executionEngine_->Cancel(jobId);
    EXPECT_EQ(executionEngine_->GetNumActiveJobs(), 0);
}

TEST_F(GenericOperatorTest, BasicRunForTenSeconds)
{
    enjima::runtime::StreamingJob streamingJob;

    enjima::operators::OperatorID srcOpId = executionEngine_->GetNextOperatorId();
    auto uPtrSrcOp = std::make_unique<RateLimitedGeneratingLRSource>(srcOpId, "src", 100'000);
    streamingJob.AddOperator(std::move(uPtrSrcOp), 0);

    enjima::operators::OperatorID filterOpId = executionEngine_->GetNextOperatorId();
    auto filterFn = [](const LinearRoadT& lrTypeEvent) { return lrTypeEvent.GetDow() > 5; };
    auto uPtrFilterOp = std::make_unique<enjima::operators::FilterOperator<LinearRoadT, decltype(filterFn)>>(filterOpId,
            "dowFilter", filterFn);
    streamingJob.AddOperator(std::move(uPtrFilterOp), 0);

    enjima::operators::OperatorID sinkOpId = executionEngine_->GetNextOperatorId();
    auto uPtrSinkOp = std::make_unique<enjima::operators::NoOpSinkOperator<LinearRoadT>>(sinkOpId, "noOpSink");
    streamingJob.AddOperator(std::move(uPtrSinkOp), 0);
    auto jobId = executionEngine_->Submit(streamingJob);
    std::this_thread::sleep_for(std::chrono::seconds(10));

    std::unsigned_integral auto sinkInCnt = profiler_->GetOrCreateCounter("noOpSink_in_counter")->GetCount();
    std::unsigned_integral auto filterOutCnt = profiler_->GetOrCreateCounter("dowFilter_out_counter")->GetCount();
    std::unsigned_integral auto filterInCnt = profiler_->GetOrCreateCounter("dowFilter_in_counter")->GetCount();
    std::unsigned_integral auto srcOutCount = profiler_->GetOrCreateCounter("src_out_counter")->GetCount();

    EXPECT_GE(filterOutCnt, sinkInCnt);
    EXPECT_GT(filterInCnt, filterOutCnt);
    EXPECT_GE(srcOutCount, filterInCnt);
    EXPECT_GT(srcOutCount, 0);

    executionEngine_->Cancel(jobId);
    EXPECT_EQ(executionEngine_->GetNumActiveJobs(), 0);

    srcOutCount = profiler_->GetOrCreateCounter("src_out_counter")->GetCount();
    filterInCnt = profiler_->GetOrCreateCounter("dowFilter_in_counter")->GetCount();
    filterOutCnt = profiler_->GetOrCreateCounter("dowFilter_out_counter")->GetCount();
    sinkInCnt = profiler_->GetOrCreateCounter("noOpSink_in_counter")->GetCount();

    EXPECT_GT(srcOutCount, 0);
    EXPECT_EQ(srcOutCount, filterInCnt);
    EXPECT_GT(filterInCnt, filterOutCnt);
    EXPECT_EQ(filterOutCnt, sinkInCnt);

    std::cout << "Count for src(out) : " << srcOutCount << std::endl;
    std::cout << "Count for filter(in) : " << filterInCnt << std::endl;
    std::cout << "Count for filter(out) : " << filterOutCnt << std::endl;
    std::cout << "Count for sink(in) : " << sinkInCnt << std::endl;
    std::cout << std::endl;
}

TEST_F(GenericOperatorTest, BasicInMemoryRunForTenSeconds)
{
    enjima::runtime::StreamingJob streamingJob;

    enjima::operators::OperatorID srcOpId = executionEngine_->GetNextOperatorId();
    auto uPtrSrcOp = std::make_unique<InMemoryLinearRoadSourceOperator>(srcOpId, "src");
    uPtrSrcOp->PopulateEventCache(10500);
    streamingJob.AddOperator(std::move(uPtrSrcOp), 0);

    enjima::operators::OperatorID filterOpId = executionEngine_->GetNextOperatorId();
    auto filterFn = [](const LinearRoadT& lrTypeEvent) { return lrTypeEvent.GetDow() > 5; };
    auto uPtrFilterOp = std::make_unique<enjima::operators::FilterOperator<LinearRoadT, decltype(filterFn)>>(filterOpId,
            "dowFilter", filterFn);
    streamingJob.AddOperator(std::move(uPtrFilterOp), 0);

    enjima::operators::OperatorID sinkOpId = executionEngine_->GetNextOperatorId();
    auto uPtrSinkOp = std::make_unique<enjima::operators::NoOpSinkOperator<LinearRoadT>>(sinkOpId, "noOpSink");
    streamingJob.AddOperator(std::move(uPtrSinkOp), 0);
    auto jobId = executionEngine_->Submit(streamingJob);
    std::this_thread::sleep_for(std::chrono::seconds(10));

    std::unsigned_integral auto sinkInCnt = profiler_->GetOrCreateCounter("noOpSink_in_counter")->GetCount();
    std::unsigned_integral auto filterOutCnt = profiler_->GetOrCreateCounter("dowFilter_out_counter")->GetCount();
    std::unsigned_integral auto filterInCnt = profiler_->GetOrCreateCounter("dowFilter_in_counter")->GetCount();
    std::unsigned_integral auto srcOutCount = profiler_->GetOrCreateCounter("src_out_counter")->GetCount();

    EXPECT_GE(filterOutCnt, sinkInCnt);
    EXPECT_GT(filterInCnt, filterOutCnt);
    EXPECT_GE(srcOutCount, filterInCnt);
    EXPECT_GT(srcOutCount, 0);

    executionEngine_->Cancel(jobId);
    EXPECT_EQ(executionEngine_->GetNumActiveJobs(), 0);

    srcOutCount = profiler_->GetOrCreateCounter("src_out_counter")->GetCount();
    filterInCnt = profiler_->GetOrCreateCounter("dowFilter_in_counter")->GetCount();
    filterOutCnt = profiler_->GetOrCreateCounter("dowFilter_out_counter")->GetCount();
    sinkInCnt = profiler_->GetOrCreateCounter("noOpSink_in_counter")->GetCount();

    EXPECT_GT(srcOutCount, 0);
    EXPECT_EQ(srcOutCount, filterInCnt);
    EXPECT_GT(filterInCnt, filterOutCnt);
    EXPECT_EQ(filterOutCnt, sinkInCnt);

    std::cout << "Count for src(out) : " << srcOutCount << std::endl;
    std::cout << "Count for filter(in) : " << filterInCnt << std::endl;
    std::cout << "Count for filter(out) : " << filterOutCnt << std::endl;
    std::cout << "Count for sink(in) : " << sinkInCnt << std::endl;
    std::cout << std::endl;
}

TEST_F(GenericOperatorTest, BasicInMemoryBatchRunForTenSeconds)
{
    enjima::runtime::StreamingJob streamingJob;

    enjima::operators::OperatorID srcOpId = executionEngine_->GetNextOperatorId();
    auto uPtrSrcOp = std::make_unique<InMemoryLinearRoadSourceOperator>(srcOpId, "src");
    uPtrSrcOp->PopulateEventCache(10500);
    streamingJob.AddOperator(std::move(uPtrSrcOp), 0);

    enjima::operators::OperatorID filterOpId = executionEngine_->GetNextOperatorId();
    auto filterFn = [](const LinearRoadT& lrTypeEvent) { return lrTypeEvent.GetDow() > 5; };
    auto uPtrFilterOp = std::make_unique<enjima::operators::FilterOperator<LinearRoadT, decltype(filterFn)>>(filterOpId,
            "dowFilter", filterFn);
    streamingJob.AddOperator(std::move(uPtrFilterOp), 0);

    enjima::operators::OperatorID sinkOpId = executionEngine_->GetNextOperatorId();
    auto uPtrSinkOp = std::make_unique<enjima::operators::NoOpSinkOperator<LinearRoadT>>(sinkOpId, "noOpSink");
    streamingJob.AddOperator(std::move(uPtrSinkOp), 0);
    streamingJob.SetProcessingMode(enjima::runtime::StreamingTask::ProcessingMode::kBlockBasedBatch);
    auto jobId = executionEngine_->Submit(streamingJob);
    std::this_thread::sleep_for(std::chrono::seconds(10));

    std::unsigned_integral auto sinkInCnt = profiler_->GetOrCreateCounter("noOpSink_in_counter")->GetCount();
    std::unsigned_integral auto filterOutCnt = profiler_->GetOrCreateCounter("dowFilter_out_counter")->GetCount();
    std::unsigned_integral auto filterInCnt = profiler_->GetOrCreateCounter("dowFilter_in_counter")->GetCount();
    std::unsigned_integral auto srcOutCount = profiler_->GetOrCreateCounter("src_out_counter")->GetCount();

    EXPECT_GE(filterOutCnt, sinkInCnt);
    EXPECT_GT(filterInCnt, filterOutCnt);
    EXPECT_GE(srcOutCount, filterInCnt);
    EXPECT_GT(srcOutCount, 0);

    executionEngine_->Cancel(jobId);
    EXPECT_EQ(executionEngine_->GetNumActiveJobs(), 0);

    srcOutCount = profiler_->GetOrCreateCounter("src_out_counter")->GetCount();
    filterInCnt = profiler_->GetOrCreateCounter("dowFilter_in_counter")->GetCount();
    filterOutCnt = profiler_->GetOrCreateCounter("dowFilter_out_counter")->GetCount();
    sinkInCnt = profiler_->GetOrCreateCounter("noOpSink_in_counter")->GetCount();

    EXPECT_GT(srcOutCount, 0);
    EXPECT_EQ(srcOutCount, filterInCnt);
    EXPECT_GT(filterInCnt, filterOutCnt);
    EXPECT_EQ(filterOutCnt, sinkInCnt);

    std::cout << "Count for src(out) : " << srcOutCount << std::endl;
    std::cout << "Count for filter(in) : " << filterInCnt << std::endl;
    std::cout << "Count for filter(out) : " << filterOutCnt << std::endl;
    std::cout << "Count for sink(in) : " << sinkInCnt << std::endl;
    std::cout << std::endl;
}