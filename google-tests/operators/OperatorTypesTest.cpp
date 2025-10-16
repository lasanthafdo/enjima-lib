//
// Created by m34ferna on 12/01/24.
//

#include "enjima/memory/MemoryUtil.h"
#include "enjima/operators/FilterOperator.h"
#include "enjima/operators/FixedEventTimeWindowCoGroupOperator.h"
#include "enjima/operators/FixedEventTimeWindowJoinOperator.h"
#include "enjima/operators/FixedEventTimeWindowOperator.h"
#include "enjima/operators/GenericSinkOperator.h"
#include "enjima/operators/MapOperator.h"
#include "enjima/operators/NoOpSinkOperator.h"
#include "enjima/operators/SlidingEventTimeWindowOperator.h"
#include "enjima/operators/StaticEquiJoinOperator.h"
#include "enjima/operators/StaticJoinOperator.h"
#include "enjima/runtime/ExecutionEngine.h"
#include "enjima/runtime/StreamingJob.h"
#include "google-tests/common/HelperFunctions.h"
#include "gtest/gtest.h"

using LinearRoadT = enjima::api::data_types::LinearRoadEvent;
using MemManT = enjima::memory::MemoryManager;
using EngineT = enjima::runtime::ExecutionEngine;
using ProflierT = enjima::metrics::Profiler;
using JobT = enjima::runtime::StreamingJob;
using LRRecordT = enjima::core::Record<LinearRoadT>;
using YSBProjT = enjima::api::data_types::YSBProjectedEvent;
using YSBWinT = enjima::api::data_types::YSBWinEmitEvent;
using YSBCampT = enjima::api::data_types::YSBCampaignAdEvent;

class OperatorTypesTest : public testing::Test {
protected:
    void SetUp() override
    {
        memoryManager_ =
                new MemManT(maxMemory_, numBlocksPerChunk, numEventsPerBlock_, MemManT ::AllocatorType::kBasic);
        profiler_ = new ProflierT(10000, true, "EnjimaOperatorTypesTest");
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
    const size_t numBlocksPerChunk = 16;
    const int32_t numEventsPerBlock_ = 1000;
};

// ================ Tests start from here ==============

TEST_F(OperatorTypesTest, FilterOperatorTest)
{
    enjima::runtime::StreamingJob streamingJob;

    enjima::operators::OperatorID srcOpId = executionEngine_->GetNextOperatorId();
    std::string srcOpName = "src";
    auto uPtrSrcOp = std::make_unique<InMemoryYSBSourceOperator>(srcOpId, srcOpName);
    uPtrSrcOp->PopulateEventCache(1048576, 100, 10);
    streamingJob.AddOperator(std::move(uPtrSrcOp), 0);

    enjima::operators::OperatorID filterOpId = executionEngine_->GetNextOperatorId();
    std::string filterOpName = "eventFilter";
    auto filterFn = [](const YSBAdT& ysbAdEvent) { return ysbAdEvent.GetEventType() == 0; };
    auto uPtrFilterOp = std::make_unique<enjima::operators::FilterOperator<YSBAdT, decltype(filterFn)>>(filterOpId,
            filterOpName, filterFn);
    streamingJob.AddOperator(std::move(uPtrFilterOp), 0);

    enjima::operators::OperatorID sinkOpId = executionEngine_->GetNextOperatorId();
    std::string sinkOpName = "genericSink";
    auto sinkFn = YSBAssertingSinkFunction{};
    auto uPtrSinkOp = std::make_unique<enjima::operators::GenericSinkOperator<YSBAdT, YSBAssertingSinkFunction>>(
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

TEST_F(OperatorTypesTest, MapOperatorTest)
{
    enjima::runtime::StreamingJob streamingJob;

    enjima::operators::OperatorID srcOpId = executionEngine_->GetNextOperatorId();
    std::string srcOpName = "src";
    auto uPtrSrcOp = std::make_unique<InMemoryYSBSourceOperator>(srcOpId, srcOpName);
    uPtrSrcOp->PopulateEventCache(1048576, 100, 10);
    streamingJob.AddOperator(std::move(uPtrSrcOp), 0);

    enjima::operators::OperatorID projectOpId = executionEngine_->GetNextOperatorId();
    std::string projectOpName = "project";
    auto uPtrProjectOp = std::make_unique<enjima::operators::MapOperator<YSBAdT, YSBProjT, YSBProjectFunction>>(
            projectOpId, projectOpName, YSBProjectFunction{});
    streamingJob.AddOperator(std::move(uPtrProjectOp), 0);

    enjima::operators::OperatorID sinkOpId = executionEngine_->GetNextOperatorId();
    std::string sinkOpName = "genericSink";
    auto sinkFn = NoOpYSBSinkFunction<YSBProjT>{};
    auto uPtrSinkOp = std::make_unique<enjima::operators::GenericSinkOperator<YSBProjT, NoOpYSBSinkFunction<YSBProjT>>>(
            sinkOpId, sinkOpName, sinkFn);
    streamingJob.AddOperator(std::move(uPtrSinkOp), 0);
    streamingJob.SetProcessingMode(enjima::runtime::StreamingTask::ProcessingMode::kBlockBasedBatch);
    auto jobId = executionEngine_->Submit(streamingJob);

    std::this_thread::sleep_for(std::chrono::seconds(10));

    auto sinkInCounter = profiler_->GetOrCreateCounter(sinkOpName + enjima::metrics::kInCounterSuffix);
    auto projectOutCounter = profiler_->GetOrCreateCounter(projectOpName + enjima::metrics::kOutCounterSuffix);
    auto projectInCounter = profiler_->GetOrCreateCounter(projectOpName + enjima::metrics::kInCounterSuffix);
    auto srcOutCounter = profiler_->GetOrCreateCounter(srcOpName + enjima::metrics::kOutCounterSuffix);

    EXPECT_GE(projectOutCounter->GetCount(), sinkInCounter->GetCount());
    EXPECT_GE(projectInCounter->GetCount(), projectOutCounter->GetCount());
    EXPECT_GE(srcOutCounter->GetCount(), projectInCounter->GetCount());
    EXPECT_GT(srcOutCounter->GetCount(), 0);

    executionEngine_->Cancel(jobId);
    EXPECT_EQ(executionEngine_->GetNumActiveJobs(), 0);

    EXPECT_GT(srcOutCounter->GetCount(), 0);
    EXPECT_EQ(srcOutCounter->GetCount(), projectInCounter->GetCount());
    EXPECT_GE(projectInCounter->GetCount(), projectOutCounter->GetCount());
    EXPECT_EQ(projectOutCounter->GetCount(), sinkInCounter->GetCount());

    std::cout << "Count for " << srcOpName << "(out) : " << srcOutCounter->GetCount() << std::endl;
    std::cout << "Count for " << projectOpName << "(in) : " << projectInCounter->GetCount() << std::endl;
    std::cout << "Count for " << projectOpName << "(out) : " << projectOutCounter->GetCount() << std::endl;
    std::cout << "Count for " << sinkOpName << "(in) : " << sinkInCounter->GetCount() << std::endl;
    std::cout << std::endl;
}

TEST_F(OperatorTypesTest, MultiOutMapOperatorTest)
{
    enjima::runtime::StreamingJob streamingJob;

    enjima::operators::OperatorID srcOpId = executionEngine_->GetNextOperatorId();
    std::string srcOpName = "src";
    auto uPtrSrcOp = std::make_unique<InMemoryYSBSourceOperator>(srcOpId, srcOpName);
    uPtrSrcOp->PopulateEventCache(1048576, 100, 10);
    streamingJob.AddOperator(std::move(uPtrSrcOp));

    enjima::operators::OperatorID projectOpId = executionEngine_->GetNextOperatorId();
    std::string projectOpName = "project";
    auto uPtrProjectOp = std::make_unique<enjima::operators::MapOperator<YSBAdT, YSBProjT, YSBProjectFunction, 4>>(
            projectOpId, projectOpName, YSBProjectFunction{});
    streamingJob.AddOperator(std::move(uPtrProjectOp), srcOpId);

    std::string sinkOpName = "genericSink";
    for (auto i = 0; i < 4; i++) {
        enjima::operators::OperatorID sinkOpId = executionEngine_->GetNextOperatorId();
        auto sinkFn = NoOpYSBSinkFunction<YSBProjT>{};
        auto uPtrSinkOp =
                std::make_unique<enjima::operators::GenericSinkOperator<YSBProjT, NoOpYSBSinkFunction<YSBProjT>>>(
                        sinkOpId, sinkOpName + std::to_string(i + 1), sinkFn);
        streamingJob.AddOperator(std::move(uPtrSinkOp), projectOpId);
    }

    streamingJob.SetProcessingMode(enjima::runtime::StreamingTask::ProcessingMode::kBlockBasedBatch);
    auto jobId = executionEngine_->Submit(streamingJob);

    std::this_thread::sleep_for(std::chrono::seconds(10));

    std::vector<enjima::metrics::Counter<uint64_t>*> sinkInCounterVec;
    sinkInCounterVec.reserve(4);
    for (auto i = 0; i < 4; i++) {
        sinkInCounterVec.emplace_back(
                profiler_->GetOrCreateCounter(sinkOpName + std::to_string(i + 1) + enjima::metrics::kInCounterSuffix));
    }
    auto projectOutCounter = profiler_->GetOrCreateCounter(projectOpName + enjima::metrics::kOutCounterSuffix);
    auto projectInCounter = profiler_->GetOrCreateCounter(projectOpName + enjima::metrics::kInCounterSuffix);
    auto srcOutCounter = profiler_->GetOrCreateCounter(srcOpName + enjima::metrics::kOutCounterSuffix);

    for (auto i = 0; i < 4; i++) {
        EXPECT_GE(projectOutCounter->GetCount(), sinkInCounterVec[i]->GetCount());
    }
    EXPECT_GE(projectInCounter->GetCount(), projectOutCounter->GetCount());
    EXPECT_GE(srcOutCounter->GetCount(), projectInCounter->GetCount());
    EXPECT_GT(srcOutCounter->GetCount(), 0);

    executionEngine_->Cancel(jobId);
    EXPECT_EQ(executionEngine_->GetNumActiveJobs(), 0);

    EXPECT_GT(srcOutCounter->GetCount(), 0);
    EXPECT_EQ(srcOutCounter->GetCount(), projectInCounter->GetCount());
    EXPECT_GE(projectInCounter->GetCount(), projectOutCounter->GetCount());
    for (auto i = 0; i < 4; i++) {
        EXPECT_EQ(projectOutCounter->GetCount(), sinkInCounterVec[i]->GetCount());
    }

    std::cout << "Count for " << srcOpName << "(out) : " << srcOutCounter->GetCount() << std::endl;
    std::cout << "Count for " << projectOpName << "(in) : " << projectInCounter->GetCount() << std::endl;
    std::cout << "Count for " << projectOpName << "(out) : " << projectOutCounter->GetCount() << std::endl;
    for (auto i = 0; i < 4; i++) {
        std::cout << "Count for " << sinkOpName << i + 1 << "(in) : " << sinkInCounterVec[i]->GetCount() << std::endl;
    }
    std::cout << std::endl;
}

TEST_F(OperatorTypesTest, DISABLED_StaticJoinOperatorTest)
{
    enjima::runtime::StreamingJob streamingJob;

    enjima::operators::OperatorID srcOpId = executionEngine_->GetNextOperatorId();
    std::string srcOpName = "src";
    auto uPtrSrcOp = std::make_unique<InMemoryYSBSourceOperator>(srcOpId, srcOpName);
    uPtrSrcOp->PopulateEventCache(1048576, 100, 10);

    enjima::operators::OperatorID projectOpId = executionEngine_->GetNextOperatorId();
    std::string projectOpName = "project";
    auto uPtrProjectOp = std::make_unique<enjima::operators::MapOperator<YSBAdT, YSBProjT, YSBProjectFunction>>(
            projectOpId, projectOpName, YSBProjectFunction{});

    enjima::operators::OperatorID statJoinOpId = executionEngine_->GetNextOperatorId();
    std::string statJoinOpName = "staticJoin";
    auto& adIdCampaignIdMap = uPtrSrcOp->GetAdIdToCampaignIdMap();
    std::vector<std::pair<uint64_t, uint64_t>> adIdCampaignIdData;
    std::copy(adIdCampaignIdMap.cbegin(), adIdCampaignIdMap.cend(), std::back_inserter(adIdCampaignIdData));
    auto uPtrStatJoinOp = std::make_unique<enjima::operators::StaticJoinOperator<YSBProjT,
            std::pair<uint64_t, uint64_t>, YSBCampT, YSBJoinPredicate, YSBJoinFunction>>(statJoinOpId, statJoinOpName,
            YSBJoinPredicate{}, YSBJoinFunction{}, adIdCampaignIdData);

    enjima::operators::OperatorID sinkOpId = executionEngine_->GetNextOperatorId();
    std::string sinkOpName = "noOpSink";
    auto uPtrSinkOp = std::make_unique<enjima::operators::NoOpSinkOperator<YSBCampT>>(sinkOpId, sinkOpName);

    streamingJob.AddOperator(std::move(uPtrSrcOp), 0);
    streamingJob.AddOperator(std::move(uPtrProjectOp), 0);
    streamingJob.AddOperator(std::move(uPtrStatJoinOp), 0);
    streamingJob.AddOperator(std::move(uPtrSinkOp), 0);
    streamingJob.SetProcessingMode(enjima::runtime::StreamingTask::ProcessingMode::kBlockBasedSingle);
    auto jobId = executionEngine_->Submit(streamingJob);
    std::this_thread::sleep_for(std::chrono::seconds(10));

    auto sinkInCounter = profiler_->GetOrCreateCounter(sinkOpName + enjima::metrics::kInCounterSuffix);
    auto joinOutCounter = profiler_->GetOrCreateCounter(statJoinOpName + enjima::metrics::kOutCounterSuffix);
    auto joinInCounter = profiler_->GetOrCreateCounter(statJoinOpName + enjima::metrics::kInCounterSuffix);
    auto srcOutCounter = profiler_->GetOrCreateCounter(srcOpName + enjima::metrics::kOutCounterSuffix);

    EXPECT_GE(joinOutCounter->GetCount(), sinkInCounter->GetCount());
    EXPECT_GE(joinInCounter->GetCount(), joinOutCounter->GetCount());
    EXPECT_GE(srcOutCounter->GetCount(), joinInCounter->GetCount());
    EXPECT_GT(srcOutCounter->GetCount(), 0);

    executionEngine_->Cancel(jobId, std::chrono::seconds{30});
    EXPECT_EQ(executionEngine_->GetNumActiveJobs(), 0);

    EXPECT_GT(srcOutCounter->GetCount(), 0);
    EXPECT_EQ(srcOutCounter->GetCount(), joinInCounter->GetCount());
    EXPECT_GE(joinInCounter->GetCount(), joinOutCounter->GetCount());
    EXPECT_EQ(joinOutCounter->GetCount(), sinkInCounter->GetCount());

    std::cout << "Count for " << srcOpName << "(out) : " << srcOutCounter->GetCount() << std::endl;
    std::cout << "Count for " << statJoinOpName << "(in) : " << joinInCounter->GetCount() << std::endl;
    std::cout << "Count for " << statJoinOpName << "(out) : " << joinOutCounter->GetCount() << std::endl;
    std::cout << "Count for " << sinkOpName << "(in) : " << sinkInCounter->GetCount() << std::endl;
    std::cout << std::endl;
}

TEST_F(OperatorTypesTest, StaticEquiJoinOperatorTest)
{
    enjima::runtime::StreamingJob streamingJob;

    enjima::operators::OperatorID srcOpId = executionEngine_->GetNextOperatorId();
    std::string srcOpName = "src";
    auto uPtrSrcOp = std::make_unique<InMemoryYSBSourceOperator>(srcOpId, srcOpName);
    uPtrSrcOp->PopulateEventCache(1048576, 100, 10);

    enjima::operators::OperatorID projectOpId = executionEngine_->GetNextOperatorId();
    std::string projectOpName = "project";
    auto uPtrProjectOp = std::make_unique<enjima::operators::MapOperator<YSBAdT, YSBProjT, YSBProjectFunction>>(
            projectOpId, projectOpName, YSBProjectFunction{});

    enjima::operators::OperatorID statJoinOpId = executionEngine_->GetNextOperatorId();
    std::string statEquiJoinOpName = "staticEquiJoin";
    auto adIdCampaignIdMap = uPtrSrcOp->GetAdIdToCampaignIdMap();
    auto uPtrStatEquiJoinOp = std::make_unique<enjima::operators::StaticEquiJoinOperator<YSBProjT, uint64_t, YSBCampT,
            uint64_t, YSBKeyExtractFunction, YSBEqJoinFunction>>(statJoinOpId, statEquiJoinOpName,
            YSBKeyExtractFunction{}, YSBEqJoinFunction{}, adIdCampaignIdMap);

    enjima::operators::OperatorID sinkOpId = executionEngine_->GetNextOperatorId();
    std::string sinkOpName = "noOpSink";
    auto uPtrSinkOp = std::make_unique<enjima::operators::NoOpSinkOperator<YSBCampT>>(sinkOpId, sinkOpName);

    streamingJob.AddOperator(std::move(uPtrSrcOp), 0);
    streamingJob.AddOperator(std::move(uPtrProjectOp), 0);
    streamingJob.AddOperator(std::move(uPtrStatEquiJoinOp), 0);
    streamingJob.AddOperator(std::move(uPtrSinkOp), 0);
    streamingJob.SetProcessingMode(enjima::runtime::StreamingTask::ProcessingMode::kBlockBasedBatch);
    auto jobId = executionEngine_->Submit(streamingJob);
    std::this_thread::sleep_for(std::chrono::seconds(10));

    auto sinkInCounter = profiler_->GetOrCreateCounter(sinkOpName + enjima::metrics::kInCounterSuffix);
    auto joinOutCounter = profiler_->GetOrCreateCounter(statEquiJoinOpName + enjima::metrics::kOutCounterSuffix);
    auto joinInCounter = profiler_->GetOrCreateCounter(statEquiJoinOpName + enjima::metrics::kInCounterSuffix);
    auto srcOutCounter = profiler_->GetOrCreateCounter(srcOpName + enjima::metrics::kOutCounterSuffix);

    EXPECT_GE(joinOutCounter->GetCount(), sinkInCounter->GetCount());
    EXPECT_GE(joinInCounter->GetCount(), joinOutCounter->GetCount());
    EXPECT_GE(srcOutCounter->GetCount(), joinInCounter->GetCount());
    EXPECT_GT(srcOutCounter->GetCount(), 0);

    executionEngine_->Cancel(jobId);
    EXPECT_EQ(executionEngine_->GetNumActiveJobs(), 0);

    EXPECT_GT(srcOutCounter->GetCount(), 0);
    EXPECT_EQ(srcOutCounter->GetCount(), joinInCounter->GetCount());
    EXPECT_GE(joinInCounter->GetCount(), joinOutCounter->GetCount());
    EXPECT_EQ(joinOutCounter->GetCount(), sinkInCounter->GetCount());

    std::cout << "Count for " << srcOpName << "(out) : " << srcOutCounter->GetCount() << std::endl;
    std::cout << "Count for " << statEquiJoinOpName << "(in) : " << joinInCounter->GetCount() << std::endl;
    std::cout << "Count for " << statEquiJoinOpName << "(out) : " << joinOutCounter->GetCount() << std::endl;
    std::cout << "Count for " << sinkOpName << "(in) : " << sinkInCounter->GetCount() << std::endl;
    std::cout << std::endl;
}

TEST_F(OperatorTypesTest, FixedEventTimeWindowOperatorTest)
{
    enjima::runtime::StreamingJob streamingJob;

    enjima::operators::OperatorID srcOpId = executionEngine_->GetNextOperatorId();
    std::string srcOpName = "src";
    auto uPtrSrcOp = std::make_unique<InMemoryYSBSourceOperator>(srcOpId, srcOpName);
    uPtrSrcOp->PopulateEventCache(1048576, 100, 10);
    streamingJob.AddOperator(std::move(uPtrSrcOp), 0);

    enjima::operators::OperatorID windowOpId = executionEngine_->GetNextOperatorId();
    std::string windowOpName = "timeWindow";
    auto uPtrWindowOp = std::make_unique<
            enjima::operators::FixedEventTimeWindowOperator<YSBAdT, YSBWinT, true, YSBAdTypeAggFunction>>(windowOpId,
            windowOpName, YSBAdTypeAggFunction{}, std::chrono::seconds(10));
    streamingJob.AddOperator(std::move(uPtrWindowOp), 0);

    enjima::operators::OperatorID sinkOpId = executionEngine_->GetNextOperatorId();
    std::string sinkOpName = "genericSink";
    auto sinkFn = NoOpYSBWinSinkFunction{};
    auto uPtrSinkOp = std::make_unique<enjima::operators::GenericSinkOperator<YSBWinT, NoOpYSBWinSinkFunction>>(
            sinkOpId, sinkOpName, sinkFn);
    streamingJob.AddOperator(std::move(uPtrSinkOp), 0);

    auto jobId = executionEngine_->Submit(streamingJob);
    std::this_thread::sleep_for(std::chrono::seconds(30));

    auto sinkInCounter = profiler_->GetOrCreateCounter(sinkOpName + enjima::metrics::kInCounterSuffix);
    auto winOutCounter = profiler_->GetOrCreateCounter(windowOpName + enjima::metrics::kOutCounterSuffix);
    auto winInCounter = profiler_->GetOrCreateCounter(windowOpName + enjima::metrics::kInCounterSuffix);
    auto srcOutCounter = profiler_->GetOrCreateCounter(srcOpName + enjima::metrics::kOutCounterSuffix);

    EXPECT_GE(winOutCounter->GetCount(), sinkInCounter->GetCount());
    EXPECT_GE(winInCounter->GetCount(), winOutCounter->GetCount());
    EXPECT_GE(srcOutCounter->GetCount(), winInCounter->GetCount());
    EXPECT_GT(srcOutCounter->GetCount(), 0);

    executionEngine_->Cancel(jobId);
    EXPECT_EQ(executionEngine_->GetNumActiveJobs(), 0);

    EXPECT_GT(srcOutCounter->GetCount(), 0);
    EXPECT_EQ(srcOutCounter->GetCount(), winInCounter->GetCount());
    EXPECT_GE(winInCounter->GetCount(), winOutCounter->GetCount());
    EXPECT_EQ(winOutCounter->GetCount(), sinkInCounter->GetCount());

    std::cout << "Count for " << srcOpName << "(out) : " << srcOutCounter->GetCount() << std::endl;
    std::cout << "Count for " << windowOpName << "(in) : " << winInCounter->GetCount() << std::endl;
    std::cout << "Count for " << windowOpName << "(out) : " << winOutCounter->GetCount() << std::endl;
    std::cout << "Count for " << sinkOpName << "(in) : " << sinkInCounter->GetCount() << std::endl;
    std::cout << std::endl;
}

TEST_F(OperatorTypesTest, FixedEventTimeWindowJoinOperatorTest)
{
    enjima::runtime::StreamingJob streamingJob;

    enjima::operators::OperatorID srcOpId = executionEngine_->GetNextOperatorId();
    std::string srcOpName = "src";
    auto uPtrSrcOp = std::make_unique<InMemoryYSBSourceOperator>(srcOpId, srcOpName);
    uPtrSrcOp->PopulateEventCache(1048576, 100, 10);
    auto adIdCampaignIdMap = uPtrSrcOp->GetAdIdToCampaignIdMap();
    streamingJob.AddOperator(std::move(uPtrSrcOp));

    enjima::operators::OperatorID filterOpId = executionEngine_->GetNextOperatorId();
    std::string filterOpName = "eventFilter";
    auto filterFn = [](const YSBAdT& ysbAdEvent) { return ysbAdEvent.GetEventType() == 0; };
    auto uPtrFilterOp = std::make_unique<enjima::operators::FilterOperator<YSBAdT, decltype(filterFn)>>(filterOpId,
            filterOpName, filterFn);
    streamingJob.AddOperator(std::move(uPtrFilterOp), srcOpId);

    enjima::operators::OperatorID projectOpId = executionEngine_->GetNextOperatorId();
    std::string projectOpName = "project";
    auto uPtrProjectOp = std::make_unique<enjima::operators::MapOperator<YSBAdT, YSBProjT, YSBProjectFunction>>(
            projectOpId, projectOpName, YSBProjectFunction{});
    streamingJob.AddOperator(std::move(uPtrProjectOp), filterOpId);

    enjima::operators::OperatorID statJoinOpId = executionEngine_->GetNextOperatorId();
    std::string statEquiJoinOpName = "staticEquiJoin";
    auto uPtrStatEquiJoinOp = std::make_unique<enjima::operators::StaticEquiJoinOperator<YSBProjT, uint64_t, YSBCampT,
            uint64_t, YSBKeyExtractFunction, YSBEqJoinFunction, 2>>(statJoinOpId, statEquiJoinOpName,
            YSBKeyExtractFunction{}, YSBEqJoinFunction{}, adIdCampaignIdMap);
    streamingJob.AddOperator(std::move(uPtrStatEquiJoinOp), projectOpId);

    enjima::operators::OperatorID windowOpId = executionEngine_->GetNextOperatorId();
    std::string windowOpName = "timeWindow";
    auto uPtrWindowOp = std::make_unique<
            enjima::operators::FixedEventTimeWindowOperator<YSBCampT, YSBWinT, true, YSBCampaignDummyAggFunction>>(
            windowOpId, windowOpName, YSBCampaignDummyAggFunction{}, std::chrono::seconds(4));
    streamingJob.AddOperator(std::move(uPtrWindowOp), statJoinOpId);

    enjima::operators::OperatorID windowJoinOpId = executionEngine_->GetNextOperatorId();
    std::string windowJoinOpName = "timeWindowJoin";
    auto uPtrWindowJoinOp = std::make_unique<enjima::operators::FixedEventTimeWindowJoinOperator<YSBCampT, YSBWinT,
            uint64_t, YSBCampTKeyExtractFunction, YSBWinTKeyExtractFunction, YSBDummyCampT, YSBDummyEqJoinFunction>>(
            windowJoinOpId, windowJoinOpName, YSBCampTKeyExtractFunction{}, YSBWinTKeyExtractFunction{},
            YSBDummyEqJoinFunction{}, std::chrono::seconds(10));
    streamingJob.AddOperator(std::move(uPtrWindowJoinOp), std::make_pair(statJoinOpId, windowOpId));

    enjima::operators::OperatorID sinkOpId = executionEngine_->GetNextOperatorId();
    std::string sinkOpName = "genericSink";
    auto sinkFn = NoOpYSBWinJoinSinkFunction{};
    auto uPtrSinkOp =
            std::make_unique<enjima::operators::GenericSinkOperator<YSBDummyCampT, NoOpYSBWinJoinSinkFunction>>(
                    sinkOpId, sinkOpName, sinkFn);
    streamingJob.AddOperator(std::move(uPtrSinkOp), windowJoinOpId);

    auto jobId = executionEngine_->Submit(streamingJob);
    std::this_thread::sleep_for(std::chrono::seconds(30));

    auto sinkInCounter = profiler_->GetOrCreateCounter(sinkOpName + enjima::metrics::kInCounterSuffix);
    auto winOutCounter = profiler_->GetOrCreateCounter(windowOpName + enjima::metrics::kOutCounterSuffix);
    auto winJoinOutCounter = profiler_->GetOrCreateCounter(windowJoinOpName + enjima::metrics::kOutCounterSuffix);
    auto winJoinInCounter = profiler_->GetOrCreateCounter(windowJoinOpName + enjima::metrics::kInCounterSuffix);
    auto statOutCounter = profiler_->GetOrCreateCounter(statEquiJoinOpName + enjima::metrics::kOutCounterSuffix);
    auto srcOutCounter = profiler_->GetOrCreateCounter(srcOpName + enjima::metrics::kOutCounterSuffix);

    EXPECT_GE(winJoinOutCounter->GetCount(), sinkInCounter->GetCount());
    EXPECT_LE(winJoinInCounter->GetCount(), winJoinOutCounter->GetCount());
    EXPECT_GE(winOutCounter->GetCount() + statOutCounter->GetCount(), winJoinInCounter->GetCount());
    EXPECT_GT(srcOutCounter->GetCount(), 0);

    executionEngine_->Cancel(jobId);
    EXPECT_EQ(executionEngine_->GetNumActiveJobs(), 0);

    EXPECT_GT(srcOutCounter->GetCount(), 0);
    EXPECT_EQ(winOutCounter->GetCount() + statOutCounter->GetCount(), winJoinInCounter->GetCount());
    EXPECT_LE(winJoinInCounter->GetCount(), winJoinOutCounter->GetCount());
    EXPECT_EQ(winJoinOutCounter->GetCount(), sinkInCounter->GetCount());

    std::cout << "Count for " << srcOpName << "(out) : " << srcOutCounter->GetCount() << std::endl;
    std::cout << "Count for " << windowJoinOpName << "(in) : " << winJoinInCounter->GetCount() << std::endl;
    std::cout << "Count for " << windowJoinOpName << "(out) : " << winJoinOutCounter->GetCount() << std::endl;
    std::cout << "Count for " << sinkOpName << "(in) : " << sinkInCounter->GetCount() << std::endl;
    std::cout << std::endl;
}

TEST_F(OperatorTypesTest, MultiKeyFixedEventTimeWindowJoinOperatorTest)
{
    enjima::runtime::StreamingJob streamingJob;

    enjima::operators::OperatorID srcOpId = executionEngine_->GetNextOperatorId();
    std::string srcOpName = "src";
    auto uPtrSrcOp = std::make_unique<InMemoryYSBSourceOperator>(srcOpId, srcOpName);
    uPtrSrcOp->PopulateEventCache(1048576, 100, 10);
    auto adIdCampaignIdMap = uPtrSrcOp->GetAdIdToCampaignIdMap();
    streamingJob.AddOperator(std::move(uPtrSrcOp));

    enjima::operators::OperatorID filterOpId = executionEngine_->GetNextOperatorId();
    std::string filterOpName = "eventFilter";
    auto filterFn = [](const YSBAdT& ysbAdEvent) { return ysbAdEvent.GetEventType() == 0; };
    auto uPtrFilterOp = std::make_unique<enjima::operators::FilterOperator<YSBAdT, decltype(filterFn)>>(filterOpId,
            filterOpName, filterFn);
    streamingJob.AddOperator(std::move(uPtrFilterOp), srcOpId);

    enjima::operators::OperatorID projectOpId = executionEngine_->GetNextOperatorId();
    std::string projectOpName = "project";
    auto uPtrProjectOp = std::make_unique<enjima::operators::MapOperator<YSBAdT, YSBProjT, YSBProjectFunction>>(
            projectOpId, projectOpName, YSBProjectFunction{});
    streamingJob.AddOperator(std::move(uPtrProjectOp), filterOpId);

    enjima::operators::OperatorID statJoinOpId = executionEngine_->GetNextOperatorId();
    std::string statEquiJoinOpName = "staticEquiJoin";
    auto uPtrStatEquiJoinOp = std::make_unique<enjima::operators::StaticEquiJoinOperator<YSBProjT, uint64_t, YSBCampT,
            uint64_t, YSBKeyExtractFunction, YSBEqJoinFunction, 2>>(statJoinOpId, statEquiJoinOpName,
            YSBKeyExtractFunction{}, YSBEqJoinFunction{}, adIdCampaignIdMap);
    streamingJob.AddOperator(std::move(uPtrStatEquiJoinOp), projectOpId);

    enjima::operators::OperatorID windowOpId = executionEngine_->GetNextOperatorId();
    std::string windowOpName = "timeWindow";
    auto uPtrWindowOp = std::make_unique<
            enjima::operators::FixedEventTimeWindowOperator<YSBCampT, YSBWinT, true, YSBCampaignDummyAggFunction>>(
            windowOpId, windowOpName, YSBCampaignDummyAggFunction{}, std::chrono::seconds(4));
    streamingJob.AddOperator(std::move(uPtrWindowOp), statJoinOpId);

    enjima::operators::OperatorID windowJoinOpId = executionEngine_->GetNextOperatorId();
    std::string windowJoinOpName = "timeWindowJoin";
    auto uPtrWindowJoinOp = std::make_unique<enjima::operators::FixedEventTimeWindowJoinOperator<YSBCampT, YSBWinT,
            std::tuple<uint64_t, uint64_t>, YSBCampTMultiKeyExtractFunction, YSBWinTMultiKeyExtractFunction, YSBDummyCampT, YSBDummyEqJoinFunction>>(
            windowJoinOpId, windowJoinOpName, YSBCampTMultiKeyExtractFunction{}, YSBWinTMultiKeyExtractFunction{},
            YSBDummyEqJoinFunction{}, std::chrono::seconds(10));
    streamingJob.AddOperator(std::move(uPtrWindowJoinOp), std::make_pair(statJoinOpId, windowOpId));

    enjima::operators::OperatorID sinkOpId = executionEngine_->GetNextOperatorId();
    std::string sinkOpName = "genericSink";
    auto sinkFn = NoOpYSBWinJoinSinkFunction{};
    auto uPtrSinkOp =
            std::make_unique<enjima::operators::GenericSinkOperator<YSBDummyCampT, NoOpYSBWinJoinSinkFunction>>(
                    sinkOpId, sinkOpName, sinkFn);
    streamingJob.AddOperator(std::move(uPtrSinkOp), windowJoinOpId);

    auto jobId = executionEngine_->Submit(streamingJob);
    std::this_thread::sleep_for(std::chrono::seconds(30));

    auto sinkInCounter = profiler_->GetOrCreateCounter(sinkOpName + enjima::metrics::kInCounterSuffix);
    auto winOutCounter = profiler_->GetOrCreateCounter(windowOpName + enjima::metrics::kOutCounterSuffix);
    auto winJoinOutCounter = profiler_->GetOrCreateCounter(windowJoinOpName + enjima::metrics::kOutCounterSuffix);
    auto winJoinInCounter = profiler_->GetOrCreateCounter(windowJoinOpName + enjima::metrics::kInCounterSuffix);
    auto statOutCounter = profiler_->GetOrCreateCounter(statEquiJoinOpName + enjima::metrics::kOutCounterSuffix);
    auto srcOutCounter = profiler_->GetOrCreateCounter(srcOpName + enjima::metrics::kOutCounterSuffix);

    EXPECT_GE(winJoinOutCounter->GetCount(), sinkInCounter->GetCount());
    EXPECT_GE(winJoinInCounter->GetCount(), winJoinOutCounter->GetCount());
    EXPECT_GE(winOutCounter->GetCount() + statOutCounter->GetCount(), winJoinInCounter->GetCount());
    EXPECT_GT(srcOutCounter->GetCount(), 0);

    executionEngine_->Cancel(jobId);
    EXPECT_EQ(executionEngine_->GetNumActiveJobs(), 0);

    EXPECT_GT(srcOutCounter->GetCount(), 0);
    EXPECT_EQ(winOutCounter->GetCount() + statOutCounter->GetCount(), winJoinInCounter->GetCount());
    EXPECT_GE(winJoinInCounter->GetCount(), winJoinOutCounter->GetCount());
    EXPECT_EQ(winJoinOutCounter->GetCount(), sinkInCounter->GetCount());

    std::cout << "Count for " << srcOpName << "(out) : " << srcOutCounter->GetCount() << std::endl;
    std::cout << "Count for " << windowJoinOpName << "(in) : " << winJoinInCounter->GetCount() << std::endl;
    std::cout << "Count for " << windowJoinOpName << "(out) : " << winJoinOutCounter->GetCount() << std::endl;
    std::cout << "Count for " << sinkOpName << "(in) : " << sinkInCounter->GetCount() << std::endl;
    std::cout << std::endl;
}

TEST_F(OperatorTypesTest, SlidingEventTimeWindowOperatorTest)
{
    enjima::runtime::StreamingJob streamingJob;

    enjima::operators::OperatorID srcOpId = executionEngine_->GetNextOperatorId();
    std::string srcOpName = "src";
    auto uPtrSrcOp = std::make_unique<InMemoryYSBSourceOperator>(srcOpId, srcOpName);
    uPtrSrcOp->PopulateEventCache(1048576, 100, 10);
    streamingJob.AddOperator(std::move(uPtrSrcOp), 0);

    enjima::operators::OperatorID windowOpId = executionEngine_->GetNextOperatorId();
    std::string windowOpName = "timeWindow";
    auto windowAggFunc = enjima::api::MergeableKeyedAggregateFunction<YSBAdT, YSBWinT>{};
    auto uPtrWindowOp = std::make_unique<
            enjima::operators::SlidingEventTimeWindowOperator<YSBAdT, YSBWinT, decltype(windowAggFunc)>>(windowOpId,
            windowOpName, windowAggFunc, std::chrono::seconds(10), std::chrono::seconds(2));
    streamingJob.AddOperator(std::move(uPtrWindowOp), 0);

    enjima::operators::OperatorID sinkOpId = executionEngine_->GetNextOperatorId();
    std::string sinkOpName = "genericSink";
    auto sinkFn = NoOpYSBWinSinkFunction{};
    auto uPtrSinkOp = std::make_unique<enjima::operators::GenericSinkOperator<YSBWinT, NoOpYSBWinSinkFunction>>(
            sinkOpId, sinkOpName, sinkFn);
    streamingJob.AddOperator(std::move(uPtrSinkOp), 0);

    auto jobId = executionEngine_->Submit(streamingJob);
    std::this_thread::sleep_for(std::chrono::seconds(30));

    auto sinkInCounter = profiler_->GetOrCreateCounter(sinkOpName + enjima::metrics::kInCounterSuffix);
    auto winOutCounter = profiler_->GetOrCreateCounter(windowOpName + enjima::metrics::kOutCounterSuffix);
    auto winInCounter = profiler_->GetOrCreateCounter(windowOpName + enjima::metrics::kInCounterSuffix);
    auto srcOutCounter = profiler_->GetOrCreateCounter(srcOpName + enjima::metrics::kOutCounterSuffix);

    EXPECT_GE(winOutCounter->GetCount(), sinkInCounter->GetCount());
    EXPECT_GE(winInCounter->GetCount(), winOutCounter->GetCount());
    EXPECT_GE(srcOutCounter->GetCount(), winInCounter->GetCount());
    EXPECT_GT(srcOutCounter->GetCount(), 0);

    executionEngine_->Cancel(jobId);
    EXPECT_EQ(executionEngine_->GetNumActiveJobs(), 0);

    EXPECT_GT(srcOutCounter->GetCount(), 0);
    EXPECT_EQ(srcOutCounter->GetCount(), winInCounter->GetCount());
    EXPECT_GE(winInCounter->GetCount(), winOutCounter->GetCount());
    EXPECT_EQ(winOutCounter->GetCount(), sinkInCounter->GetCount());

    std::cout << "Count for " << srcOpName << "(out) : " << srcOutCounter->GetCount() << std::endl;
    std::cout << "Count for " << windowOpName << "(in) : " << winInCounter->GetCount() << std::endl;
    std::cout << "Count for " << windowOpName << "(out) : " << winOutCounter->GetCount() << std::endl;
    std::cout << "Count for " << sinkOpName << "(in) : " << sinkInCounter->GetCount() << std::endl;
    std::cout << std::endl;
}

TEST_F(OperatorTypesTest, FixedEventTimeWindowCoGroupOperatorTest)
{
    enjima::runtime::StreamingJob streamingJob;

    enjima::operators::OperatorID srcOpId = executionEngine_->GetNextOperatorId();
    std::string srcOpName = "src";
    auto uPtrSrcOp = std::make_unique<InMemoryYSBSourceOperator>(srcOpId, srcOpName);
    uPtrSrcOp->PopulateEventCache(1048576, 100, 10);
    auto adIdCampaignIdMap = uPtrSrcOp->GetAdIdToCampaignIdMap();
    streamingJob.AddOperator(std::move(uPtrSrcOp));

    enjima::operators::OperatorID filterOpId = executionEngine_->GetNextOperatorId();
    std::string filterOpName = "eventFilter";
    auto filterFn = [](const YSBAdT& ysbAdEvent) { return ysbAdEvent.GetEventType() == 0; };
    auto uPtrFilterOp = std::make_unique<enjima::operators::FilterOperator<YSBAdT, decltype(filterFn)>>(filterOpId,
            filterOpName, filterFn);
    streamingJob.AddOperator(std::move(uPtrFilterOp), srcOpId);

    enjima::operators::OperatorID projectOpId = executionEngine_->GetNextOperatorId();
    std::string projectOpName = "project";
    auto uPtrProjectOp = std::make_unique<enjima::operators::MapOperator<YSBAdT, YSBProjT, YSBProjectFunction>>(
            projectOpId, projectOpName, YSBProjectFunction{});
    streamingJob.AddOperator(std::move(uPtrProjectOp), filterOpId);

    enjima::operators::OperatorID statJoinOpId = executionEngine_->GetNextOperatorId();
    std::string statEquiJoinOpName = "staticEquiJoin";
    auto uPtrStatEquiJoinOp = std::make_unique<enjima::operators::StaticEquiJoinOperator<YSBProjT, uint64_t, YSBCampT,
            uint64_t, YSBKeyExtractFunction, YSBEqJoinFunction, 2>>(statJoinOpId, statEquiJoinOpName,
            YSBKeyExtractFunction{}, YSBEqJoinFunction{}, adIdCampaignIdMap);
    streamingJob.AddOperator(std::move(uPtrStatEquiJoinOp), projectOpId);

    enjima::operators::OperatorID windowOpId = executionEngine_->GetNextOperatorId();
    std::string windowOpName = "timeWindow";
    auto uPtrWindowOp = std::make_unique<
            enjima::operators::FixedEventTimeWindowOperator<YSBCampT, YSBWinT, true, YSBCampaignDummyAggFunction>>(
            windowOpId, windowOpName, YSBCampaignDummyAggFunction{}, std::chrono::seconds(4));
    streamingJob.AddOperator(std::move(uPtrWindowOp), statJoinOpId);

    enjima::operators::OperatorID windowCoGroupOpId = executionEngine_->GetNextOperatorId();
    std::string windowCoGroupOpName = "timeWindowCoGroup";
    auto uPtrWindowCoGroupOp = std::make_unique<enjima::operators::FixedEventTimeWindowCoGroupOperator<YSBCampT,
            YSBWinT, YSBDummyCampT, YSBDummyCoGroupFunction>>(windowCoGroupOpId, windowCoGroupOpName,
            YSBDummyCoGroupFunction{}, std::chrono::seconds(10));
    streamingJob.AddOperator(std::move(uPtrWindowCoGroupOp), std::make_pair(statJoinOpId, windowOpId));

    enjima::operators::OperatorID sinkOpId = executionEngine_->GetNextOperatorId();
    std::string sinkOpName = "genericSink";
    auto sinkFn = NoOpYSBWinJoinSinkFunction{};
    auto uPtrSinkOp =
            std::make_unique<enjima::operators::GenericSinkOperator<YSBDummyCampT, NoOpYSBWinJoinSinkFunction>>(
                    sinkOpId, sinkOpName, sinkFn);
    streamingJob.AddOperator(std::move(uPtrSinkOp), windowCoGroupOpId);

    auto jobId = executionEngine_->Submit(streamingJob);
    std::this_thread::sleep_for(std::chrono::seconds(30));

    auto sinkInCounter = profiler_->GetOrCreateCounter(sinkOpName + enjima::metrics::kInCounterSuffix);
    auto winOutCounter = profiler_->GetOrCreateCounter(windowOpName + enjima::metrics::kOutCounterSuffix);
    auto winCoGroupOutCounter = profiler_->GetOrCreateCounter(windowCoGroupOpName + enjima::metrics::kOutCounterSuffix);
    auto winCoGroupInCounter = profiler_->GetOrCreateCounter(windowCoGroupOpName + enjima::metrics::kInCounterSuffix);
    auto statOutCounter = profiler_->GetOrCreateCounter(statEquiJoinOpName + enjima::metrics::kOutCounterSuffix);
    auto srcOutCounter = profiler_->GetOrCreateCounter(srcOpName + enjima::metrics::kOutCounterSuffix);

    EXPECT_GE(winCoGroupOutCounter->GetCount(), sinkInCounter->GetCount());
    EXPECT_GE(winCoGroupInCounter->GetCount(), winCoGroupOutCounter->GetCount());
    EXPECT_GE(winOutCounter->GetCount() + statOutCounter->GetCount(), winCoGroupInCounter->GetCount());
    EXPECT_GT(srcOutCounter->GetCount(), 0);

    executionEngine_->Cancel(jobId);
    EXPECT_EQ(executionEngine_->GetNumActiveJobs(), 0);

    EXPECT_GT(srcOutCounter->GetCount(), 0);
    EXPECT_EQ(winOutCounter->GetCount() + statOutCounter->GetCount(), winCoGroupInCounter->GetCount());
    EXPECT_GE(winCoGroupInCounter->GetCount(), winCoGroupOutCounter->GetCount());
    EXPECT_EQ(winCoGroupOutCounter->GetCount(), sinkInCounter->GetCount());

    std::cout << "Count for " << srcOpName << "(out) : " << srcOutCounter->GetCount() << std::endl;
    std::cout << "Count for " << windowCoGroupOpName << "(in) : " << winCoGroupInCounter->GetCount() << std::endl;
    std::cout << "Count for " << windowCoGroupOpName << "(out) : " << winCoGroupOutCounter->GetCount() << std::endl;
    std::cout << "Count for " << sinkOpName << "(in) : " << sinkInCounter->GetCount() << std::endl;
    std::cout << std::endl;
}
