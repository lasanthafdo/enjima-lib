//
// Created by m34ferna on 12/01/24.
//


#include "enjima/api/data_types/LinearRoadEvent.h"
#include "enjima/memory/MemoryUtil.h"
#include "enjima/operators/FilterOperator.h"
#include "enjima/operators/GenericSinkOperator.h"
#include "enjima/operators/MapOperator.h"
#include "enjima/operators/NoOpSinkOperator.h"
#include "enjima/operators/SourceOperator.h"
#include "enjima/runtime/ExecutionEngine.h"
#include "enjima/runtime/StreamingJob.h"
#include "google-tests/common/InputGenerationHelper.h"
#include "google-tests/common/TestSetupHelperFunctions.h"
#include "gtest/gtest.h"
#include <atomic>
#include <thread>

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

class QueueOperatorTest : public testing::Test {
protected:
    void SetUp() override
    {
        memoryManager_ =
                new MemManT(maxMemory_, numBlocksPerChunk, numEventsPerBlock_, MemManT ::AllocatorType::kBasic);
        profiler_ = new ProflierT(10000, true, "EnjimaQueueOperatorTest");
        executionEngine_ = new EngineT;
    }

    void TearDown() override
    {
        executionEngine_->Shutdown();
    }

    void StartEngineWithDefaults()
    {
        executionEngine_->Init(memoryManager_, profiler_);
        executionEngine_->Start();
    }

    void StartEngineWithParams(uint64_t schedPeriodMs, SchedModeT schedMode, uint32_t numWorkers, ProcModeT procMode,
            PreemptModeT preemptMode, PriorityModeT priorityMode, uint64_t eventThreshold, uint64_t iIdleThresholdMs)
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
        executionEngine_->SetIdleThresholdMs(iIdleThresholdMs);
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
    const uint64_t defaultMaxIdleThreshold_ = 10;
    const uint64_t defaultEventThreshold_ = 1000;
    const uint64_t defaultInputRate_ = 12'000'000;

    const std::string srcOpName = "src";
    const std::string filterOpName = "filter";
    const std::string projectOpName = "project";
    const std::string statEqJoinOpName = "staticEqJoin";
    const std::string windowOpName = "timeWindow";
    const std::string sinkOpName = "genericSink";
};


class LinearRoadMapFunc : public enjima::api::MapFunction<LinearRoadT, LinearRoadT> {
public:
    LinearRoadT operator()(const LinearRoadT& inputEvent) override
    {
        return inputEvent;
    }
};

TEST_F(QueueOperatorTest, Init)
{
    StartEngineWithDefaults();
    EXPECT_EQ(memoryManager_->GetMaxMemory(), maxMemory_);
    EXPECT_EQ(memoryManager_->GetDefaultNumEventsPerBlock(), numEventsPerBlock_);
}

TEST_F(QueueOperatorTest, BasicPipeline)
{
    StartEngineWithDefaults();
    enjima::runtime::StreamingJob streamingJob;

    enjima::operators::OperatorID srcOpId = executionEngine_->GetNextOperatorId();
    auto uPtrSrcOp = std::make_unique<GeneratingLinearRoadSourceOperator>(srcOpId, "src");
    streamingJob.AddOperator(std::move(uPtrSrcOp), 0);

    enjima::operators::OperatorID filterOpId = executionEngine_->GetNextOperatorId();
    auto filterFn = [](const LinearRoadT& lrTypeEvent) { return lrTypeEvent.GetDow() >= 1; };
    auto uPtrFilterOp = std::make_unique<enjima::operators::FilterOperator<LinearRoadT, decltype(filterFn)>>(filterOpId,
            "dowFilter", filterFn);
    streamingJob.AddOperator(std::move(uPtrFilterOp), 0);

    enjima::operators::OperatorID sinkOpId = executionEngine_->GetNextOperatorId();
    auto uPtrSinkOp = std::make_unique<enjima::operators::NoOpSinkOperator<LinearRoadT>>(sinkOpId, "noOpSink");
    streamingJob.AddOperator(std::move(uPtrSinkOp), 0);
    streamingJob.SetProcessingMode(enjima::runtime::StreamingTask::ProcessingMode::kQueueBasedSingle);

    auto jobId = executionEngine_->Submit(streamingJob);
    EXPECT_EQ(executionEngine_->GetNumActiveJobs(), 1);

    executionEngine_->Cancel(jobId);
    EXPECT_EQ(executionEngine_->GetNumActiveJobs(), 0);
}

TEST_F(QueueOperatorTest, BasicRunForTenSeconds)
{
    StartEngineWithDefaults();
    enjima::runtime::StreamingJob streamingJob;

    enjima::operators::OperatorID srcOpId = executionEngine_->GetNextOperatorId();
    auto uPtrSrcOp = std::make_unique<GeneratingLinearRoadSourceOperator>(srcOpId, "src");
    streamingJob.AddOperator(std::move(uPtrSrcOp), 0);

    enjima::operators::OperatorID filterOpId = executionEngine_->GetNextOperatorId();
    auto filterFn = [](const LinearRoadT& lrTypeEvent) { return lrTypeEvent.GetDow() >= 1; };
    auto uPtrFilterOp = std::make_unique<enjima::operators::FilterOperator<LinearRoadT, decltype(filterFn)>>(filterOpId,
            "dowFilter", filterFn);
    streamingJob.AddOperator(std::move(uPtrFilterOp), 0);

    enjima::operators::OperatorID sinkOpId = executionEngine_->GetNextOperatorId();
    auto uPtrSinkOp = std::make_unique<enjima::operators::NoOpSinkOperator<LinearRoadT>>(sinkOpId, "noOpSink");
    streamingJob.AddOperator(std::move(uPtrSinkOp), 0);

    streamingJob.SetProcessingMode(enjima::runtime::StreamingTask::ProcessingMode::kQueueBasedSingle);

    auto jobId = executionEngine_->Submit(streamingJob, 10000);
    std::this_thread::sleep_for(std::chrono::seconds(10));
    executionEngine_->Cancel(jobId);

    EXPECT_EQ(executionEngine_->GetNumActiveJobs(), 0);

    std::unsigned_integral auto srcOutCount = profiler_->GetOrCreateCounter("src_out_counter")->GetCount();
    std::unsigned_integral auto filterInCount = profiler_->GetOrCreateCounter("dowFilter_in_counter")->GetCount();
    std::unsigned_integral auto filterOutCount = profiler_->GetOrCreateCounter("dowFilter_out_counter")->GetCount();
    std::unsigned_integral auto sinkInCnt = profiler_->GetOrCreateCounter("noOpSink_in_counter")->GetCount();

    EXPECT_GT(srcOutCount, 0);
    EXPECT_EQ(srcOutCount, filterInCount);
    EXPECT_EQ(filterOutCount, sinkInCnt);

    std::cout << "Count for src(out) : " << srcOutCount << std::endl;
    std::cout << "Count for sink(in) : " << sinkInCnt << std::endl;
    std::cout << std::endl;
}

// The current implementation of the queue based processing mode does not support SB. It does not return when the output queue
// is full. Therefore, writing a test with size 1 that intentionally causes the thread to spin forever is wrong for SB.
TEST_F(QueueOperatorTest, MapOperatorTest)
{
    StartEngineWithDefaults();
    enjima::runtime::StreamingJob streamingJob;

    enjima::operators::OperatorID srcOpId = executionEngine_->GetNextOperatorId();
    auto uPtrSrcOp = std::make_unique<InMemoryLinearRoadSourceOperator>(srcOpId, "src");
    uPtrSrcOp->PopulateEventCache(105000);
    streamingJob.AddOperator(std::move(uPtrSrcOp), 0);

    enjima::operators::OperatorID projectOpId = executionEngine_->GetNextOperatorId();
    std::string projectOpName = "project";
    auto uPtrProjectOp = std::make_unique<enjima::operators::MapOperator<LinearRoadT, LinearRoadT, LinearRoadMapFunc>>(
            projectOpId, projectOpName, LinearRoadMapFunc{});
    streamingJob.AddOperator(std::move(uPtrProjectOp), 0);

    enjima::operators::OperatorID sinkOpId = executionEngine_->GetNextOperatorId();
    auto uPtrSinkOp = std::make_unique<enjima::operators::NoOpSinkOperator<LinearRoadT>>(sinkOpId, "noOpSink");
    streamingJob.AddOperator(std::move(uPtrSinkOp), 0);
    streamingJob.SetProcessingMode(enjima::runtime::StreamingTask::ProcessingMode::kQueueBasedSingle);

    auto jobId = executionEngine_->Submit(streamingJob, 1);
    std::this_thread::sleep_for(std::chrono::seconds(10));

    executionEngine_->Cancel(jobId);
    EXPECT_EQ(executionEngine_->GetNumActiveJobs(), 0);

    std::unsigned_integral auto srcOutCount = profiler_->GetOrCreateCounter("src_out_counter")->GetCount();
    std::unsigned_integral auto sinkInCnt = profiler_->GetOrCreateCounter("noOpSink_in_counter")->GetCount();

    EXPECT_GT(srcOutCount, 0);

    std::cout << "Count for src(out) : " << srcOutCount << std::endl;
    std::cout << "Count for sink(in) : " << sinkInCnt << std::endl;
    std::cout << std::endl;
}

TEST_F(QueueOperatorTest, StaticEquiJoinOperatorTest)
{
    StartEngineWithDefaults();
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
    streamingJob.SetProcessingMode(enjima::runtime::StreamingTask::ProcessingMode::kQueueBasedSingle);
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

TEST_F(QueueOperatorTest, FixedEventTimeWindowOperatorTest)
{
    StartEngineWithDefaults();
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

    streamingJob.SetProcessingMode(enjima::runtime::StreamingTask::ProcessingMode::kQueueBasedSingle);
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

TEST_F(QueueOperatorTest, SlidingEventTimeWindowOperatorTest)
{
    StartEngineWithDefaults();
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

    streamingJob.SetProcessingMode(enjima::runtime::StreamingTask::ProcessingMode::kQueueBasedSingle);
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

TEST_F(QueueOperatorTest, FixedEventTimeWindowJoinOperatorTest)
{
    StartEngineWithDefaults();
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

    streamingJob.SetProcessingMode(enjima::runtime::StreamingTask::ProcessingMode::kQueueBasedSingle);
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

TEST_F(QueueOperatorTest, FixedEventTimeWindowCoGroupOperatorTest)
{
    StartEngineWithDefaults();
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

    streamingJob.SetProcessingMode(enjima::runtime::StreamingTask::ProcessingMode::kQueueBasedSingle);
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

TEST_F(QueueOperatorTest, SecondFixedEventTimeWindowCoGroupOperatorTest)
{
    StartEngineWithDefaults();
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
    auto uPtrProjectOp = std::make_unique<enjima::operators::MapOperator<YSBAdT, YSBProjT, YSBProjectFunction, 3>>(
            projectOpId, projectOpName, YSBProjectFunction{});
    streamingJob.AddOperator(std::move(uPtrProjectOp), filterOpId);

    enjima::operators::OperatorID filter2OpId = executionEngine_->GetNextOperatorId();
    std::string filter2OpName = "eventFilter2";
    auto filterFn2 = [](const YSBProjT& ysbProjEvent) { return ysbProjEvent.GetAdId() > 0; };
    auto uPtrFilter2Op = std::make_unique<enjima::operators::FilterOperator<YSBProjT, decltype(filterFn2)>>(filter2OpId,
            filter2OpName, filterFn2);
    streamingJob.AddOperator(std::move(uPtrFilter2Op), projectOpId);

    enjima::operators::OperatorID statJoinOpId = executionEngine_->GetNextOperatorId();
    std::string statEquiJoinOpName = "staticEquiJoin";
    auto uPtrStatEquiJoinOp = std::make_unique<enjima::operators::StaticEquiJoinOperator<YSBProjT, uint64_t, YSBCampT,
            uint64_t, YSBKeyExtractFunction, YSBEqJoinFunction, 2>>(statJoinOpId, statEquiJoinOpName,
            YSBKeyExtractFunction{}, YSBEqJoinFunction{}, adIdCampaignIdMap);
    streamingJob.AddOperator(std::move(uPtrStatEquiJoinOp), filter2OpId);

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

    enjima::operators::OperatorID sinkOpId2 = executionEngine_->GetNextOperatorId();
    std::string sinkOpName2 = "genericSink2";
    auto uPtrSinkOp2 = std::make_unique<enjima::operators::NoOpSinkOperator<YSBProjT>>(sinkOpId2, sinkOpName2);
    streamingJob.AddOperator(std::move(uPtrSinkOp2), projectOpId);

    enjima::operators::OperatorID sinkOpId3 = executionEngine_->GetNextOperatorId();
    std::string sinkOpName3 = "genericSink3";
    auto uPtrSinkOp3 = std::make_unique<enjima::operators::NoOpSinkOperator<YSBProjT>>(sinkOpId3, sinkOpName3);
    streamingJob.AddOperator(std::move(uPtrSinkOp3), projectOpId);

    streamingJob.SetProcessingMode(enjima::runtime::StreamingTask::ProcessingMode::kQueueBasedSingle);
    auto jobId = executionEngine_->Submit(streamingJob);
    std::this_thread::sleep_for(std::chrono::seconds(30));

    auto sinkInCounter = profiler_->GetOrCreateCounter(sinkOpName + enjima::metrics::kInCounterSuffix);
    auto sinkInCounter2 = profiler_->GetOrCreateCounter(sinkOpName2 + enjima::metrics::kInCounterSuffix);
    auto sinkInCounter3 = profiler_->GetOrCreateCounter(sinkOpName3 + enjima::metrics::kInCounterSuffix);
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
    std::cout << "Count for " << sinkOpName2 << "(in) : " << sinkInCounter2->GetCount() << std::endl;
    std::cout << "Count for " << sinkOpName3 << "(in) : " << sinkInCounter3->GetCount() << std::endl;
    std::cout << std::endl;
}

TEST_F(QueueOperatorTest, SlidingEventTimeWindowOperatorTestSBL)
{
    StartEngineWithParams(10, SchedModeT::kStateBasedPriority, 2, ProcModeT::kQueueBasedSingle,
            PreemptModeT ::kNonPreemptive, PriorityModeT::kLatencyOptimized, defaultEventThreshold_,
            defaultMaxIdleThreshold_);
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

TEST_F(QueueOperatorTest, FixedEventTimeWindowJoinOperatorTestSBL)
{
    StartEngineWithParams(10, SchedModeT::kStateBasedPriority, 2, ProcModeT::kQueueBasedSingle,
            PreemptModeT ::kNonPreemptive, PriorityModeT::kLatencyOptimized, defaultEventThreshold_,
            defaultMaxIdleThreshold_);
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

TEST_F(QueueOperatorTest, FixedEventTimeWindowCoGroupOperatorTestSBL)
{
    StartEngineWithParams(10, SchedModeT::kStateBasedPriority, 2, ProcModeT::kQueueBasedSingle,
            PreemptModeT ::kNonPreemptive, PriorityModeT::kLatencyOptimized, defaultEventThreshold_,
            defaultMaxIdleThreshold_);
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

TEST_F(QueueOperatorTest, MPMCQueue)
{
    rigtorp::MPMCQueue<int> q(1000);
    std::atomic<bool> done;
    std::atomic<int> inc;
    std::atomic<int> dec;
    auto t1 = std::thread([&] {
        int v = 0;
        while (!done) {
            q.push(v);
            inc += 1;
        }
    });
    auto t2 = std::thread([&] {
        int v;
        while (!done) {
            q.pop(v);
            dec += 1;
        }
    });
    std::this_thread::sleep_for(std::chrono::seconds(10));
    done = true;
    t1.join();
    t2.join();
    std::cout << "#pushed: " << inc << ", #popped: " << dec << std::endl;
}

TEST_F(QueueOperatorTest, SPSCQueue)
{
    rigtorp::SPSCQueue<int> q(1000000);
    std::atomic<bool> done;
    int inc = 0;
    int dec = 0;
    auto t1 = std::thread([&] {
        int v = 0;
        while (!done.load(std::memory_order_relaxed)) {
            q.push(v);
            inc += 1;
        }
    });
    auto t2 = std::thread([&] {
        while (!done.load(std::memory_order_relaxed)) {
            if (q.front()) {
                q.pop();
                dec += 1;
            }
        }
    });
    std::this_thread::sleep_for(std::chrono::seconds(10));
    done = true;
    t1.join();
    t2.join();
    std::cout << "#pushed: " << inc << ", #popped: " << dec << std::endl;
}

TEST_F(QueueOperatorTest, AssertedQueuedWindowedLinearPipelineTB)
{
    StartEngineWithParams(10, SchedModeT::kThreadBased, 2, ProcModeT::kQueueBasedSingle, PreemptModeT ::kNonPreemptive,
            PriorityModeT::kInputQueueSize, defaultEventThreshold_, defaultMaxIdleThreshold_);
    enjima::runtime::StreamingJob streamingJob = SetUpBasicWindowedPipeline(executionEngine_, srcOpName, projectOpName,
            statEqJoinOpName, windowOpName, sinkOpName);
    auto jobId = executionEngine_->Submit(streamingJob);
    std::this_thread::sleep_for(std::chrono::seconds(30));
    executionEngine_->Cancel(jobId, std::chrono::seconds(10));

    ValidatePipelineResults(true, profiler_, executionEngine_, srcOpName, projectOpName, statEqJoinOpName, windowOpName,
            sinkOpName);
}

TEST_F(QueueOperatorTest, AssertedQueuedWindowedLinearPipelineSBL)
{
    StartEngineWithParams(10, SchedModeT::kStateBasedPriority, 2, ProcModeT::kQueueBasedSingle,
            PreemptModeT ::kNonPreemptive, PriorityModeT::kLatencyOptimized, defaultEventThreshold_,
            defaultMaxIdleThreshold_);
    enjima::runtime::StreamingJob streamingJob = SetUpBasicWindowedPipeline(executionEngine_, srcOpName, projectOpName,
            statEqJoinOpName, windowOpName, sinkOpName);
    auto jobId = executionEngine_->Submit(streamingJob);
    std::this_thread::sleep_for(std::chrono::seconds(30));
    executionEngine_->Cancel(jobId, std::chrono::seconds(10));

    ValidatePipelineResults(true, profiler_, executionEngine_, srcOpName, projectOpName, statEqJoinOpName, windowOpName,
            sinkOpName);
}
