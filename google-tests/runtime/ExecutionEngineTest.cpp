//
// Created by m34ferna on 12/01/24.
//

#include "enjima/runtime/ExecutionEngine.h"
#include "enjima/api/data_types/LinearRoadEvent.h"
#include "enjima/memory/MemoryUtil.h"
#include "enjima/runtime/StreamingJob.h"
#include "gtest/gtest.h"
#include <climits>
#include <random>

using LinearRoadT = enjima::api::data_types::LinearRoadEvent;
using EngineT = enjima::runtime::ExecutionEngine;
using JobT = enjima::runtime::StreamingJob;
using LRRecordT = enjima::core::Record<LinearRoadT>;
using UniformIntDistParamT = std::uniform_int_distribution<int>::param_type;

class ExecutionEngineTest : public testing::Test {
protected:
    void SetUp() override
    {
        executionEngine_ = new EngineT();
        executionEngine_->Init(maxMemory_, numBlocksPerChunk_, numEventsPerBlock_, "EnjimaExecEngineTest");
        executionEngine_->Start();
    }

    void TearDown() override
    {
        executionEngine_->Shutdown();
    }

    LinearRoadT generateLRTypeEvent()
    {
        LinearRoadT lrEvent = LinearRoadT(0, GetRandomInt(0, 10799), GetRandomInt(0, INT_MAX), GetRandomInt(0, 100),
                GetRandomInt(0, 2), GetRandomInt(0, 4), GetRandomInt(0, 1), GetRandomInt(0, 99),
                GetRandomInt(0, 527999), 0, 0, 0, GetRandomInt(1, 7), GetRandomInt(1, 1440), GetRandomInt(1, 69));
        return lrEvent;
    }

    int GetRandomInt(const int& a, const int& b)
    {
        UniformIntDistParamT pt(a, b);
        uniformIntDistribution.param(pt);
        return uniformIntDistribution(gen);
    }

    std::random_device rd;
    std::mt19937 gen{rd()};
    std::uniform_int_distribution<int> uniformIntDistribution;
    EngineT* executionEngine_ = nullptr;
    const size_t maxMemory_ = enjima::memory::MegaBytes(64);
    const size_t numBlocksPerChunk_ = 16;
    const int32_t numEventsPerBlock_ = 1000;
};

TEST_F(ExecutionEngineTest, InitComponent)
{
    const size_t maxMemory = enjima::memory::MegaBytes(64);
    const int32_t defaultBlockSize = 100;
    const int32_t numBlocksPerChunk = 8;
    EngineT executionEngine{};
    executionEngine.Init(maxMemory, numBlocksPerChunk, defaultBlockSize, "EnjimaExecEngineTest");
    executionEngine.Start();
    EXPECT_TRUE(executionEngine.IsInitialized());
    EXPECT_TRUE(executionEngine.IsRunning());
}

TEST_F(ExecutionEngineTest, Init)
{
    EXPECT_TRUE(executionEngine_->IsInitialized());
    EXPECT_TRUE(executionEngine_->IsRunning());
}

TEST_F(ExecutionEngineTest, SubmitJob)
{
    JobT streamingJob;
    executionEngine_->Submit(streamingJob);
    EXPECT_EQ(executionEngine_->GetNumActiveJobs(), 1);
}
