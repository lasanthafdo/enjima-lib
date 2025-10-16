//
// Created by m34ferna on 16/01/24.
//

#include "enjima/memory/MemoryChunk.h"
#include "enjima/api/data_types/LinearRoadEvent.h"
#include "enjima/memory/BasicMemoryAllocator.h"
#include <gtest/gtest.h>

using ChunkT = enjima::memory::MemoryChunk;
using MemAllocT = enjima::memory::BasicMemoryAllocator;
using LinearRoadT = enjima::api::data_types::LinearRoadEvent;

class MemoryChunkTest : public testing::Test {
protected:
    void SetUp() override
    {
        memAllocator_ = new MemAllocT;
        memChunk_ = memAllocator_->Allocate(memoryChunkSize_, defaultBlockSize_);
    }

    void TearDown() override
    {
        memAllocator_->Deallocate(memChunk_);
    }

    ChunkT* memChunk_{nullptr};
    MemAllocT* memAllocator_{nullptr};
    const size_t memoryChunkSize_ = enjima::memory::KiloBytes(32);
    const size_t defaultBlockSize_ = enjima::memory::KiloBytes(1);
};

TEST_F(MemoryChunkTest, InitChunk)
{
    auto expectedCapacity = memoryChunkSize_ - sizeof(ChunkT);
    EXPECT_EQ(memChunk_, memChunk_->GetChunkPtr());
    EXPECT_NE(memChunk_->GetChunkPtr(), memChunk_->GetDataPtr());
    EXPECT_EQ(memChunk_->GetChunkId(), 1);
    EXPECT_EQ(memChunk_->GetCapacity(), expectedCapacity);
}

TEST_F(MemoryChunkTest, ReserveMemory)
{
    void* beginPos = memChunk_->ReserveBlock();
    std::cout << "Size of " << typeid(LinearRoadT).name() << " = " << sizeof(LinearRoadT) << std::endl;
    auto lrEvent = new (beginPos) LinearRoadT(0, 1, 10, 51, 3, 1, 0, 45, 1202, 12, 34, -1, 3, 986, 7);
    EXPECT_EQ(lrEvent->GetType(), 0);
    EXPECT_EQ(lrEvent->GetTimestamp(), 1);
    EXPECT_EQ(lrEvent->GetSpeed(), 51);
}

TEST_F(MemoryChunkTest, OverallocateBlockWithinChunk)
{
    auto pBegin = static_cast<LinearRoadT*>(memChunk_->ReserveBlock());
    auto currentPos = pBegin;
    size_t linearRoadSize = sizeof(LinearRoadT);
    std::cout << "Size of " << typeid(LinearRoadT).name() << " = " << linearRoadSize << std::endl;
    size_t eventsInBlock = defaultBlockSize_ / linearRoadSize;
    size_t eventsInChunk = memChunk_->GetCapacity() / linearRoadSize;
    auto blockEndPos = pBegin + eventsInBlock;
    auto chunkEndPos = (void*) (static_cast<char*>(memChunk_->GetDataPtr()) + memChunk_->GetCapacity());
    for (decltype(eventsInBlock) i = 0; i < eventsInBlock; i++) {
        auto lrEvent = new (currentPos)
                LinearRoadT(0, static_cast<int>(i) + 1, 10, 51, 3, 1, 0, 45, 1202, 12, 34, -1, 3, 986, 7);
        currentPos++;
        EXPECT_EQ(lrEvent->GetType(), 0);
        EXPECT_EQ(lrEvent->GetTimestamp(), i + 1);
        EXPECT_EQ(lrEvent->GetSpeed(), 51);
    }
    EXPECT_LE(currentPos, blockEndPos);
    std::cout << "Filled block with " << eventsInBlock << " events consuming " << (currentPos - pBegin) * linearRoadSize
              << " bytes" << std::endl;
    auto numItrRemaining = eventsInChunk - eventsInBlock;
    for (decltype(numItrRemaining) i = 0; i < numItrRemaining; i++) {
        auto lrEvent = new (currentPos)
                LinearRoadT(0, static_cast<int>(i) + 1, 10, 51, 3, 1, 0, 45, 1202, 12, 34, -1, 3, 986, 7);
        currentPos++;
        EXPECT_EQ(lrEvent->GetType(), 0);
        EXPECT_EQ(lrEvent->GetTimestamp(), i + 1);
        EXPECT_EQ(lrEvent->GetSpeed(), 51);
    }
    EXPECT_LE(currentPos, chunkEndPos);
    std::cout << "Memory chunk begin pos : " << pBegin << ", current pos: " << currentPos
              << ", used memory : " << (currentPos - pBegin) * linearRoadSize << std::endl;
    std::cout << "Chunk capacity : " << memChunk_->GetCapacity() << std::endl;
}

TEST_F(MemoryChunkTest, ReserveReleaseBlocks)
{
    size_t linearRoadSize = sizeof(LinearRoadT);
    std::cout << "Size of " << typeid(LinearRoadT).name() << " = " << linearRoadSize << std::endl;
    auto eventsInBlock = defaultBlockSize_ / linearRoadSize;
    auto blocksInChunk = memChunk_->GetCapacity() / defaultBlockSize_;
    void* chunkBeginPos = const_cast<void*>(memChunk_->GetChunkPtr());
    auto chunkEndPos = (void*) (static_cast<char*>(memChunk_->GetDataPtr()) + memChunk_->GetCapacity());

    auto blkBeginPos = static_cast<LinearRoadT*>(memChunk_->ReserveBlock());
    for (decltype(blocksInChunk) blockIdx = 0; blockIdx < blocksInChunk; blockIdx++) {
        auto expectedBlkBeginPos =
                static_cast<void*>(static_cast<char*>(memChunk_->GetDataPtr()) + defaultBlockSize_ * blockIdx);
        EXPECT_EQ(static_cast<void*>(blkBeginPos), expectedBlkBeginPos);
        auto blkCurrentPos = blkBeginPos;
        for (decltype(eventsInBlock) i = 0; i < eventsInBlock; i++) {
            auto lrEvent = new (blkCurrentPos)
                    LinearRoadT(0, static_cast<int>(i) + 1, 10, 51, 3, 1, 0, 45, 1202, 12, 34, -1, 3, 986, 7);
            blkCurrentPos++;
            EXPECT_EQ(lrEvent->GetType(), 0);
            EXPECT_EQ(lrEvent->GetTimestamp(), i + 1);
            EXPECT_EQ(lrEvent->GetSpeed(), 51);
        }
        if (blockIdx == 0 || blockIdx == (blocksInChunk - 1)) {
            std::cout << "Filled block " << blockIdx << " with " << eventsInBlock << " events consuming "
                      << (blkCurrentPos - blkBeginPos) * linearRoadSize << " bytes" << std::endl;
            std::cout << "Memory chunk begin pos : " << blkBeginPos << ", current pos: " << blkCurrentPos
                      << ", used memory : "
                      << (blkCurrentPos - static_cast<LinearRoadT*>(chunkBeginPos)) * linearRoadSize << std::endl;
        }
        EXPECT_LT(blkCurrentPos, chunkEndPos);
        blkBeginPos = static_cast<LinearRoadT*>(memChunk_->ReserveBlock());
    }
    // Doing one additional reservation, which should return nullptr
    auto lastChunkAllocBeginPos = static_cast<LinearRoadT*>(memChunk_->ReserveBlock());
    EXPECT_EQ(lastChunkAllocBeginPos, nullptr);
}

TEST_F(MemoryChunkTest, OverallocateBlocks)
{
    size_t linearRoadSize = sizeof(LinearRoadT);
    std::cout << "Size of " << typeid(LinearRoadT).name() << " = " << linearRoadSize << std::endl;
    auto eventsInBlock = defaultBlockSize_ / linearRoadSize;
    auto blocksInChunk = memChunk_->GetCapacity() / defaultBlockSize_;
    void* chunkBeginPos = const_cast<void*>(memChunk_->GetChunkPtr());
    auto chunkEndPos = (void*) (static_cast<char*>(memChunk_->GetDataPtr()) + memChunk_->GetCapacity());

    for (decltype(blocksInChunk) blockIdx = 0; blockIdx < blocksInChunk; blockIdx++) {
        auto blkBeginPos = static_cast<LinearRoadT*>(memChunk_->ReserveBlock());
        auto blkCurrentPos = blkBeginPos;
        for (decltype(eventsInBlock) i = 0; i < eventsInBlock; i++) {
            auto lrEvent = new (blkCurrentPos)
                    LinearRoadT(0, static_cast<int>(i) + 1, 10, 51, 3, 1, 0, 45, 1202, 12, 34, -1, 3, 986, 7);
            blkCurrentPos++;
            EXPECT_EQ(lrEvent->GetType(), 0);
            EXPECT_EQ(lrEvent->GetTimestamp(), i + 1);
            EXPECT_EQ(lrEvent->GetSpeed(), 51);
        }
        if (blockIdx == 0 || blockIdx == (blocksInChunk - 1)) {
            std::cout << "Filled block " << blockIdx << " with " << eventsInBlock << " events consuming "
                      << (blkCurrentPos - blkBeginPos) * linearRoadSize << " bytes" << std::endl;
            std::cout << "Memory chunk begin pos : " << blkBeginPos << ", current pos: " << blkCurrentPos
                      << ", used memory : "
                      << (blkCurrentPos - static_cast<LinearRoadT*>(chunkBeginPos)) * linearRoadSize << std::endl;
        }
        EXPECT_LE(blkCurrentPos, chunkEndPos);
    }
    // Doing one additional reservation, which should return nullptr
    auto lastChunkAllocBeginPos = static_cast<LinearRoadT*>(memChunk_->ReserveBlock());
    EXPECT_EQ(lastChunkAllocBeginPos, nullptr);
}