//
// Created by m34ferna on 12/01/24.
//

#include "enjima/memory/MemoryManager.h"
#include "enjima/api/data_types/LinearRoadEvent.h"
#include "enjima/memory/MemoryUtil.h"
#include "gtest/gtest.h"
#include <climits>
#include <random>
#include <ranges>

using LinearRoadT = enjima::api::data_types::LinearRoadEvent;
using MemManT = enjima::memory::MemoryManager;
using LRRecordT = enjima::core::Record<LinearRoadT>;
using UniformIntDistParamT = std::uniform_int_distribution<int>::param_type;

class MemoryManagerTest : public testing::Test {
protected:
    void SetUp() override
    {
        pMemoryManager_ =
                new MemManT(maxMemory_, numBlocksPerChunk, numEventsPerBlock_, MemManT ::AllocatorType::kBasic);
        pMemoryManager_->InitForTesting(16);
        pMemoryManager_->StartMemoryPreAllocator();
        defaultEvent_ = generateLRTypeEvent();
    }

    void TearDown() override
    {
        pMemoryManager_->ReleaseAllResources();
    }

    LRRecordT generateLRTypeRecord()
    {
        LinearRoadT lrEvent = generateLRTypeEvent();
        uint64_t timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::high_resolution_clock::now().time_since_epoch())
                                     .count();
        return LRRecordT{timestamp, lrEvent};
    }

    LinearRoadT generateLRTypeEvent()
    {
        LinearRoadT lrEvent = LinearRoadT(0, GetRandomInt(0, 10799), GetRandomInt(0, INT_MAX), GetRandomInt(0, 100),
                GetRandomInt(0, 2), GetRandomInt(0, 4), GetRandomInt(0, 1), GetRandomInt(0, 99),
                GetRandomInt(0, 527999), 0, 0, 0, GetRandomInt(1, 7), GetRandomInt(1, 1440), GetRandomInt(1, 69));
        return lrEvent;
    }

    LinearRoadT getDefaultLRTypeEvent()
    {
        return defaultEvent_;
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
    LinearRoadT defaultEvent_{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    MemManT* pMemoryManager_ = nullptr;
    const size_t maxMemory_ = enjima::memory::KiloBytes(81988);
    const size_t numBlocksPerChunk = 16;
    const int32_t numEventsPerBlock_ = 1024;
};

TEST(MemoryManagerTestSuite, InitComponent)
{
    const size_t maxMemory = enjima::memory::MegaBytes(64);
    const int32_t defaultBlockSize = 100;
    const int32_t numBlocksPerChunk = 8;
    MemManT memoryManager(maxMemory, numBlocksPerChunk, defaultBlockSize, MemManT ::AllocatorType::kBasic);
    memoryManager.InitForTesting(16);
    memoryManager.StartMemoryPreAllocator();
    EXPECT_EQ(memoryManager.GetMaxMemory(), maxMemory);
    EXPECT_EQ(memoryManager.GetDefaultNumEventsPerBlock(), defaultBlockSize);
}

TEST_F(MemoryManagerTest, Init)
{
    EXPECT_EQ(pMemoryManager_->GetMaxMemory(), maxMemory_);
    EXPECT_EQ(pMemoryManager_->GetDefaultNumEventsPerBlock(), numEventsPerBlock_);
}

TEST_F(MemoryManagerTest, RequestEmptyBlock)
{
    auto* pBlock = pMemoryManager_->RequestEmptyBlock<LinearRoadT>(MemManT::kReservedOperatorId);
    auto* pSecondBlock = pMemoryManager_->RequestEmptyBlock<LinearRoadT>(MemManT::kReservedOperatorId);
    EXPECT_NE(pBlock->GetChunkId(), pSecondBlock->GetChunkId());
    EXPECT_EQ(pBlock->GetBlockId(), pSecondBlock->GetBlockId());
    std::cout << *pBlock << std::endl;
    std::cout << "Size of MemoryBlock: " << sizeof(*pBlock) << std::endl;

    size_t expectedCapacity = numEventsPerBlock_ * sizeof(LRRecordT);
    void* expectedEndPos = static_cast<char*>(pBlock->GetDataPtr()) + expectedCapacity;
    void* expectedDataPtr = static_cast<char*>(static_cast<void*>(pBlock)) + sizeof(enjima::memory::MemoryBlock);
    void* expectedDataPtrSecondBlock =
            static_cast<char*>(static_cast<void*>(pSecondBlock)) + sizeof(enjima::memory::MemoryBlock);
    EXPECT_EQ(pBlock->GetBlockId(), 1);
    EXPECT_EQ(pSecondBlock->GetBlockId(), 1);
    EXPECT_EQ(pBlock->GetChannelId(), MemManT::kReservedChannelId);
    EXPECT_EQ(pBlock->GetTotalCapacity(), expectedCapacity);
    EXPECT_EQ(pBlock->GetEndPtr(), expectedEndPos);
    EXPECT_EQ(pBlock->GetDataPtr(), expectedDataPtr);
    EXPECT_EQ(pBlock->GetReadPtr(), pBlock->GetDataPtr());
    EXPECT_EQ(pBlock->GetWritePtr(), pBlock->GetDataPtr());
    EXPECT_EQ(pBlock->GetCurrentSize(), 0);
    EXPECT_TRUE(pBlock->CanWrite());
    EXPECT_FALSE(pBlock->CanRead());
    EXPECT_FALSE(pBlock->IsReadComplete());
    EXPECT_EQ(pSecondBlock->GetDataPtr(), expectedDataPtrSecondBlock);
}

TEST_F(MemoryManagerTest, RequestReleaseCycles)
{
    auto blocksPerChunk = numBlocksPerChunk;
    auto* pBlock = pMemoryManager_->RequestEmptyBlock<LinearRoadT>(MemManT::kReservedOperatorId);
    pBlock->SetWriteActive();
    auto firstBlockId = pBlock->GetBlockId();
    auto firstChunkId = pBlock->GetChunkId();
    pBlock = pMemoryManager_->RequestEmptyBlock<LinearRoadT>(MemManT::kReservedOperatorId);
    pBlock->SetWriteActive();
    EXPECT_NE(pBlock->GetChunkId(), firstChunkId);
    EXPECT_EQ(pBlock->GetBlockId(), firstBlockId);

    std::vector<enjima::memory::MemoryBlock*> blkPtrVec;
    pBlock = pMemoryManager_->RequestEmptyBlock<LinearRoadT>(MemManT::kReservedOperatorId);
    pBlock->SetWriteActive();
    blkPtrVec.emplace_back(pBlock);

    enjima::memory::MemoryBlock* pPrevBlock = pBlock;
    for (decltype(blocksPerChunk) idx = 0; idx < blocksPerChunk - 3; idx++) {
        pBlock = static_cast<enjima::memory::MemoryBlock*>(pBlock->GetEndPtr());
        pBlock->SetWriteActive();
        blkPtrVec.emplace_back(pBlock);
        EXPECT_NE(pBlock->GetBlockId(), pPrevBlock->GetBlockId());
        EXPECT_EQ(pBlock->GetChunkId(), pPrevBlock->GetChunkId());
        pPrevBlock = pBlock;
    }
    pBlock = pMemoryManager_->RequestEmptyBlock<LinearRoadT>(MemManT::kReservedOperatorId);
    pBlock->SetWriteActive();
    blkPtrVec.emplace_back(pBlock);
    EXPECT_NE(pBlock->GetBlockId(), pPrevBlock->GetBlockId());
    EXPECT_NE(pBlock->GetChunkId(), pPrevBlock->GetChunkId());
    std::cout << *pBlock << std::endl;
}

TEST_F(MemoryManagerTest, BlockRecycling)
{
    auto* pBlock = pMemoryManager_->RequestEmptyBlock<LinearRoadT>(MemManT::kReservedOperatorId);
    auto firstBlockId = pBlock->GetBlockId();
    auto firstChunkId = pBlock->GetChunkId();
    EXPECT_TRUE(pBlock->SetReadActive());
    pBlock->SetWriteActive();
    pMemoryManager_->ReturnBlock(pBlock);

    pBlock = pMemoryManager_->RequestEmptyBlock<LinearRoadT>(MemManT::kReservedOperatorId);
    EXPECT_NE(pBlock->GetChunkId(), firstChunkId);
    EXPECT_EQ(pBlock->GetBlockId(), firstBlockId);
    EXPECT_TRUE(pBlock->SetReadActive());
    pBlock->SetWriteActive();
    pMemoryManager_->ReturnBlock(pBlock);

    std::vector<enjima::memory::MemoryBlock*> blkPtrVec;
    pBlock = pMemoryManager_->RequestEmptyBlock<LinearRoadT>(MemManT::kReservedOperatorId);
    auto thirdChunkId = pBlock->GetChunkId();
    EXPECT_TRUE(pBlock->SetReadActive());
    blkPtrVec.emplace_back(pBlock);

    enjima::memory::MemoryBlock* pPrevBlock = pBlock;
    for (size_t idx = 0; idx < numBlocksPerChunk - 1; idx++) {
        pBlock = static_cast<enjima::memory::MemoryBlock*>(pBlock->GetEndPtr());
        EXPECT_TRUE(pBlock->SetWriteActive());
        blkPtrVec.emplace_back(pBlock);
        EXPECT_NE(pBlock->GetBlockId(), pPrevBlock->GetBlockId());
        EXPECT_EQ(pBlock->GetChunkId(), pPrevBlock->GetChunkId());
        EXPECT_TRUE(pBlock->SetReadActive());
        pPrevBlock = pBlock;
    }

    for (const auto& pBlkFromVec: blkPtrVec) {
        pBlkFromVec->SetWriteCompleted();
        pMemoryManager_->ReturnBlock(pBlkFromVec);
    }

    // Since block reclamation happens asynchronously, we need to sleep a bit to allow reclamation to complete.
    std::this_thread::sleep_for(std::chrono::seconds(1));

    pBlock = pMemoryManager_->RequestEmptyBlock<LinearRoadT>(MemManT::kReservedOperatorId);
    for (auto pPrevStoredBlk: blkPtrVec) {
        EXPECT_EQ(pBlock->GetChunkId(), thirdChunkId);
        EXPECT_EQ(pBlock, pPrevStoredBlk);
        EXPECT_TRUE(pBlock->SetReadActive());
        pBlock = static_cast<enjima::memory::MemoryBlock*>(pBlock->GetEndPtr());
        EXPECT_TRUE(pBlock->SetWriteActive());
    }

    for (const auto& pBlkFromVec: blkPtrVec) {
        pBlkFromVec->SetWriteCompleted();
        pMemoryManager_->ReturnBlock(pBlkFromVec);
    }
}

TEST_F(MemoryManagerTest, WriteToBlock)
{
    auto* pBlock = pMemoryManager_->RequestEmptyBlock<LinearRoadT>(MemManT::kReservedOperatorId);

    auto recordToWrite = generateLRTypeRecord();
    pBlock->Write<LinearRoadT>(recordToWrite);
    EXPECT_TRUE(pBlock->CanRead());
    EXPECT_TRUE(pBlock->CanWrite());
    EXPECT_EQ(pBlock->GetCurrentSize(), sizeof(recordToWrite));
}

TEST_F(MemoryManagerTest, ReadFromBlock)
{
    auto* pBlock = pMemoryManager_->RequestEmptyBlock<LinearRoadT>(MemManT::kReservedOperatorId);

    auto recordToWrite = generateLRTypeRecord();
    pBlock->Write<LinearRoadT>(recordToWrite);
    auto recordRead = pBlock->Read<LinearRoadT>();
    EXPECT_FALSE(pBlock->CanRead());
    EXPECT_TRUE(pBlock->CanWrite());
    EXPECT_EQ(recordToWrite.GetTimestamp(), recordRead->GetTimestamp());

    auto dataWritten = recordToWrite.GetData();
    auto dataRead = recordRead->GetData();
    EXPECT_EQ(dataRead.GetType(), dataWritten.GetType());
    EXPECT_EQ(dataRead.GetTimestamp(), dataWritten.GetTimestamp());
    EXPECT_EQ(dataRead.GetSpeed(), dataWritten.GetSpeed());
    EXPECT_EQ(dataRead.GetPos(), dataWritten.GetPos());
    EXPECT_EQ(dataRead.GetDay(), dataWritten.GetDay());
}

TEST_F(MemoryManagerTest, WriteMultiple)
{
    auto* pBlock = pMemoryManager_->RequestEmptyBlock<LinearRoadT>(MemManT::kReservedOperatorId);

    int numRecordsWritten = 0;
    while (pBlock->CanWrite()) {
        auto recordToWrite = generateLRTypeRecord();
        pBlock->Write<LinearRoadT>(recordToWrite);
        numRecordsWritten++;
    }
    EXPECT_TRUE(pBlock->CanRead());
    EXPECT_FALSE(pBlock->CanWrite());
    EXPECT_EQ(pBlock->GetCurrentSize(), pBlock->GetTotalCapacity());
    EXPECT_EQ(numRecordsWritten, numEventsPerBlock_);

    std::cout << *pBlock << std::endl;
}

TEST_F(MemoryManagerTest, EmplaceMultiple)
{
    auto* pBlock = pMemoryManager_->RequestEmptyBlock<LinearRoadT>(MemManT::kReservedOperatorId);

    int numRecordsWritten = 0;
    while (pBlock->CanWrite()) {
        auto eventToWrite = generateLRTypeEvent();
        uint64_t timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::high_resolution_clock::now().time_since_epoch())
                                     .count();
        pBlock->Emplace<LinearRoadT>(timestamp, eventToWrite);
        numRecordsWritten++;
    }
    EXPECT_TRUE(pBlock->CanRead());
    EXPECT_FALSE(pBlock->CanWrite());
    EXPECT_EQ(pBlock->GetCurrentSize(), pBlock->GetTotalCapacity());
    EXPECT_EQ(numRecordsWritten, numEventsPerBlock_);

    std::cout << *pBlock << std::endl;
}

TEST_F(MemoryManagerTest, ReadMultiple)
{
    auto* pBlock = pMemoryManager_->RequestEmptyBlock<LinearRoadT>(MemManT::kReservedOperatorId);

    int numRecordsWritten = 0;
    while (pBlock->CanWrite()) {
        auto recordToWrite = generateLRTypeRecord();
        pBlock->Write<LinearRoadT>(recordToWrite);
        numRecordsWritten++;
    }

    int numRecordsRead = 0;
    while (pBlock->CanRead()) {
        auto recordRead = pBlock->Read<LinearRoadT>();
        EXPECT_NE(recordRead, nullptr);
        auto eventRead = recordRead->GetData();
        EXPECT_EQ(eventRead.GetType(), 0);
        EXPECT_TRUE(eventRead.GetSpeed() >= 0 && eventRead.GetSpeed() <= 100);
        EXPECT_TRUE(eventRead.GetDay() > 0 && eventRead.GetDay() < 70);
        numRecordsRead++;
    }
    EXPECT_EQ(numRecordsRead, numRecordsWritten);
    EXPECT_FALSE(pBlock->CanRead());
    EXPECT_FALSE(pBlock->CanWrite());
    EXPECT_TRUE(pBlock->IsReadComplete());

    auto additionalRecordRead = pBlock->Read<LinearRoadT>();
    EXPECT_EQ(additionalRecordRead, nullptr);

    std::cout << *pBlock << std::endl;
}

TEST_F(MemoryManagerTest, LargeFastWrite)
{
    std::vector<enjima::memory::MemoryBlock*> blkPtrVec;
    auto* pBlockWrite = pMemoryManager_->RequestEmptyBlock<LinearRoadT>(MemManT::kReservedOperatorId);
    int numRecordsWritten = 0;
    auto writeStartAt = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch())
                                .count();
    while (pBlockWrite != nullptr) {
        while (pBlockWrite->CanWrite()) {
            auto eventToWrite = getDefaultLRTypeEvent();
            uint64_t timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::high_resolution_clock::now().time_since_epoch())
                                         .count();
            pBlockWrite->Emplace<LinearRoadT>(timestamp, eventToWrite);
            numRecordsWritten++;
        }
        EXPECT_FALSE(pBlockWrite->CanWrite());
        blkPtrVec.emplace_back(pBlockWrite);
        if (pBlockWrite->GetEndPtr() < pBlockWrite->GetChunkPtr()->GetEndPtr()) {
            pBlockWrite = static_cast<enjima::memory::MemoryBlock*>(pBlockWrite->GetEndPtr());
        }
        else {
            pBlockWrite = pMemoryManager_->RequestEmptyBlock<LinearRoadT>(MemManT::kReservedOperatorId);
        }
    }
    auto currentTime = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch())
                               .count();
    std::cout << "Took " << currentTime - writeStartAt << " ms to write " << numRecordsWritten << " records"
              << std::endl;

    EXPECT_EQ(pMemoryManager_->GetCurrentlyAllocatedMemory(), pMemoryManager_->GetMaxMemory());
}

TEST_F(MemoryManagerTest, LargeFastWriteReadInBatches)
{
    std::vector<LRRecordT> records;
    uint32_t batchSize = 128;
    for (auto i = 0u; i < batchSize; i++) {
        uint64_t currentTimestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::high_resolution_clock::now().time_since_epoch())
                                            .count();
        records.emplace_back(currentTimestamp, generateLRTypeEvent());
    }
    EXPECT_EQ(records.size(), batchSize);
    std::vector<enjima::memory::MemoryBlock*> blkPtrVec;
    auto* pBlockWrite = pMemoryManager_->RequestEmptyBlock<LinearRoadT>(MemManT::kReservedOperatorId);
    size_t numRecordsWritten = 0;
    auto writeStartAt = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch())
                                .count();
    while (pBlockWrite != nullptr) {
        while (pBlockWrite->CanWrite()) {
            pBlockWrite->WriteBatch<LinearRoadT>(records.data(), batchSize);
            numRecordsWritten += records.size();
        }
        EXPECT_FALSE(pBlockWrite->CanWrite());
        blkPtrVec.emplace_back(pBlockWrite);
        if (pBlockWrite->GetEndPtr() < pBlockWrite->GetChunkPtr()->GetEndPtr()) {
            pBlockWrite = static_cast<enjima::memory::MemoryBlock*>(pBlockWrite->GetEndPtr());
        }
        else {
            pBlockWrite = pMemoryManager_->RequestEmptyBlock<LinearRoadT>(MemManT::kReservedOperatorId);
        }
    }
    auto currentTime = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch())
                               .count();
    std::cout << "Took " << currentTime - writeStartAt << " ms to write " << numRecordsWritten << " records"
              << std::endl;
    auto sizeOfBlock =
            numEventsPerBlock_ * sizeof(enjima::core::Record<LinearRoadT>) + sizeof(enjima::memory::MemoryBlock);
    auto sizeOfChunk = numBlocksPerChunk * sizeOfBlock + sizeof(enjima::memory::MemoryChunk);
    auto minMemoryAlloc = sizeOfChunk * 16;
    EXPECT_EQ(pMemoryManager_->GetCurrentlyAllocatedMemory(), minMemoryAlloc);
    int numRecordsRead = 0;
    void* inputBuffer = malloc(batchSize * sizeof(LRRecordT));

    auto readStartAt = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch())
                               .count();
    for (auto pBlockRead: blkPtrVec) {
        EXPECT_NE(pBlockRead, nullptr);
        while (pBlockRead->CanRead()) {
            auto recordsRead = pBlockRead->ReadBatch<LinearRoadT>(inputBuffer, batchSize);
            EXPECT_EQ(recordsRead, batchSize);
            for (auto i = 0u; i < batchSize; i++) {
                auto currentRecord = static_cast<LRRecordT*>(inputBuffer) + i;
                auto eventRead = currentRecord->GetData();
                auto recordWritten = records.at(i);
                auto eventWritten = recordWritten.GetData();
                EXPECT_EQ(eventRead.GetType(), eventWritten.GetType());
                EXPECT_TRUE(eventRead.GetSpeed() >= 0 && eventRead.GetSpeed() <= 100);
                EXPECT_TRUE(eventRead.GetDay() > 0 && eventRead.GetDay() < 70);
                EXPECT_EQ(currentRecord->GetTimestamp(), recordWritten.GetTimestamp());
                EXPECT_EQ(eventRead.GetSpeed(), eventWritten.GetSpeed());
                EXPECT_EQ(eventRead.GetDay(), eventWritten.GetDay());
                EXPECT_EQ(eventRead.GetType(), eventWritten.GetType());
                EXPECT_EQ(eventRead.GetTimestamp(), eventWritten.GetTimestamp());
                EXPECT_EQ(eventRead.GetPos(), eventWritten.GetPos());
                EXPECT_EQ(eventRead.GetDir(), eventWritten.GetDir());
                EXPECT_EQ(eventRead.GetDow(), eventWritten.GetDow());
                EXPECT_EQ(eventRead.GetLane(), eventWritten.GetLane());
                EXPECT_EQ(eventRead.GetQid(), eventWritten.GetQid());
                EXPECT_EQ(eventRead.GetSeg(), eventWritten.GetSeg());
                EXPECT_EQ(eventRead.GetSend(), eventWritten.GetSend());
                EXPECT_EQ(eventRead.GetSinit(), eventWritten.GetSinit());
                EXPECT_EQ(eventRead.GetTod(), eventWritten.GetTod());
                numRecordsRead++;
            }
        }
        EXPECT_FALSE(pBlockRead->CanRead());
        EXPECT_FALSE(pBlockRead->CanWrite());
        EXPECT_TRUE(pBlockRead->IsReadComplete());
        auto additionalRecordRead = pBlockRead->Read<LinearRoadT>();
        EXPECT_EQ(additionalRecordRead, nullptr);
    }
    currentTime = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch())
                          .count();
    std::cout << "Took " << currentTime - readStartAt << " ms to read " << numRecordsRead << " records" << std::endl;

    EXPECT_EQ(numRecordsRead, numRecordsWritten);
}

TEST_F(MemoryManagerTest, LargeRandomWriteRead)
{
    std::vector<enjima::memory::MemoryBlock*> blkPtrVec;
    auto* pBlockWrite = pMemoryManager_->RequestEmptyBlock<LinearRoadT>(MemManT::kReservedOperatorId);
    int numRecordsWritten = 0;
    auto writeStartAt = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch())
                                .count();
    while (pBlockWrite != nullptr) {
        while (pBlockWrite->CanWrite()) {
            auto eventToWrite = generateLRTypeEvent();
            uint64_t timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::high_resolution_clock::now().time_since_epoch())
                                         .count();
            pBlockWrite->Emplace<LinearRoadT>(timestamp, eventToWrite);
            numRecordsWritten++;
        }
        EXPECT_FALSE(pBlockWrite->CanWrite());
        blkPtrVec.emplace_back(pBlockWrite);
        if (pBlockWrite->GetEndPtr() < pBlockWrite->GetChunkPtr()->GetEndPtr()) {
            pBlockWrite = static_cast<enjima::memory::MemoryBlock*>(pBlockWrite->GetEndPtr());
        }
        else {
            pBlockWrite = pMemoryManager_->RequestEmptyBlock<LinearRoadT>(MemManT::kReservedOperatorId);
        }
    }
    auto currentTime = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch())
                               .count();
    std::cout << "Took " << currentTime - writeStartAt << " ms to write " << numRecordsWritten << " records"
              << std::endl;

    EXPECT_EQ(pMemoryManager_->GetCurrentlyAllocatedMemory(), pMemoryManager_->GetMaxMemory());
    int numRecordsRead = 0;

    auto readStartAt = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch())
                               .count();
    for (auto pBlockRead: blkPtrVec) {
        EXPECT_NE(pBlockRead, nullptr);
        while (pBlockRead->CanRead()) {
            auto recordRead = pBlockRead->Read<LinearRoadT>();
            EXPECT_NE(recordRead, nullptr);
            auto eventRead = recordRead->GetData();
            EXPECT_EQ(eventRead.GetType(), 0);
            EXPECT_TRUE(eventRead.GetSpeed() >= 0 && eventRead.GetSpeed() <= 100);
            EXPECT_TRUE(eventRead.GetDay() > 0 && eventRead.GetDay() < 70);
            numRecordsRead++;
        }
        EXPECT_FALSE(pBlockRead->CanRead());
        EXPECT_FALSE(pBlockRead->CanWrite());
        EXPECT_TRUE(pBlockRead->IsReadComplete());
        auto additionalRecordRead = pBlockRead->Read<LinearRoadT>();
        EXPECT_EQ(additionalRecordRead, nullptr);
    }
    currentTime = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch())
                          .count();
    std::cout << "Took " << currentTime - readStartAt << " ms to read " << numRecordsRead << " records" << std::endl;

    EXPECT_EQ(numRecordsRead, numRecordsWritten);
}
