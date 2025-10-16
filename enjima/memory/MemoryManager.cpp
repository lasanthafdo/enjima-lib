//
// Created by m34ferna on 05/01/24.
//


#include "MemoryManager.h"
#include "AdaptiveMemoryAllocator.h"
#include "BasicMemoryAllocator.h"
#include "enjima/api/data_types/LinearRoadEvent.h"
#include "enjima/core/StreamingPipeline.h"
#include "enjima/runtime/AlreadyExistsException.h"
#include "enjima/runtime/IllegalStateException.h"
#include "enjima/runtime/RuntimeUtil.h"
#include "spdlog/spdlog.h"
#include <iostream>

namespace enjima::memory {

    const uint64_t MemoryManager::kReservedOperatorId = 0;
    const uint64_t MemoryManager::kReservedChannelId = 0;
    const size_t MemoryManager::kMinMemory = 1'048'576;
    const size_t MemoryManager::kMinBlocksPerChunk = 2;
    const uint8_t MemoryManager::kNumInitialChunksPerOperator = 2;

    using DefaultEventType = api::data_types::LinearRoadEvent;

    uint64_t MemoryManager::DeriveBlockId(size_t blockSize, const char* pChunkData, const char* pBlk)
    {
        SPDLOG_DEBUG("chunk address: {}, block address: {}, block id: {}", (void*) pChunkData, (void*) pBlk,
                1 + ((pBlk - pChunkData) / blockSize));
        return 1 + ((pBlk - pChunkData) / blockSize);
    }

    MemoryManager::MemoryManager(size_t maxMemory, size_t numBlocksPerChunk, int32_t defaultNumRecsPerBlock,
            AllocatorType allocatorType)
        : maxMemory_(maxMemory), numBlocksPerChunk_(numBlocksPerChunk), defaultNumRecsPerBlock_(defaultNumRecsPerBlock),
          allocatorType_(allocatorType)
    {
        if (maxMemory < kMinMemory || numBlocksPerChunk < kMinBlocksPerChunk) {
            throw std::invalid_argument("Max memory should be >= " + std::to_string(kMinMemory) +
                                        " and chunk size should be >= " + std::to_string(kMinBlocksPerChunk));
        }
        if (defaultNumRecsPerBlock % hardware_constructive_interference_size != 0) {
            throw std::invalid_argument("Num records per block should be multiple of " +
                                        std::to_string(hardware_constructive_interference_size));
        }

        switch (allocatorType_) {
            case AllocatorType::kAdaptive:
                allocator_ = new AdaptiveMemoryAllocator;
                break;
            case AllocatorType::kBasic:
            default:
                allocator_ = new BasicMemoryAllocator;
                break;
        }
        pAllocCoordinator_ = new MemoryAllocationCoordinator(this);
        numEventsPerBlocksByOpId_[kReservedOperatorId] = 0;
        auto iterBool = readableFirstBlockOfChunkByChannelId_.emplace(kReservedChannelId,
                ConcurrentBoundedQueueTBB<MemoryBlock*>{});
        if (iterBool.second) {
            auto& concurrentQueue = iterBool.first->second;
            concurrentQueue.set_capacity(
                    (ConcurrentBoundedQueueTBB<MemoryBlock*>::size_type) maxActiveChunksPerOperator_);
        }
        else {
            throw runtime::IllegalStateException{std::string(
                    "Could not allocate and emplace queue of readable memory block pointers for channel id ")
                            .append(std::to_string(kReservedChannelId))};
        }
    }

    MemoryManager::~MemoryManager()
    {
        if (!releasedResources_) {
            ReleaseAllResources();
        }
        delete pAllocCoordinator_;
        delete allocator_;
    }

    void MemoryManager::InitForTesting(uint32_t initAllocatedChunks)
    {
        spdlog::warn("This method should only be called during unit-testing!");
        // This initialization is only for testing with the reserved operator ID
        numEventsPerBlocksByOpId_[kReservedOperatorId] = defaultNumRecsPerBlock_;
        opIdsByChannelId_[kReservedChannelId] = kReservedOperatorId;
        activeChunkCountByChannelId_.emplace(kReservedChannelId, 0);
        outputRecordSizeByOpId_[kReservedOperatorId] = sizeof(core::Record<DefaultEventType>);
        for (unsigned int i = 0; i < initAllocatedChunks; i++) {
            TryInitializeAndGetChunk(kReservedChannelId);
        }
        initialized_ = true;
    }

    void MemoryManager::Init(bool preAllocationEnabled)
    {
        if (initialized_) {
            throw runtime::IllegalStateException("Memory manager has already being initialized!");
        }
        preAllocationEnabled_ = preAllocationEnabled;
        outputRecordSizeByOpId_[kReservedOperatorId] = 0;
        opIdsByChannelId_[kReservedChannelId] = kReservedOperatorId;
        size_t chunkSize = GetReservedChunkSize();
        auto* pMemoryChunk = InitializeAndGetChunkMT(kReservedChannelId, chunkSize);
        InitReservedMemoryBlock(pMemoryChunk);
        initialized_ = true;
    }

    void MemoryManager::InitReservedMemoryBlock(const MemoryChunk* pReservedMemoryChunk)
    {
        // The reserved memory block is always marked as returned and used as a placeholder instead of nullptr
        // when a new read/write block cannot be obtained by an operator
        reservedMemoryBlock_ = static_cast<MemoryBlock*>(pReservedMemoryChunk->GetDataPtr());
        reservedMemoryBlock_->SetWriteActive();
        auto currentTimeMillis = runtime::GetSystemTimeMillis();
        while (reservedMemoryBlock_->CanWrite()) {
            reservedMemoryBlock_->Write(core::Record<DefaultEventType>(
                    core::Record<DefaultEventType>::RecordType::kLatency, currentTimeMillis));
        }
        reservedMemoryBlock_->SetWriteCompleted();
        reservedMemoryBlock_->SetReadActive();
        while (reservedMemoryBlock_->CanRead()) {
            reservedMemoryBlock_->Read<DefaultEventType>();
        }
        reservedMemoryBlock_->SetBlockReturned();
        assert(reservedMemoryBlock_->GetBlockId() == 1);
    }

    inline size_t MemoryManager::GetChunkSize(operators::OperatorID operatorId) const
    {
        return GetBlockSize(operatorId) * numBlocksPerChunk_ + sizeof(MemoryChunk);
    }

    inline size_t MemoryManager::GetReservedChunkSize() const
    {
        return GetBlockSize(0) * 1 + sizeof(MemoryChunk);
    }

    inline size_t MemoryManager::GetBlockSize(operators::OperatorID operatorId) const
    {
        return GetBlockDataSize(operatorId) + sizeof(MemoryBlock);
    }

    inline size_t MemoryManager::GetBlockDataSize(operators::OperatorID operatorId) const
    {
        return numEventsPerBlocksByOpId_.at(operatorId) * outputRecordSizeByOpId_.at(operatorId);
    }

    void MemoryManager::InitializePipeline(core::StreamingPipeline* pipelinePtr)
    {
        if (preAllocationEnabled_) {
            pAllocCoordinator_->PreAllocatePipeline(pipelinePtr);
        }
        else {
            PreAllocatePipelineMT(pipelinePtr);
        }
    }

    void MemoryManager::PreAllocatePipeline(const core::StreamingPipeline* pipelinePtr)
    {
        const auto& opVec = pipelinePtr->GetOperatorsInTopologicalOrder();
        for (auto streamOpPtr: opVec) {
            auto opId = streamOpPtr->GetOperatorId();
            auto downstreamOpIdVec = pipelinePtr->GetDownstreamOperatorIDs(opId);
            // numEventsPerBlock and outputRecordSize has to be set before calling GetChunkSize
            numEventsPerBlocksByOpId_[opId] = defaultNumRecsPerBlock_;
            outputRecordSizeByOpId_[opId] = streamOpPtr->GetOutputRecordSize();
            auto chunkSize = GetChunkSize(opId);
            for (auto downstreamOpId: downstreamOpIdVec) {
                auto downstreamChannelId = GetNextChannelId();
                downstreamChannelIdsByOpId_[opId].emplace_back(downstreamChannelId);
                opIdsByChannelId_.emplace(downstreamChannelId, opId);
                channelIdMapping_.emplace(downstreamChannelId, std::make_pair(opId, downstreamOpId));
                reverseChannelIdMapping_.emplace(std::make_pair(opId, downstreamOpId), downstreamChannelId);
                for (auto i = 0; i < kNumInitialChunksPerOperator; i++) {
                    InitializeAndGetChunk(downstreamChannelId, chunkSize);
                }
                auto emplaceStatus = activeChunkCountByChannelId_.emplace(downstreamChannelId, 0);
                if (!emplaceStatus.second) {
                    throw runtime::AlreadyExistsException(std::string(
                            "Active count chunk already exists for operator with id " + std::to_string(opId) +
                            " and channel id " + std::to_string(downstreamChannelId)));
                }
            }
            auto upstreamOpIdVec = pipelinePtr->GetUpstreamOperatorIDs(opId);
            for (auto upstreamOpId: upstreamOpIdVec) {
                auto upstreamChannelId = reverseChannelIdMapping_.at(std::make_pair(upstreamOpId, opId));
                upstreamChannelIdsByOpId_[opId].emplace_back(upstreamChannelId);
                auto iterBool = readableFirstBlockOfChunkByChannelId_.emplace(upstreamChannelId,
                        ConcurrentBoundedQueueTBB<MemoryBlock*>{});
                if (iterBool.second) {
                    auto& concurrentQueue = iterBool.first->second;
                    concurrentQueue.set_capacity(
                            (ConcurrentBoundedQueueTBB<MemoryBlock*>::size_type) maxActiveChunksPerOperator_);
                }
                else {
                    throw runtime::IllegalStateException{std::string(
                            "Could not allocate and emplace queue of readable memory block pointers for channel id ")
                                    .append(std::to_string(upstreamChannelId))};
                }
            }
        }
    }

    void MemoryManager::PreAllocatePipelineMT(const core::StreamingPipeline* pipelinePtr)
    {
        const auto& opVec = pipelinePtr->GetOperatorsInTopologicalOrder();
        // Get exclusive lock for writing to the std::unordered_map variables
        std::unique_lock<std::shared_mutex> uniqueLock{sharedMapsMutex_};
        for (auto streamOpPtr: opVec) {
            auto opId = streamOpPtr->GetOperatorId();
            auto downstreamOpIdVec = pipelinePtr->GetDownstreamOperatorIDs(opId);
            // numEventsPerBlock and outputRecordSize has to be set before calling GetChunkSize
            numEventsPerBlocksByOpId_[opId] = defaultNumRecsPerBlock_;
            outputRecordSizeByOpId_[opId] = streamOpPtr->GetOutputRecordSize();
            auto chunkSize = GetChunkSize(opId);
            for (auto downstreamOpId: downstreamOpIdVec) {
                auto downstreamChannelId = GetNextChannelId();
                downstreamChannelIdsByOpId_[opId].emplace_back(downstreamChannelId);
                opIdsByChannelId_.emplace(downstreamChannelId, opId);
                channelIdMapping_.emplace(downstreamChannelId, std::make_pair(opId, downstreamOpId));
                reverseChannelIdMapping_.emplace(std::make_pair(opId, downstreamOpId), downstreamChannelId);
                for (auto i = 0; i < kNumInitialChunksPerOperator; i++) {
                    InitializeAndGetChunkMT(downstreamChannelId, chunkSize);
                }
                auto emplaceStatus = activeChunkCountByChannelId_.emplace(downstreamChannelId, 0);
                if (!emplaceStatus.second) {
                    throw runtime::AlreadyExistsException(std::string(
                            "Active count chunk already exists for operator with id " + std::to_string(opId) +
                            " and channel id " + std::to_string(downstreamChannelId)));
                }
            }
            auto upstreamOpIdVec = pipelinePtr->GetUpstreamOperatorIDs(opId);
            for (auto upstreamOpId: upstreamOpIdVec) {
                auto upstreamChannelId = reverseChannelIdMapping_.at(std::make_pair(upstreamOpId, opId));
                upstreamChannelIdsByOpId_[opId].emplace_back(upstreamChannelId);
                auto iterBool = readableFirstBlockOfChunkByChannelId_.emplace(upstreamChannelId,
                        ConcurrentBoundedQueueTBB<MemoryBlock*>{});
                if (iterBool.second) {
                    auto& concurrentQueue = iterBool.first->second;
                    concurrentQueue.set_capacity(
                            (ConcurrentBoundedQueueTBB<MemoryBlock*>::size_type) maxActiveChunksPerOperator_);
                }
                else {
                    throw runtime::IllegalStateException{std::string(
                            "Could not allocate and emplace queue of readable memory block pointers for channel id ")
                                    .append(std::to_string(upstreamChannelId))};
                }
            }
        }
    }

    MemoryChunk* MemoryManager::GetNextFreeChunk(operators::ChannelID channelId)
    {
        auto opChunkMap = chunkMapsByChannelId_.at(channelId);
        MemoryChunk* pCandidateChunk = nullptr;
        for (const auto& chunkItr: opChunkMap) {
            if (!chunkItr.second->IsInUse()) {
                pCandidateChunk = chunkItr.second;
                break;
            }
        }
        return pCandidateChunk;
    }

    MemoryChunk* MemoryManager::TryInitializeAndGetChunk(unsigned long channelId)
    {
        MemoryChunk* pCandidateChunk = nullptr;
        auto operatorId = opIdsByChannelId_.at(channelId);
        auto chunkSize = GetChunkSize(operatorId);
        // This method should only be called by the memory allocation thread (except when testing).
        // So relaxed memory ordering for currentlyAllocatedMemory_ is fine
        if ((currentlyAllocatedMemory_.load(std::memory_order::relaxed) + chunkSize) <= maxMemory_) {
            pCandidateChunk = InitializeAndGetChunk(channelId, chunkSize);
        }
        else {
            auto currentTime = enjima::runtime::GetSystemTimeMillis();
            if (currentTime - lastMemoryWarningAt_ > 1000) {
                spdlog::warn("Not enough memory to allocate more chunks for channel with id {} of operator id {}",
                        channelId, operatorId);
                lastMemoryWarningAt_ = currentTime;
            }
        }
        return pCandidateChunk;
    }

    MemoryChunk* MemoryManager::InitializeAndGetChunk(unsigned long channelId, size_t chunkSize)
    {
        auto operatorId = opIdsByChannelId_.at(channelId);
        auto recordSize = outputRecordSizeByOpId_.at(operatorId);
        auto* pChunk = AllocateChunk(operatorId, chunkSize, recordSize);
        auto reqBlkDataSize = GetBlockDataSize(operatorId);
        PopulateBlocksForChunk(channelId, reqBlkDataSize, pChunk);
        chunkMapsByChannelId_[channelId].insert({pChunk->GetChunkId(), pChunk});
        return pChunk;
    }

    MemoryChunk* MemoryManager::AllocateChunk(operators::OperatorID operatorId, size_t chunkSize, size_t recordSize)
    {
        auto blockSize = sizeof(MemoryBlock) + numEventsPerBlocksByOpId_[operatorId] * recordSize;
        auto pChunk = allocator_->Allocate(chunkSize, blockSize);
        // This method should only be called by the memory allocation thread or during initialization.
        // So relaxed memory ordering for currentlyAllocatedMemory_ is fine
        currentlyAllocatedMemory_.fetch_add(chunkSize, std::memory_order::relaxed);
        return pChunk;
    }

    MemoryChunk* MemoryManager::TryInitializeAndGetChunkMT(unsigned long channelId)
    {
        // Acquire shared lock to read from shared std::unordered_maps
        std::shared_lock<std::shared_mutex> sharedLock{sharedMapsMutex_};
        MemoryChunk* pCandidateChunk = nullptr;
        auto operatorId = opIdsByChannelId_.at(channelId);
        auto chunkSize = GetChunkSize(operatorId);
        if ((currentlyAllocatedMemory_.load(std::memory_order::acquire) + chunkSize) <= maxMemory_) {
            pCandidateChunk = InitializeAndGetChunkMT(channelId, chunkSize);
        }
        else {
            auto currentTime = enjima::runtime::GetSystemTimeMillis();
            if (currentTime - lastMemoryWarningAt_ > 1000) {
                spdlog::warn("Not enough memory to allocate more chunks for channel with id {} of operator id {}",
                        channelId, operatorId);
                lastMemoryWarningAt_ = currentTime;
            }
        }
        return pCandidateChunk;
    }

    MemoryChunk* MemoryManager::InitializeAndGetChunkMT(unsigned long channelId, size_t chunkSize)
    {
        auto operatorId = opIdsByChannelId_.at(channelId);
        auto recordSize = outputRecordSizeByOpId_.at(operatorId);
        auto* pChunk = AllocateChunkMT(operatorId, chunkSize, recordSize);
        auto reqBlkDataSize = GetBlockDataSize(operatorId);
        PopulateBlocksForChunk(channelId, reqBlkDataSize, pChunk);
        chunkMapsByChannelId_[channelId].insert({pChunk->GetChunkId(), pChunk});
        return pChunk;
    }

    MemoryChunk* MemoryManager::AllocateChunkMT(operators::OperatorID operatorId, size_t chunkSize, size_t recordSize)
    {
        auto blockSize = sizeof(MemoryBlock) + numEventsPerBlocksByOpId_[operatorId] * recordSize;
        auto pChunk = allocator_->Allocate(chunkSize, blockSize);
        currentlyAllocatedMemory_.fetch_add(chunkSize, std::memory_order::acq_rel);
        return pChunk;
    }

    size_t MemoryManager::GetMaxMemory() const
    {
        return maxMemory_;
    }

    size_t MemoryManager::GetDefaultNumEventsPerBlock() const
    {
        return defaultNumRecsPerBlock_;
    }

    size_t MemoryManager::GetCurrentlyAllocatedMemory() const
    {
        return currentlyAllocatedMemory_.load(std::memory_order::acquire);
    }

    MemoryBlock* MemoryManager::RequestReadableBlock(operators::ChannelID channelId)
    {
        MemoryBlock* pReadableBlk = nullptr;
        if (readableFirstBlockOfChunkByChannelId_.at(channelId).try_pop(pReadableBlk)) {
            pReadableBlk->SetReadActive();
            return pReadableBlk;
        }
        return nullptr;
    }

    void MemoryManager::ReturnBlock(MemoryBlock* pBlock)
    {
        if (pBlock != nullptr) {
            // assert(pBlock->GetOperatorId() != 0);
            assert(pBlock->IsWriteActive());
            assert(pBlock->IsReadActive());
            auto pChunk = pBlock->GetChunkPtr();
            auto chunkId = pChunk->GetChunkId();
            ConcurrentUnorderedSetTBB<MemoryBlock*>* pFreeBlockSetForChunk = nullptr;
            ConcurrentHashMapTBB<ChunkID, ConcurrentUnorderedSetTBB<MemoryBlock*>*>::const_accessor hmConstAccessor;
            if (!reclaimableBlocksByChunkId_.find(hmConstAccessor, chunkId)) {
                pFreeBlockSetForChunk = new ConcurrentUnorderedSetTBB<MemoryBlock*>;
                reclaimableBlocksByChunkId_.emplace(chunkId, pFreeBlockSetForChunk);
            }
            else {
                pFreeBlockSetForChunk = hmConstAccessor->second;
            }
            pFreeBlockSetForChunk->emplace(pBlock);
            pBlock->SetBlockReturned();

            auto blocksPerChunk = pChunk->GetNumBlocks();
            if (pFreeBlockSetForChunk->size() == blocksPerChunk) {
                assert(pBlock->GetBlockId() == blocksPerChunk);
                pAllocCoordinator_->EnqueueChunkReclamationRequest(chunkId);
            }
        }
    }

    bool MemoryManager::ReleaseChunk(ChunkID chunkId)
    {
        ConcurrentHashMapTBB<ChunkID, ConcurrentUnorderedSetTBB<MemoryBlock*>*>::const_accessor hmConstAccessor;
        if (reclaimableBlocksByChunkId_.find(hmConstAccessor, chunkId)) {
            auto pFreeBlockSetForChunk = hmConstAccessor->second;
            for (auto& pFreeBlock: *pFreeBlockSetForChunk) {
                if (!pFreeBlock->IsWriteCompleted()) {
                    return false;
                }
            }
            for (auto& pFreeBlock: *pFreeBlockSetForChunk) {
                pFreeBlock->SetReadWriteInactive();
            }
            auto pFirstBlock = *pFreeBlockSetForChunk->begin();
            auto pChunk = pFirstBlock->GetChunkPtr();
            auto channelId = pFirstBlock->GetChannelId();
            if (reclaimableBlocksByChunkId_.erase(hmConstAccessor)) {
                delete pFreeBlockSetForChunk;
            }
            pChunk->SetInUse(false);
            activeChunkCountByChannelId_.at(channelId).fetch_sub(1, std::memory_order::acq_rel);
            return true;
        }
        return false;
    }

    void MemoryManager::PopulateBlocksForChunk(unsigned long channelId, size_t reqBlkDataSize,
            MemoryChunk* pCandidateChunk)
    {
        auto pBlk = pCandidateChunk->ReserveBlock();
        while (pBlk != nullptr) {
            auto reqBlkSize = sizeof(MemoryBlock) + reqBlkDataSize;
            auto blockId = DeriveBlockId(reqBlkSize,
                    static_cast<const char*>(static_cast<void*>(pCandidateChunk->GetDataPtr())),
                    static_cast<char*>(pBlk));
            new (pBlk) MemoryBlock{blockId, pCandidateChunk, channelId, static_cast<uint32_t>(reqBlkDataSize), pBlk};
            pBlk = pCandidateChunk->ReserveBlock();
        }
    }

    bool MemoryManager::PreAllocateMemoryChunk(operators::ChannelID channelId)
    {
        SPDLOG_DEBUG("Pre-allocating memory chunk for channel with id {}", channelId);
        auto pCandidateChunk = TryInitializeAndGetChunk(channelId);
        return pCandidateChunk != nullptr;
    }

    void MemoryManager::ReleaseAllResources()
    {
        bool expected = false;
        if (releasedResources_.compare_exchange_strong(expected, true)) {
            spdlog::info("Cleaning up release manager...");
            pAllocCoordinator_->Stop();
            pAllocCoordinator_->JoinThread();
            spdlog::info("Pre-allocator cleaned-up");
            for ([[maybe_unused]] auto& [chunkId, emptyBlockPtrSetPtr]: reclaimableBlocksByChunkId_) {
                delete emptyBlockPtrSetPtr;
            }
            for ([[maybe_unused]] auto& [channelId, chunkPtrMap]: chunkMapsByChannelId_) {
                for ([[maybe_unused]] const auto& [chunkId, pChunkPtr]: chunkPtrMap) {
                    auto chunkSize = pChunkPtr->GetCapacity() + sizeof(MemoryChunk);
                    allocator_->Deallocate(pChunkPtr);
                    currentlyAllocatedMemory_.fetch_sub(chunkSize);
                }
                chunkPtrMap.clear();
            }
            chunkMapsByChannelId_.clear();
            assert(currentlyAllocatedMemory_.load(std::memory_order::acquire) == 0);
            spdlog::info("Released all memory allocated by memory manager");
        }
    }

    MemoryBlock* MemoryManager::GetReservedMemoryBlock() const
    {
        return reservedMemoryBlock_;
    }

    void MemoryManager::StartMemoryPreAllocator()
    {
        pAllocCoordinator_->Start();
    }

    void MemoryManager::SetMaxActiveChunksPerOperator(uint64_t maxActiveChunksPerOperator)
    {
        if (initialized_) {
            throw runtime::IllegalStateException{
                    std::string("maxActiveChunksPerOperator has to be set before initialization!")};
        }
        maxActiveChunksPerOperator_ = maxActiveChunksPerOperator;
    }

    operators::ChannelID MemoryManager::GetNextChannelId()
    {
        return nextChannelId_++;
    }

    const std::vector<operators::ChannelID>& MemoryManager::GetUpstreamChannelIds(operators::OperatorID opId) const
    {
        return upstreamChannelIdsByOpId_.at(opId);
    }

    const std::vector<operators::ChannelID>& MemoryManager::GetDownstreamChannelIds(operators::OperatorID opId) const
    {
        return downstreamChannelIdsByOpId_.at(opId);
    }

    operators::ChannelID MemoryManager::GetChannelID(operators::OperatorID upstreamOpId,
            operators::OperatorID downstreamOpId) const
    {
        return reverseChannelIdMapping_.at(std::make_pair(upstreamOpId, downstreamOpId));
    }

    bool MemoryManager::IsPreAllocationEnabled() const
    {
        return preAllocationEnabled_;
    }

}// namespace enjima::memory
