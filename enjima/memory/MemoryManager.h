//
// Created by m34ferna on 05/01/24.
//

#ifndef ENJIMA_MEMORY_MANAGER_H
#define ENJIMA_MEMORY_MANAGER_H

#include "MemoryAllocationCoordinator.h"
#include "MemoryAllocator.h"
#include "MemoryBlock.h"
#include "MemoryChunk.h"
#include "MemoryTypeAliases.h"
#include "enjima/common/TypeAliases.h"
#include "enjima/core/CoreInternals.fwd.h"
#include "enjima/operators/OperatorsTypeAliases.h"
#include "enjima/runtime/RuntimeInternals.fwd.h"
#include <unordered_set>
#include <vector>

using StdOpIDPair = std::pair<enjima::operators::OperatorID, enjima::operators::OperatorID>;

template<>
struct std::hash<StdOpIDPair> {
    std::size_t operator()(const StdOpIDPair& k) const
    {
        return (std::hash<enjima::operators::OperatorID>()(k.first) ^
                       (std::hash<enjima::operators::OperatorID>()(k.second) << 1)) >>
               1;
    }
};

namespace enjima::memory {

    class MemoryManager {
    public:
        enum class AllocatorType { kBasic, kAdaptive };

        MemoryManager(size_t maxMemory, size_t numBlocksPerChunk, int32_t defaultNumRecsPerBlock,
                AllocatorType allocatorType);
        virtual ~MemoryManager();
        MemoryManager(const MemoryManager&) = delete;
        MemoryManager& operator=(const MemoryManager&) = delete;

        void InitForTesting(uint32_t initAllocatedChunks);
        void Init(bool preAllocationEnabled = true);
        void StartMemoryPreAllocator();
        void ReleaseAllResources();
        void InitializePipeline(core::StreamingPipeline* pipelinePtr);
        void PreAllocatePipeline(const core::StreamingPipeline* pipelinePtr);
        bool PreAllocateMemoryChunk(operators::ChannelID channelId);
        bool ReleaseChunk(ChunkID chunkId);

        const std::vector<operators::ChannelID>& GetUpstreamChannelIds(operators::OperatorID opId) const;
        const std::vector<operators::ChannelID>& GetDownstreamChannelIds(operators::OperatorID opId) const;
        operators::ChannelID GetChannelID(operators::OperatorID upstreamOpId,
                operators::OperatorID downstreamOpId) const;

        template<typename T>
        MemoryBlock* RequestEmptyBlock(operators::ChannelID currentChannelId);
        MemoryBlock* RequestReadableBlock(operators::ChannelID channelId);
        void ReturnBlock(MemoryBlock* pBlock);

        void SetMaxActiveChunksPerOperator(uint64_t maxActiveChunksPerOperator);
        [[nodiscard]] size_t GetMaxMemory() const;
        [[nodiscard]] size_t GetDefaultNumEventsPerBlock() const;
        [[nodiscard]] size_t GetCurrentlyAllocatedMemory() const;
        [[nodiscard]] MemoryBlock* GetReservedMemoryBlock() const;
        bool IsPreAllocationEnabled() const;

        static const operators::OperatorID kReservedOperatorId;
        static const operators::OperatorID kReservedChannelId;
        static const size_t kMinMemory;
        static const size_t kMinBlocksPerChunk;
        static const uint8_t kNumInitialChunksPerOperator;

    private:
        static uint64_t DeriveBlockId(size_t blockSize, const char* pChunkData, const char* pBlk);
        static void PopulateBlocksForChunk(unsigned long channelId, size_t reqBlkDataSize,
                MemoryChunk* pCandidateChunk);
        MemoryChunk* GetNextFreeChunk(operators::ChannelID channelId);
        MemoryChunk* AllocateChunk(operators::OperatorID operatorId, size_t chunkSize, size_t recordSize);
        MemoryChunk* AllocateChunkMT(operators::OperatorID operatorId, size_t chunkSize, size_t recordSize);
        [[nodiscard]] size_t GetBlockDataSize(operators::OperatorID operatorId) const;
        [[nodiscard]] size_t GetBlockSize(operators::OperatorID operatorId) const;
        [[nodiscard]] size_t GetChunkSize(operators::OperatorID operatorId) const;
        [[nodiscard]] size_t GetReservedChunkSize() const;
        MemoryChunk* TryInitializeAndGetChunk(unsigned long channelId);
        MemoryChunk* TryInitializeAndGetChunkMT(unsigned long channelId);
        MemoryChunk* InitializeAndGetChunk(unsigned long channelId, size_t chunkSize);
        MemoryChunk* InitializeAndGetChunkMT(unsigned long channelId, size_t chunkSize);
        void PreAllocatePipelineMT(const core::StreamingPipeline* pipelinePtr);
        void InitReservedMemoryBlock(const MemoryChunk* pReservedMemoryChunk);
        operators::ChannelID GetNextChannelId();

        size_t maxMemory_;
        size_t numBlocksPerChunk_;
        int32_t defaultNumRecsPerBlock_;
        std::atomic<size_t> currentlyAllocatedMemory_{0};
        AllocatorType allocatorType_;
        MemoryAllocator* allocator_;
        MemoryAllocationCoordinator* pAllocCoordinator_;
        MemoryBlock* reservedMemoryBlock_{nullptr};
        std::atomic<bool> releasedResources_{false};
        uint64_t maxActiveChunksPerOperator_{100};
        uint64_t lastMemoryWarningAt_{0};
        bool initialized_{false};
        bool preAllocationEnabled_{true};
        operators::ChannelID nextChannelId_{1};
        std::shared_mutex sharedMapsMutex_;

        // We need for chunkMapsByChannelId_ to be a concurrent hash map since it is by multiple threads in direct alloc
        ConcurrentUnorderedMapTBB<operators::ChannelID, ConcurrentUnorderedMapTBB<ChunkID, MemoryChunk*>>
                chunkMapsByChannelId_;
        ConcurrentUnorderedMapTBB<operators::ChannelID, std::atomic<uint64_t>> activeChunkCountByChannelId_;
        ConcurrentUnorderedMapTBB<operators::ChannelID, ConcurrentBoundedQueueTBB<MemoryBlock*>>
                readableFirstBlockOfChunkByChannelId_;
        ConcurrentHashMapTBB<memory::ChunkID, ConcurrentUnorderedSetTBB<MemoryBlock*>*> reclaimableBlocksByChunkId_;
        UnorderedHashMapST<operators::OperatorID, size_t> outputRecordSizeByOpId_;
        UnorderedHashMapST<operators::OperatorID, int32_t> numEventsPerBlocksByOpId_;
        UnorderedHashMapST<operators::ChannelID, std::pair<operators::OperatorID, operators::OperatorID>>
                channelIdMapping_;
        UnorderedHashMapST<std::pair<operators::OperatorID, operators::OperatorID>, operators::ChannelID>
                reverseChannelIdMapping_;
        UnorderedHashMapST<operators::ChannelID, operators::OperatorID> opIdsByChannelId_;
        UnorderedHashMapST<operators::OperatorID, std::vector<operators::ChannelID>> downstreamChannelIdsByOpId_;
        UnorderedHashMapST<operators::OperatorID, std::vector<operators::ChannelID>> upstreamChannelIdsByOpId_;
    };

}// namespace enjima::memory

#include "MemoryManager.tpp"

#endif// ENJIMA_MEMORY_MANAGER_H
