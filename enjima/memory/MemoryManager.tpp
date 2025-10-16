#ifndef ENJIMA_MEMORY_MANAGER_TPP
#define ENJIMA_MEMORY_MANAGER_TPP

namespace enjima::memory {

    template<typename T>
    MemoryBlock* MemoryManager::RequestEmptyBlock(operators::ChannelID currentChannelId)
    {
        auto& opActiveChunkCounter = activeChunkCountByChannelId_.at(currentChannelId);
        if (opActiveChunkCounter.load(std::memory_order::acquire) < maxActiveChunksPerOperator_) {
            MemoryBlock* pMemBlock = nullptr;
            MemoryChunk* pCurrentChunk = GetNextFreeChunk(currentChannelId);
            if (preAllocationEnabled_) {
                if (pCurrentChunk == nullptr) {
                    // There are no more free chunks to allocate
                    return nullptr;
                }
                pCurrentChunk->SetInUse(true);
                auto numActiveChunks = opActiveChunkCounter.fetch_add(1, std::memory_order::acq_rel) + 1;
                if (numActiveChunks == chunkMapsByChannelId_[currentChannelId].size()) {
                    pAllocCoordinator_->EnqueueChunkAllocationRequest(currentChannelId);
                }
            }
            else {
                if (pCurrentChunk == nullptr) {
                    // There are no more free chunks to allocate
                    pCurrentChunk = TryInitializeAndGetChunkMT(currentChannelId);
                    if (pCurrentChunk == nullptr) {
                        return nullptr;
                    }
                }
                pCurrentChunk->SetInUse(true);
                opActiveChunkCounter.fetch_add(1, std::memory_order::acq_rel);
            }
            pMemBlock = static_cast<MemoryBlock*>(pCurrentChunk->GetDataPtr());
            pMemBlock->SetWriteActive();
            readableFirstBlockOfChunkByChannelId_.at(currentChannelId).push(pMemBlock);
            return pMemBlock;
        }
        return nullptr;
    }
}// namespace enjima::memory

#endif