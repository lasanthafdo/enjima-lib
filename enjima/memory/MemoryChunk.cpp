//
// Created by m34ferna on 13/01/24.
//

#include "MemoryChunk.h"

namespace enjima::memory {
    MemoryChunk::MemoryChunk(void* begin, ChunkID chunkId, size_t chunkCapacity, size_t blockSize, bool memoryMapped)
        : chunkId_(chunkId), pChunk_(static_cast<void*>(begin)), capacity_(chunkCapacity - sizeof(MemoryChunk)),
          blockSize_(blockSize), memoryMapped_(memoryMapped)
    {
        pData_ = static_cast<char*>(begin) + sizeof(MemoryChunk);
        pCurrent_ = pData_;
        pEnd_ = static_cast<char*>(pData_) + capacity_;
    }

    const void* MemoryChunk::GetChunkPtr() const
    {
        return pChunk_;
    }

    size_t MemoryChunk::GetCapacity() const
    {
        return capacity_;
    }

    ChunkID MemoryChunk::GetChunkId() const
    {
        return chunkId_;
    }

    void* MemoryChunk::ReserveBlock()
    {
        void* currentPos = pCurrent_.load();
        void* nextPos = static_cast<char*>(currentPos) + blockSize_;
        while (nextPos <= pEnd_) {
            if (pCurrent_.compare_exchange_strong(currentPos, nextPos)) {
                return currentPos;
            }
            else {
                currentPos = pCurrent_.load();
                nextPos = static_cast<char*>(currentPos) + blockSize_;
            }
        }
        // If we reach here, we've failed to reserve memory from this chunk
        return nullptr;
    }

    bool MemoryChunk::IsInUse() const
    {
        return inUse_.load(std::memory_order::acquire);
    }

    bool MemoryChunk::IsMemoryMapped() const
    {
        return memoryMapped_;
    }

    void* MemoryChunk::GetDataPtr() const
    {
        return pData_;
    }

    void MemoryChunk::SetInUse(bool inUse)
    {
        inUse_.store(inUse, std::memory_order::release);
    }

    const void* MemoryChunk::GetEndPtr() const
    {
        return pEnd_;
    }

    size_t MemoryChunk::GetBlockSize() const
    {
        return blockSize_;
    }

    size_t MemoryChunk::GetNumBlocks() const
    {
        return capacity_ / blockSize_;
    }
}// namespace enjima::memory
