//
// Created by m34ferna on 13/01/24.
//

#ifndef ENJIMA_MEMORY_CHUNK_H
#define ENJIMA_MEMORY_CHUNK_H

#include "MemoryTypeAliases.h"

#include <atomic>
#include <cstddef>
#include <cstdint>

namespace enjima::memory {
    class alignas(hardware_constructive_interference_size) MemoryChunk {
    public:
        MemoryChunk(void* begin, ChunkID chunkId, size_t chunkCapacity, size_t blockSize, bool memoryMapped);
        [[nodiscard]] const void* GetChunkPtr() const;
        [[nodiscard]] void* GetDataPtr() const;
        [[nodiscard]] const void* GetEndPtr() const;
        [[nodiscard]] size_t GetCapacity() const;
        [[nodiscard]] ChunkID GetChunkId() const;
        [[nodiscard]] size_t GetBlockSize() const;
        [[nodiscard]] size_t GetNumBlocks() const;
        [[nodiscard]] bool IsMemoryMapped() const;
        [[nodiscard]] bool IsInUse() const;
        void SetInUse(bool inUse);
        void* ReserveBlock();

    private:
        const ChunkID chunkId_;
        const void* pChunk_;
        void* pData_;
        const void* pEnd_;
        std::atomic<void*> pCurrent_;
        const size_t capacity_;
        const size_t blockSize_;
        bool memoryMapped_{true};
        std::atomic<bool> inUse_{false};
    };

}// namespace enjima::memory


#endif//ENJIMA_MEMORY_CHUNK_H
