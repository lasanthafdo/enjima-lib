//
// Created by m34ferna on 11/01/24.
//

#ifndef ENJIMA_BASIC_MEMORY_ALLOCATOR_H
#define ENJIMA_BASIC_MEMORY_ALLOCATOR_H

#include "MemoryAllocator.h"
#include "MemoryChunk.h"
#include <unordered_map>

namespace enjima::memory {

    class BasicMemoryAllocator : public MemoryAllocator {
    public:
        ~BasicMemoryAllocator() noexcept override;
        MemoryChunk* Allocate(size_t chunkSize, size_t blockSize) override;
        void Deallocate(MemoryChunk* pChunk) override;

    private:
        std::atomic<uint64_t> totalAllocatedChunks_{0};
    };

}// namespace enjima::memory


#endif//ENJIMA_BASIC_MEMORY_ALLOCATOR_H
