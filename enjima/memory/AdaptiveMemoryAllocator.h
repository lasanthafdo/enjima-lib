//
// Created by m34ferna on 10/01/24.
//

#ifndef ENJIMA_ADAPTIVE_MEMORY_ALLOCATOR_H
#define ENJIMA_ADAPTIVE_MEMORY_ALLOCATOR_H

#include "MemoryAllocator.h"
#include "MemoryBlock.h"
#include <unordered_map>

namespace enjima::memory {

    class AdaptiveMemoryAllocator : public MemoryAllocator {
    public:
        MemoryChunk* Allocate(size_t chunkSize, size_t blockSize) override;
        void Deallocate(MemoryChunk* pChunk) override;
        ~AdaptiveMemoryAllocator() noexcept override;

    private:
        std::atomic<uint64_t> totalAllocatedChunks_{0};
    };

}// namespace enjima::memory


#endif//ENJIMA_ADAPTIVE_MEMORY_ALLOCATOR_H
