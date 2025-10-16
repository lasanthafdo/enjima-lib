//
// Created by m34ferna on 08/01/24.
//

#ifndef ENJIMA_MEMORY_ALLOCATOR_H
#define ENJIMA_MEMORY_ALLOCATOR_H

#include "MemoryBlock.h"
#include "MemoryChunk.h"
#include <cstddef>

namespace enjima::memory {

    class MemoryAllocator {
    public:
        virtual MemoryChunk* Allocate(size_t chunkSize, size_t blockSize) = 0;
        virtual void Deallocate(MemoryChunk* chunk) = 0;
        virtual ~MemoryAllocator() noexcept = default;
    };

}// namespace enjima::memory

#endif// ENJIMA_MEMORY_ALLOCATOR_H
