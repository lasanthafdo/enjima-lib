//
// Created by m34ferna on 11/01/24.
//

#include "BasicMemoryAllocator.h"
#include "BadMemoryOperation.h"

#include <sys/mman.h>

namespace enjima::memory {

    BasicMemoryAllocator::~BasicMemoryAllocator() noexcept = default;

    MemoryChunk* BasicMemoryAllocator::Allocate(size_t chunkSize, size_t blockSize)
    {
        bool memoryMapped = true;
        auto ptrBegin = mmap(NULL, chunkSize, PROT_READ | PROT_WRITE,
                MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE, -1, 0);
        // If mmap() failed, fallback to malloc
        if (ptrBegin == MAP_FAILED) {
            memoryMapped = false;
            ptrBegin = malloc(chunkSize);
        }
        // ptrBegin would be NULL iff both mmap() and malloc() failed
        if (!ptrBegin) {
            throw std::bad_alloc{};
        }
        // totalAllocatedChunks_ is currently not accessed from anywhere. So we do the FAA with relaxed memory ordering.
        auto memChunk = new (ptrBegin) MemoryChunk{ptrBegin,
                totalAllocatedChunks_.fetch_add(1, std::memory_order::relaxed) + 1, chunkSize, blockSize, memoryMapped};
        return memChunk;
    }

    void BasicMemoryAllocator::Deallocate(MemoryChunk* pChunk)
    {
        if (pChunk != nullptr) {
            void* addr = static_cast<void*>(pChunk);
            size_t len = pChunk->GetCapacity() + sizeof(MemoryChunk);
            bool memMapped = pChunk->IsMemoryMapped();
            pChunk->~MemoryChunk();
            if (memMapped) {
                if (munmap(static_cast<void*>(pChunk), len) != 0) {
                    // munmap() failed!
                    throw BadMemoryOperation(
                            std::string("Memory unmap (munmap) syscall failed when freeing memory chunk with id : " +
                                        std::to_string(pChunk->GetChunkId())));
                }
            }
            else {
                free(addr);
            }
        }
    }
}// namespace enjima::memory
