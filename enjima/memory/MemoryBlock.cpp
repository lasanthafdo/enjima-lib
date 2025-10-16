//
// Created by m34ferna on 05/01/24.
//

#include "MemoryBlock.h"
#include <cassert>
#include <ostream>


namespace enjima::memory {

    MemoryBlock::MemoryBlock(uint64_t blockId, MemoryChunk* pChunk, operators::ChannelID channelId, uint32_t capacity,
            void* pBlk)
        : blockId_(blockId), channelId_(channelId), pChunk_(pChunk),
          pData_(static_cast<char*>(pBlk) + sizeof(MemoryBlock)), pEnd_(static_cast<char*>(pData_) + capacity),
          capacity_(capacity), pWrite_(pData_), pRead_(pData_)
    {
    }

    bool MemoryBlock::CanRead() const
    {
        return pRead_ < pWrite_;
    }

    bool MemoryBlock::CanWrite() const
    {
        return pWrite_ < pEnd_;
    }

    bool MemoryBlock::IsReadComplete() const
    {
        return pRead_ == pEnd_;
    }

    operators::OperatorID MemoryBlock::GetChannelId() const
    {
        return channelId_;
    }

    void* MemoryBlock::GetEndPtr() const
    {
        return pEnd_;
    }

    uint32_t MemoryBlock::GetTotalCapacity() const
    {
        return capacity_;
    }

    uint32_t MemoryBlock::GetCurrentSize() const
    {
        return static_cast<char*>(pWrite_.load(std::memory_order_acquire)) - static_cast<const char*>(pData_);
    }

    bool MemoryBlock::IsReadOnly() const
    {
        return CanRead() && !CanWrite();
    }

    void* MemoryBlock::GetReadPtr() const
    {
        return pRead_;
    }

    void* MemoryBlock::GetWritePtr() const
    {
        return pWrite_;
    }

    uint64_t MemoryBlock::GetBlockId() const
    {
        return blockId_;
    }

    uint64_t MemoryBlock::GetChunkId() const
    {
        // pChunk_ is initially set and never changed. So relaxed ordering should be OK
        return pChunk_.load(std::memory_order::relaxed)->GetChunkId();
    }

    void* MemoryBlock::GetDataPtr() const
    {
        return pData_;
    }

    void MemoryBlock::ResetForWriting()
    {
        pWrite_ = pData_;
        pRead_ = pData_;
        writeCompleted_.store(false, std::memory_order::release);
        blockReturned_.store(false, std::memory_order::release);
    }

    bool MemoryBlock::IsValid() const
    {
        bool valid = ((char*) this == static_cast<const char*>(pData_) - sizeof(MemoryBlock));
        valid = valid && ((char*) pEnd_ == static_cast<const char*>(pData_) + capacity_);
        valid = valid && (pRead_ >= pData_) && (pRead_ <= pEnd_);
        valid = valid && (pWrite_ >= pData_) && (pWrite_ <= pEnd_);
        valid = valid && (pWrite_ >= pRead_);
        return valid;
    }

    MemoryChunk* MemoryBlock::GetChunkPtr() const
    {
        // pChunk_ is initially set and never changed. So relaxed ordering should be OK
        return pChunk_.load(std::memory_order::relaxed);
    }

    bool MemoryBlock::IsWriteActive() const
    {
        return writeActive_.load(std::memory_order::acquire);
    }

    bool MemoryBlock::SetWriteActive()
    {
        if (!readActive_.load(std::memory_order::acquire)) {
            bool expected = false;
            return writeActive_.compare_exchange_strong(expected, true, std::memory_order::acq_rel);
        }
        return false;
    }

    bool MemoryBlock::IsReadActive() const
    {
        return readActive_.load(std::memory_order::acquire);
    }

    bool MemoryBlock::SetReadActive()
    {
        if (writeActive_.load(std::memory_order::acquire)) {
            bool expected = false;
            return readActive_.compare_exchange_strong(expected, true, std::memory_order::acq_rel);
        }
        return false;
    }

    void MemoryBlock::SetReadWriteInactive()
    {
        assert(readActive_.load(std::memory_order::acquire));
        assert(writeActive_.load(std::memory_order::acquire));
        ResetForWriting();
        readActive_.store(false, std::memory_order::release);
        writeActive_.store(false, std::memory_order::release);
    }

    bool MemoryBlock::IsWriteCompleted() const
    {
        return writeCompleted_.load(std::memory_order::acquire);
    }

    void MemoryBlock::SetWriteCompleted()
    {
        writeCompleted_.store(true, std::memory_order::release);
    }

    bool MemoryBlock::IsBlockReturned() const
    {
        return blockReturned_.load(std::memory_order::acquire);
    }

    void MemoryBlock::SetBlockReturned()
    {
        blockReturned_.store(true, std::memory_order::release);
    }

    std::ostream& operator<<(std::ostream& os, const MemoryBlock& block)
    {
        os << "MemoryBlock {\n\tblockId : " << block.GetBlockId() << ", chunkId: " << block.GetChunkId()
           << ", channelId : " << block.GetChannelId() << "\n\tblockPtr : " << &block
           << ", endPtr : " << block.GetEndPtr() << "\n\tdataPtr : " << block.GetDataPtr()
           << ", readPtr : " << block.GetReadPtr() << ", writePtr: " << block.GetWritePtr()
           << "\n\tcurrentSize : " << block.GetCurrentSize() << ", capacity: " << block.GetTotalCapacity() << "\n}";
        return os;
    }
}// namespace enjima::memory
