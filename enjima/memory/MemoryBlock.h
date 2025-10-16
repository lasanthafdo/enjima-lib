//
// Created by m34ferna on 05/01/24.
//

#ifndef ENJIMA_MEMORY_BLOCK_H
#define ENJIMA_MEMORY_BLOCK_H

#include "MemoryChunk.h"
#include "MemoryUtil.h"
#include "enjima/core/Record.h"
#include "enjima/operators/OperatorsTypeAliases.h"

#include <cassert>
#include <cstring>
#include <ostream>
#include <vector>

namespace enjima::memory {

    class alignas(hardware_constructive_interference_size) MemoryBlock {
    public:
        MemoryBlock(uint64_t blockId, MemoryChunk* pChunk, operators::ChannelID channelId, uint32_t capacity,
                void* pBlk);

        template<typename T>
        core::Record<T>* Read();
        template<typename T>
        uint32_t ReadBatch(void* inputBuffer, uint32_t numRecordsToRead);
        template<typename T>
        void Write(const core::Record<T>& record);
        template<typename T>
        void WriteBatch(void* outputBuffer, uint32_t numRecordsToWrite);
        template<class T, class... Args>
        void Emplace(Args&&... args);
        template<typename T>
        [[nodiscard]] uint32_t GetNumWritableEvents() const;
        template<typename T>
        [[nodiscard]] uint32_t GetNumReadableEvents() const;
        template<typename T>
        [[nodiscard]] uint32_t GetTotalEventCapacity() const;

        [[nodiscard]] bool CanRead() const;
        [[nodiscard]] bool CanWrite() const;
        [[nodiscard]] bool IsReadComplete() const;
        [[nodiscard]] bool IsReadOnly() const;
        [[nodiscard]] bool IsValid() const;

        [[nodiscard]] uint64_t GetBlockId() const;
        [[nodiscard]] uint64_t GetChunkId() const;
        [[nodiscard]] MemoryChunk* GetChunkPtr() const;
        [[nodiscard]] operators::ChannelID GetChannelId() const;

        [[nodiscard]] void* GetDataPtr() const;
        [[nodiscard]] void* GetEndPtr() const;
        [[nodiscard]] void* GetReadPtr() const;
        [[nodiscard]] void* GetWritePtr() const;
        [[nodiscard]] uint32_t GetTotalCapacity() const;
        [[nodiscard]] uint32_t GetCurrentSize() const;
        [[nodiscard]] bool IsWriteActive() const;
        bool SetWriteActive();
        [[nodiscard]] bool IsReadActive() const;
        bool SetReadActive();
        void SetReadWriteInactive();
        [[nodiscard]] bool IsWriteCompleted() const;
        void SetWriteCompleted();
        [[nodiscard]] bool IsBlockReturned() const;
        void SetBlockReturned();

    private:
        void ResetForWriting();

        const uint64_t blockId_;
        const operators::ChannelID channelId_;
        const std::atomic<MemoryChunk*> pChunk_;
        void* const pData_;
        void* const pEnd_;
        const uint32_t capacity_;
        std::atomic<bool> writeActive_{false};
        std::atomic<bool> writeCompleted_{false};
        std::atomic<bool> readActive_{false};
        std::atomic<bool> blockReturned_{false};
        std::atomic<void*> pWrite_;
        std::atomic<void*> pRead_;
    };

    std::ostream& operator<<(std::ostream& os, const MemoryBlock& block);
}// namespace enjima::memory

#include "MemoryBlock.tpp"

#endif// ENJIMA_MEMORY_BLOCK_H
