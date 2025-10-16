//
// Created by m34ferna on 15/02/24.
//

#ifndef ENJIMA_OPERATOR_UTIL_H
#define ENJIMA_OPERATOR_UTIL_H

#include "enjima/core/OutputCollector.h"
#include "enjima/memory/MemoryBlock.h"
#include "enjima/memory/MemoryManager.h"

namespace enjima::operators {

    template<typename TOutput>
    static memory::MemoryBlock* GetNextOutputBlock(operators::ChannelID channelId,
            memory::MemoryBlock* currentOutputBlockPtr, memory::MemoryManager* pMemoryManager)
    {
        auto reservedBlock = pMemoryManager->GetReservedMemoryBlock();
        if (currentOutputBlockPtr != reservedBlock) {
            currentOutputBlockPtr->SetWriteCompleted();
            auto pChunkEnd = currentOutputBlockPtr->GetChunkPtr()->GetEndPtr();
            auto nextOutputBlockPtr = currentOutputBlockPtr;
            // As long as nextOutputBlockPtr is not the last in the chunk, iterate via pointers
            while (nextOutputBlockPtr->GetEndPtr() < pChunkEnd) {
                nextOutputBlockPtr = static_cast<memory::MemoryBlock*>(nextOutputBlockPtr->GetEndPtr());
                if (nextOutputBlockPtr->SetWriteActive()) {
                    return nextOutputBlockPtr;
                }
            }
        }
        auto nextOutputChunkFirstBlockPtr = pMemoryManager->RequestEmptyBlock<TOutput>(channelId);
        if (nextOutputChunkFirstBlockPtr == nullptr) {
            return pMemoryManager->GetReservedMemoryBlock();
        }
        else {
            assert(nextOutputChunkFirstBlockPtr->IsValid());
            return nextOutputChunkFirstBlockPtr;
        }
    }

    static memory::MemoryBlock* GetNextInputBlock(operators::ChannelID channelId,
            memory::MemoryBlock* currentInputBlockPtr, memory::MemoryManager* pMemoryManager)
    {
        auto inputBlockPtr = currentInputBlockPtr;
        auto pChunkEnd = inputBlockPtr->GetChunkPtr()->GetEndPtr();
        while (inputBlockPtr->GetEndPtr() < pChunkEnd) {
            auto prevInputBlockPtr = inputBlockPtr;
            inputBlockPtr = static_cast<memory::MemoryBlock*>(inputBlockPtr->GetEndPtr());
            if (inputBlockPtr->IsWriteActive()) {
                if (inputBlockPtr->SetReadActive()) {
                    assert(!inputBlockPtr->IsBlockReturned());
                    return inputBlockPtr;
                }
                else {
                    assert(false);
                }
            }
            else {
                assert(prevInputBlockPtr->IsReadActive());
                return prevInputBlockPtr;
            }
        }
        inputBlockPtr = pMemoryManager->RequestReadableBlock(channelId);
        if (inputBlockPtr != nullptr) {
            assert(inputBlockPtr->IsValid());
            return inputBlockPtr;
        }
        else {
            return pMemoryManager->GetReservedMemoryBlock();
        }
    }

}// namespace enjima::operators

#endif//ENJIMA_OPERATOR_UTIL_H
