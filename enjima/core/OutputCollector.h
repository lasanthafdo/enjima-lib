//
// Created by m34ferna on 02/02/24.
//

#ifndef ENJIMA_OUTPUT_COLLECTOR_H
#define ENJIMA_OUTPUT_COLLECTOR_H

#include "enjima/memory/MemoryBlock.h"
#include "enjima/metrics/types/Counter.h"
#include "enjima/runtime/RuntimeUtil.h"

namespace enjima::core {

    class OutputCollector {
    public:
        explicit OutputCollector(memory::MemoryBlock* outputBlk, metrics::Counter<uint64_t>* outCounter);

        template<class T, class... Args>
        void Collect(Args&&... args);

        template<class T, class... Args>
        void CollectWithTimestamp(uint64_t timestamp, Args&&... args);

        template<class T>
        void Collect(core::Record<T> outputRecord);

        template<class T>
        void CollectBatch(void* dataBuffer, uint32_t numEvents);

        void SetOutputBlock(memory::MemoryBlock* outputBlk);

    private:
        memory::MemoryBlock* outputBlk_;
        metrics::Counter<uint64_t>* outCounter_;
    };

}// namespace enjima::core

#include "OutputCollector.tpp"

#endif//ENJIMA_OUTPUT_COLLECTOR_H
