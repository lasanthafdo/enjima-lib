//
// Created by m34ferna on 28/02/24.
//

#include "OutputCollector.h"

namespace enjima::core {
    OutputCollector::OutputCollector(memory::MemoryBlock* outputBlk, metrics::Counter<uint64_t>* outCounter)
        : outputBlk_(outputBlk), outCounter_(outCounter)
    {
    }

    void OutputCollector::SetOutputBlock(memory::MemoryBlock* outputBlk)
    {
        outputBlk_ = outputBlk;
    }
}// namespace enjima::core