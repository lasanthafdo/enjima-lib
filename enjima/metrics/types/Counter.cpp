//
// Created by m34ferna on 09/01/24.
//

#include "Counter.h"

namespace enjima::metrics {
    uint64_t Counter<uint64_t>::GetCount() const
    {
        return count_.load(std::memory_order::acquire);
    }

    void Counter<uint64_t>::Inc()
    {
        count_.fetch_add(1, std::memory_order::acq_rel);
    }

    void Counter<uint64_t>::Inc(uint64_t delta)
    {
        count_.fetch_add(delta, std::memory_order::acq_rel);
    }

    void Counter<uint64_t>::IncRelaxed()
    {
        count_.fetch_add(1, std::memory_order::relaxed);
    }

    void Counter<uint64_t>::IncRelaxed(uint64_t delta)
    {
        count_.fetch_add(delta, std::memory_order::relaxed);
    }
}// namespace enjima::metrics
