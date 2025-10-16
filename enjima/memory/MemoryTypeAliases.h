//
// Created by m34ferna on 02/02/24.
//

#ifndef ENJIMA_MEMORY_TYPE_ALIASES_H
#define ENJIMA_MEMORY_TYPE_ALIASES_H

#include <cstdint>
#include <new>

#ifdef __cpp_lib_hardware_interference_size
using std::hardware_constructive_interference_size;
using std::hardware_destructive_interference_size;
#else
// 64 bytes on x86-64 │ L1_CACHE_BYTES │ L1_CACHE_SHIFT │ __cacheline_aligned │ ...
constexpr std::size_t hardware_constructive_interference_size = 64;
constexpr std::size_t hardware_destructive_interference_size = 64;
#endif

namespace enjima::memory {
    using ChunkID = uint64_t;
}

#endif//ENJIMA_MEMORY_TYPE_ALIASES_H
