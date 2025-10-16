//
// Created by m34ferna on 17/01/24.
//

#ifndef ENJIMA_MEMORY_UTIL_H
#define ENJIMA_MEMORY_UTIL_H

#include <cstddef>
#include <type_traits>

#if ENJIMA_MEM_CPY_METHOD >= 1
#include <cstdint>
#include <immintrin.h>
#endif

namespace enjima::memory {
    constexpr size_t KiloBytes(const size_t val)
    {
        return val << 10;
    }

    constexpr size_t MegaBytes(const size_t val)
    {
        return val << 20;
    }

    constexpr size_t GigaBytes(const size_t val)
    {
        return val << 30;
    }

    template<typename T>
        requires std::is_integral_v<T>
    constexpr T ToKiloBytes(const T bytes)
    {
        return bytes >> 10;
    }

#if ENJIMA_MEM_CPY_METHOD == 1
    void memcpy_avx_unaligned(void* dst, const void* src, size_t n);
#elif ENJIMA_MEM_CPY_METHOD == 2
    void memcpy_avx_nt(void* dst, const void* src, size_t n);
#elif ENJIMA_MEM_CPY_METHOD == 3
    void memcpy_sse2(void* dst, const void* src, size_t n);
#elif ENJIMA_MEM_CPY_METHOD == 4
    void memcpy_avx_unroll(void* dst, const void* src, size_t n);
#endif
}// namespace enjima::memory

#endif//ENJIMA_MEMORY_UTIL_H
