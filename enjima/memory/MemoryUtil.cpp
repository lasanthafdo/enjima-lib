//
// Created by m34ferna on 10/05/25.
//


#include "MemoryUtil.h"

#include <cassert>
#include <cstring>

namespace enjima::memory {
#if ENJIMA_MEM_CPY_METHOD >= 1

#if ENJIMA_MEM_CPY_METHOD == 3
#define VEC_SIZE_16 16
#elif ENJIMA_MEM_CPY_METHOD == 4
#define VEC_SIZE_32 32
#define LOOP_SIZE_128 128
#else
#define VEC_SIZE_32 32
#endif


    // Based on https://stackoverflow.com/questions/42093360/how-to-check-if-a-pointer-points-to-a-properly-aligned-memory-location
    inline bool is_aligned(const void* ptr, std::uintptr_t alignment) noexcept
    {
        auto iptr = reinterpret_cast<std::uintptr_t>(ptr);
        return !(iptr % alignment);
    }

    // Based on https://stackoverflow.com/questions/38381233/find-next-aligned-memory-address
    inline size_t get_aligned_offset(size_t alignment, void* ptr) noexcept
    {
        const auto uintptr = reinterpret_cast<std::uintptr_t>(ptr);
        const auto aligned = (uintptr - 1u + alignment) & -alignment;
        return aligned - uintptr;
    }

#if ENJIMA_MEM_CPY_METHOD == 1
    void memcpy_avx_unaligned(void* dst, const void* src, size_t n)
    {
        auto n_avx = (n - 1u + 32) & -32;
        auto n_tail = n % 32;
        auto src_cast = static_cast<const size_t*>(src);
        auto dst_cast = static_cast<size_t*>(dst);
        assert(n_avx % 32 == 0);
        auto n_iter = n_avx / sizeof(size_t);
        for (size_t i = 0; i < n_iter; i += 4) {
            __m256i val = _mm256_lddqu_si256(reinterpret_cast<const __m256i*>(src_cast + i));
            _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst_cast + i), val);
        }
        // _mm_sfence();
        void* src_tail = static_cast<char*>(const_cast<void*>(src)) + n_avx;
        void* dst_tail = static_cast<char*>(const_cast<void*>(dst)) + n_avx;
        std::memcpy(dst_tail, src_tail, n_tail);
    }

#elif ENJIMA_MEM_CPY_METHOD == 2
    // Based on https://github.com/ibogosavljevic/johnysswlab/blame/master/2023-01-save-memory-bandwidth/memcpy.h
    void _memcpy_avx_nt(void* dst, const void* src, size_t n)
    {
        assert(is_aligned(dst, VEC_SIZE_32));
        const auto* src_vec = static_cast<const __m256i*>(src);
        auto* dst_vec = static_cast<__m256i*>(dst);
        assert(n % VEC_SIZE_32 == 0);
        auto n_iter = n / sizeof(__m256i);
        for (size_t i = 0; i < n_iter; i++) {
            _mm256_stream_si256(dst_vec + i, _mm256_lddqu_si256(src_vec + i));
        }
        _mm_sfence();
    }

    void memcpy_avx_nt(void* dst, const void* src, size_t n)
    {
        if (n > 0) {
            if (is_aligned(dst, VEC_SIZE_32)) {
                if (n > VEC_SIZE_32) {
                    auto n_aligned = n & -VEC_SIZE_32;
                    assert(n_aligned <= n);
                    _memcpy_avx_nt(dst, src, n_aligned);
                    src = static_cast<char*>(const_cast<void*>(src)) + n_aligned;
                    dst = static_cast<char*>(const_cast<void*>(dst)) + n_aligned;
                    n = n % VEC_SIZE_32;
                }
                std::memcpy(dst, src, n);
            }
            else {
                size_t offset = get_aligned_offset(VEC_SIZE_32, dst);
                std::memcpy(dst, src, offset);
                if (n > offset) {
                    n -= offset;
                    src = static_cast<char*>(const_cast<void*>(src)) + offset;
                    dst = static_cast<char*>(const_cast<void*>(dst)) + offset;
                    if (n > VEC_SIZE_32) {
                        auto n_aligned = n & -VEC_SIZE_32;
                        assert(n_aligned <= n);
                        _memcpy_avx_nt(dst, src, n_aligned);
                        src = static_cast<char*>(const_cast<void*>(src)) + n_aligned;
                        dst = static_cast<char*>(const_cast<void*>(dst)) + n_aligned;
                        n = n % VEC_SIZE_32;
                    }
                    std::memcpy(dst, src, n);
                }
            }
        }
    }

#elif ENJIMA_MEM_CPY_METHOD == 3
    // Based on https://github.com/ibogosavljevic/johnysswlab/blame/master/2023-01-save-memory-bandwidth/memcpy.h
    void memcpy_sse2_nt(void* dst, const void* src, size_t n)
    {
        assert(is_aligned(dst, VEC_SIZE_16));
        auto src_cast = static_cast<const size_t*>(src);
        auto dst_cast = static_cast<size_t*>(dst);
        assert(n % VEC_SIZE_16 == 0);
        auto n_iter = n / sizeof(size_t);
        for (size_t i = 0; i < n_iter; i += 2) {
            const auto* load_addr = reinterpret_cast<const __m128i*>(src_cast + i);
            __m128i val = _mm_lddqu_si128(load_addr);
            auto* store_addr = reinterpret_cast<__m128i*>(dst_cast + i);
            _mm_stream_si128(store_addr, val);
        }
        _mm_sfence();
    }

    void memcpy_sse2(void* dst, const void* src, size_t n)
    {
        if (is_aligned(dst, VEC_SIZE_16)) {
            auto n_aligned = (n - 1u + VEC_SIZE_16) & -VEC_SIZE_16;
            auto n_tail = n % VEC_SIZE_16;
            enjima::memory::memcpy_sse2_nt(dst, src, n_aligned);
            void* src_tail = static_cast<char*>(const_cast<void*>(src)) + n_aligned;
            void* dst_tail = static_cast<char*>(const_cast<void*>(dst)) + n_aligned;
            std::memcpy(dst_tail, src_tail, n_tail);
        }
        else {
            size_t offset = 0;
            void* aligned_dst = get_aligned(VEC_SIZE_16, dst, offset);
            auto n_remaining = n - offset;
            auto n_aligned = (n_remaining - 1u + VEC_SIZE_16) & -VEC_SIZE_16;
            auto n_tail = n_remaining % VEC_SIZE_16;
            void* src_with_offset = static_cast<char*>(const_cast<void*>(src)) + offset;
            void* src_tail = static_cast<char*>(const_cast<void*>(src)) + offset + n_aligned;
            void* dst_tail = static_cast<char*>(const_cast<void*>(dst)) + offset + n_aligned;
            std::memcpy(dst, src, offset);
            enjima::memory::memcpy_sse2_nt(aligned_dst, src_with_offset, n_aligned);
            std::memcpy(dst_tail, src_tail, n_tail);
        }
    }
#elif ENJIMA_MEM_CPY_METHOD == 4

    // Based on https://squadrick.dev/journal/going-faster-than-memcpy.html
    void _memcpy_aligned_avx_unroll(void* dst, const void* src, size_t n)
    {
        assert(is_aligned(dst, LOOP_SIZE_128));
        const auto* src_vec = static_cast<const __m256i*>(src);
        auto* dst_vec = static_cast<__m256i*>(dst);
        assert(n % VEC_SIZE_32 == 0);
        auto n_iter = n / sizeof(__m256i);
        for (size_t i = 0; i < n_iter; i += 4) {
            _mm256_store_si256(dst_vec + i, _mm256_load_si256(src_vec + i));
            _mm256_store_si256(dst_vec + i + 1, _mm256_load_si256(src_vec + i + 1));
            _mm256_store_si256(dst_vec + i + 2, _mm256_load_si256(src_vec + i + 2));
            _mm256_store_si256(dst_vec + i + 3, _mm256_load_si256(src_vec + i + 3));
        }
    }

    void _memcpy_avx_unroll(void* dst, const void* src, size_t n)
    {
        assert(is_aligned(dst, LOOP_SIZE_128));
        const auto* src_vec = static_cast<const __m256i*>(src);
        auto* dst_vec = static_cast<__m256i*>(dst);
        assert(n % VEC_SIZE_32 == 0);
        auto n_iter = n / sizeof(__m256i);
        for (size_t i = 0; i < n_iter; i += 4) {
            _mm256_store_si256(dst_vec + i, _mm256_lddqu_si256(src_vec + i));
            _mm256_store_si256(dst_vec + i + 1, _mm256_lddqu_si256(src_vec + i + 1));
            _mm256_store_si256(dst_vec + i + 2, _mm256_lddqu_si256(src_vec + i + 2));
            _mm256_store_si256(dst_vec + i + 3, _mm256_lddqu_si256(src_vec + i + 3));
        }
    }

    void memcpy_avx_unroll(void* dst, const void* src, size_t n)
    {
        if (is_aligned(dst, LOOP_SIZE_128)) {
            if (n > LOOP_SIZE_128) {
                auto n_aligned = n & -LOOP_SIZE_128;
                assert(n_aligned <= n);
                if (is_aligned(src, LOOP_SIZE_128)) {
                    _memcpy_aligned_avx_unroll(dst, src, n_aligned);
                }
                else {
                    _memcpy_avx_unroll(dst, src, n_aligned);
                }
                src = static_cast<char*>(const_cast<void*>(src)) + n_aligned;
                dst = static_cast<char*>(const_cast<void*>(dst)) + n_aligned;
                n = n % LOOP_SIZE_128;
            }
        }
        else {
            size_t offset = get_aligned_offset(LOOP_SIZE_128, dst);
            if (n >= offset + LOOP_SIZE_128) {
                std::memcpy(dst, src, offset);
                n -= offset;
                src = static_cast<char*>(const_cast<void*>(src)) + offset;
                dst = static_cast<char*>(const_cast<void*>(dst)) + offset;
                auto n_aligned = n & -LOOP_SIZE_128;
                assert(n_aligned <= n);
                _memcpy_avx_unroll(dst, src, n_aligned);
                src = static_cast<char*>(const_cast<void*>(src)) + n_aligned;
                dst = static_cast<char*>(const_cast<void*>(dst)) + n_aligned;
                n = n % LOOP_SIZE_128;
            }
        }
        std::memcpy(dst, src, n);
    }

#endif

#endif
}// namespace enjima::memory
