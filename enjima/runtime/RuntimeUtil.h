//
// Created by m34ferna on 06/02/24.
//

#ifndef ENJIMA_RUNTIME_UTIL_H
#define ENJIMA_RUNTIME_UTIL_H

#include <chrono>
#include <cstdint>

namespace enjima::runtime {
    uint64_t GetSystemTimeMillis();

    uint64_t GetSystemTimeMicros();

    inline uint64_t GetSteadyClockMicros();

    inline uint64_t GetSteadyClockMillis();

    std::chrono::high_resolution_clock::time_point GetSystemTimePoint();

    template<typename Duration>
    std::chrono::high_resolution_clock::time_point GetSystemTimePoint(uint64_t timeSinceEpoch);

    template<typename Duration>
    std::chrono::steady_clock::time_point GetSteadyClockTimePoint(uint64_t timeSinceEpoch);

    template<typename Duration>
    uint64_t GetSystemTime();

    uint64_t GetCurrentThreadCPUTimeMicros();

    uint64_t GetCurrentProcessCPUTimeMicros();

    uint64_t GetRUsageCurrentProcessCPUTimeMicros();

    uint64_t GetRUsageCurrentThreadCPUTimeMicros();

}// namespace enjima::runtime

#include "RuntimeUtil.tpp"

#endif//ENJIMA_RUNTIME_UTIL_H
