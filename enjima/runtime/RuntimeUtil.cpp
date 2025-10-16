//
// Created by m34ferna on 28/02/24.
//

#include "RuntimeUtil.h"
#include <sys/resource.h>

namespace enjima::runtime {
    /**
     * Get the current system clock time as the number of milliseconds since epoch using the std::chrono::high_resolution_clock
     * @return the current time as the number of milliseconds since epoch
     */
    uint64_t GetSystemTimeMillis()
    {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::high_resolution_clock::now().time_since_epoch())
                .count();
    }

    uint64_t GetSystemTimeMicros()
    {
        return std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::high_resolution_clock::now().time_since_epoch())
                .count();
    }

    std::chrono::high_resolution_clock::time_point GetSystemTimePoint()
    {
        return std::chrono::high_resolution_clock::now();
    }

    uint64_t GetCurrentThreadCPUTimeMicros()
    {
        struct timespec currThreadCpuTime {};
        if (clock_gettime(CLOCK_THREAD_CPUTIME_ID, &currThreadCpuTime) == 0) {
            uint64_t timeInMicros = currThreadCpuTime.tv_sec * 1'000'000 + currThreadCpuTime.tv_nsec / 1'000;
            return timeInMicros;
        }
        return 0;
    }

    uint64_t GetCurrentProcessCPUTimeMicros()
    {
        struct timespec currProcessCpuTime {};
        if (clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &currProcessCpuTime) == 0) {
            uint64_t timeInMicros = currProcessCpuTime.tv_sec * 1'000'000 + currProcessCpuTime.tv_nsec / 1'000;
            return timeInMicros;
        }
        return 0;
    }

    uint64_t GetRUsageCurrentProcessCPUTimeMicros()
    {
        struct rusage rusageStruct {};
        if (getrusage(RUSAGE_SELF, &rusageStruct) == 0) {
            uint64_t uTimeInMicros = rusageStruct.ru_utime.tv_sec * 1'000'000 + rusageStruct.ru_utime.tv_usec;
            uint64_t sTimeInMicros = rusageStruct.ru_stime.tv_sec * 1'000'000 + rusageStruct.ru_stime.tv_usec;
            return uTimeInMicros + sTimeInMicros;
        }
        return 0;
    }

    uint64_t GetRUsageCurrentThreadCPUTimeMicros()
    {
        struct rusage rusageStruct {};
        if (getrusage(RUSAGE_THREAD, &rusageStruct) == 0) {
            uint64_t uTimeInMicros = rusageStruct.ru_utime.tv_sec * 1'000'000 + rusageStruct.ru_utime.tv_usec;
            uint64_t sTimeInMicros = rusageStruct.ru_stime.tv_sec * 1'000'000 + rusageStruct.ru_stime.tv_usec;
            return uTimeInMicros + sTimeInMicros;
        }
        return 0;
    }

}// namespace enjima::runtime
