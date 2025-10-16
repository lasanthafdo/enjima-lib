//
// Created by m34ferna on 28/02/24.
//

namespace enjima::runtime {
    /**
     * Get the current system clock time as time since epoch for the corresponding resolution provided by Duration using the std::chrono::high_resolution_clock
     * @return the current time as the number of repetitions since epoch in the specified resolution (from template parameter)
     */
    template<typename Duration = std::chrono::milliseconds>
    uint64_t GetSystemTime()
    {
        return std::chrono::duration_cast<Duration>(std::chrono::high_resolution_clock::now().time_since_epoch())
                .count();
    }

    template<typename Duration = std::chrono::milliseconds>
    std::chrono::high_resolution_clock::time_point GetSystemTimePoint(uint64_t timeSinceEpoch)
    {
        return std::chrono::high_resolution_clock::time_point{Duration{timeSinceEpoch}};
    }

    template<typename Duration = std::chrono::milliseconds>
    std::chrono::steady_clock::time_point GetSteadyClockTimePoint(uint64_t timeSinceEpoch)
    {
        return std::chrono::steady_clock::time_point{Duration{timeSinceEpoch}};
    }

    uint64_t GetSteadyClockMicros()
    {
        return std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now().time_since_epoch())
                .count();
    }

    uint64_t GetSteadyClockMillis()
    {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now().time_since_epoch())
                .count();
    }

}// namespace enjima::runtime
