//
// Created by m34ferna on 26/03/24.
//

#ifndef ENJIMA_PERIODIC_METRICS_LOGGER_H
#define ENJIMA_PERIODIC_METRICS_LOGGER_H

#include "MetricsInternals.fwd.h"
#include "spdlog/sinks/basic_file_sink.h"
#include <atomic>
#include <cstdint>
#include <future>

namespace enjima::metrics {

    class PeriodicMetricsLogger {
    public:
        PeriodicMetricsLogger(uint64_t loggingPeriodMs, metrics::Profiler* profilerPtr, bool systemMetricsEnabled,
                std::string systemIDString);
        ~PeriodicMetricsLogger();

        void Start();
        void Shutdown();
        void FlushMetricsToFile();
        [[nodiscard]] pid_t GetThreadId() const;

    private:
        static std::shared_ptr<spdlog::logger> CreateMetricLogger(const std::string& metricFilename,
                const std::string& loggerName, const std::string& systemIDString);

        void Run();
        void LogMetrics();

        std::string systemIDString_;
        std::atomic<bool> running_;
        std::future<void> loggerFuture_;
        uint64_t loggingPeriodMs_;
        uint64_t nextLoggingAtMs_;
        std::shared_ptr<spdlog::logger> pCounterLogger_;
        std::shared_ptr<spdlog::logger> pThroughputLogger_;
        std::shared_ptr<spdlog::logger> pLatencyLogger_;
        std::shared_ptr<spdlog::logger> pLatencyGradientLogger_;
        std::shared_ptr<spdlog::logger> pOutputCostLogger_;
        std::shared_ptr<spdlog::logger> pOpMetricsLogger_;
        std::shared_ptr<spdlog::logger> pSysMetricsLogger_;
        metrics::Profiler* pProfiler_;
        std::condition_variable cv_;
        std::mutex cvMutex_;
        bool systemMetricsEnabled_;
        std::atomic<pid_t> threadId_{-1};
    };

}// namespace enjima::metrics


#endif//ENJIMA_PERIODIC_METRICS_LOGGER_H
