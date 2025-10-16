//
// Created by m34ferna on 06/05/24.
//

#include "SystemMetricsProfiler.h"
#include "enjima/memory/MemoryUtil.h"
#include "enjima/runtime/RuntimeUtil.h"
#include "spdlog/spdlog.h"
#include <fstream>
#include <unistd.h>

namespace enjima::metrics {
    static const long kPageSizeInKB = enjima::memory::ToKiloBytes(sysconf(_SC_PAGE_SIZE));
    static const long kClockTicksPerSec = sysconf(_SC_CLK_TCK);

    SystemMetricsProfiler::SystemMetricsProfiler() : enjimaPID_(getpid())
    {
        procFileStream_ = "/proc/" + std::to_string(enjimaPID_) + "/stat";
        numProcessors_ = std::thread::hardware_concurrency();
    }

    SystemMetricsProfiler::~SystemMetricsProfiler()
    {
        for (const auto& sysGaugePair: systemGaugeLongMap_) {
            delete sysGaugePair.second;
        }
        for (const auto& sysGaugePair: systemGaugeUnsignedLongMap_) {
            delete sysGaugePair.second;
        }
        for (const auto& sysGaugePair: systemGaugeDoubleMap_) {
            delete sysGaugePair.second;
        }
    }

    bool SystemMetricsProfiler::AddSystemGauge(const std::string& metricName, const std::string& processName,
            pid_t processID, long* pMetricVal)
    {
        auto pSystemGauge = new SystemGauge<long>{processID, processName, pMetricVal};
        auto emplaceSuccess = systemGaugeLongMap_.emplace(metricName, pSystemGauge);
        return emplaceSuccess.second;
    }

    bool SystemMetricsProfiler::AddSystemGauge(const std::string& metricName, const std::string& processName,
            pid_t processID, unsigned long* pMetricVal)
    {
        auto pSystemGauge = new SystemGauge<unsigned long>{processID, processName, pMetricVal};
        auto emplaceSuccess = systemGaugeUnsignedLongMap_.emplace(metricName, pSystemGauge);
        return emplaceSuccess.second;
    }

    bool SystemMetricsProfiler::AddSystemGauge(const std::string& metricName, const std::string& processName,
            pid_t processID, double* pMetricVal)
    {
        auto pSystemGauge = new SystemGauge<double>{processID, processName, pMetricVal};
        auto emplaceSuccess = systemGaugeDoubleMap_.emplace(metricName, pSystemGauge);
        return emplaceSuccess.second;
    }

    void SystemMetricsProfiler::UpdateSystemMetrics()
    {
        // the fields we want
        unsigned long minFlt;
        unsigned long cMinFlt;
        unsigned long majFlt;
        unsigned long cMajFlt;
        unsigned long uTime;
        unsigned long sTime;
        unsigned long cUTime;
        unsigned long cSTime;
        long nThread;
        unsigned long vSize;
        long rss;
        unsigned long startData;
        unsigned long endData;
        unsigned long startBrk;
        int procsr;

        auto currentTimeMs = enjima::runtime::GetSystemTimeMillis();
        {
            std::string ignore;
            std::ifstream ifs(procFileStream_, std::ios_base::in);
            ifs >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> minFlt >>
                    cMinFlt >> majFlt >> cMajFlt >> uTime >> sTime >> cUTime >> cSTime >> ignore >> ignore >> nThread >>
                    ignore >> ignore >> vSize >> rss >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >>
                    ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> procsr >> ignore >>
                    ignore >> ignore >> ignore >> ignore >> startData >> endData >> startBrk;
        }

        minFlt_ = minFlt;
        cMinFlt_ = cMinFlt;
        majFlt_ = majFlt;
        cMajFlt_ = cMajFlt;

        if (lastUpdatedAt_ > 0) {
            auto cpuTimeDeltaMs =
                    ((uTime * 1000 / kClockTicksPerSec) + (sTime * 1000 / kClockTicksPerSec)) - (uTimeMs_ + sTimeMs_);
            auto wallTimeDelta = currentTimeMs - lastUpdatedAt_;
            cpuPercent_ = ((double) cpuTimeDeltaMs * 100 / numProcessors_) / (double) wallTimeDelta;
        }
        lastUpdatedAt_ = currentTimeMs;
        uTimeMs_ = uTime * 1000 / kClockTicksPerSec;
        sTimeMs_ = sTime * 1000 / kClockTicksPerSec;
        cUTimeMs_ = cUTime * 1000 / kClockTicksPerSec;
        cSTimeMs_ = cSTime * 1000 / kClockTicksPerSec;
        numThreads_ = nThread;

        vmUsageKB_ = enjima::memory::ToKiloBytes(vSize);
        residentSetSizeKB_ = rss * kPageSizeInKB;
        processor_ = procsr;
        startData_ = startData;
        endData_ = endData;
        startBrk_ = startBrk;
    }

    const ConcurrentUnorderedMapTBB<std::string, SystemGauge<long>*>& SystemMetricsProfiler::GetSystemGaugeLongMap()
    {
        return systemGaugeLongMap_;
    }

    const ConcurrentUnorderedMapTBB<std::string, SystemGauge<unsigned long>*>&
    SystemMetricsProfiler::GetSystemGaugeUnsignedLongMap()
    {
        return systemGaugeUnsignedLongMap_;
    }

    const ConcurrentUnorderedMapTBB<std::string, SystemGauge<double>*>& SystemMetricsProfiler::GetSystemGaugeDoubleMap()
    {
        return systemGaugeDoubleMap_;
    }

    void SystemMetricsProfiler::Init()
    {
        spdlog::info("Process ID of program detected as {} and proc file stream set to : {}", enjimaPID_,
                procFileStream_);
        AddSystemGauge("min_flt", "enjima", enjimaPID_, &minFlt_);
        AddSystemGauge("c_min_flt", "enjima", enjimaPID_, &cMinFlt_);
        AddSystemGauge("maj_flt", "enjima", enjimaPID_, &majFlt_);
        AddSystemGauge("c_maj_flt", "enjima", enjimaPID_, &cMajFlt_);
        AddSystemGauge("u_time_ms", "enjima", enjimaPID_, &uTimeMs_);
        AddSystemGauge("s_time_ms", "enjima", enjimaPID_, &sTimeMs_);
        AddSystemGauge("c_u_time_ms", "enjima", enjimaPID_, &cUTimeMs_);
        AddSystemGauge("c_s_time_ms", "enjima", enjimaPID_, &cSTimeMs_);
        AddSystemGauge("num_threads", "enjima", enjimaPID_, &numThreads_);
        AddSystemGauge("vm_usage_kb", "enjima", enjimaPID_, &vmUsageKB_);
        AddSystemGauge("rss_kb", "enjima", enjimaPID_, &residentSetSizeKB_);
        AddSystemGauge("processor", "enjima", enjimaPID_, &processor_);
        AddSystemGauge("start_data", "enjima", enjimaPID_, &startData_);
        AddSystemGauge("end_data", "enjima", enjimaPID_, &endData_);
        AddSystemGauge("start_brk", "enjima", enjimaPID_, &startBrk_);
        AddSystemGauge("cpu_percent", "enjima", enjimaPID_, &cpuPercent_);
    }

}// namespace enjima::metrics
