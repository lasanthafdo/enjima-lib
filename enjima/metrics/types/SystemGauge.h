//
// Created by m34ferna on 06/05/24.
//

#ifndef ENJIMA_SYSTEM_GAUGE_H
#define ENJIMA_SYSTEM_GAUGE_H

#include "Gauge.h"
#include <string>
#include <sys/types.h>

namespace enjima::metrics {

    template<typename T>
        requires std::is_arithmetic_v<T>
    class SystemGauge : public Gauge<T> {
    public:
        SystemGauge(pid_t processID, std::string processName, T* pSysMetricVal);
        [[nodiscard]] pid_t GetProcessId() const;
        [[nodiscard]] const std::string& GetProcessName() const;
        T GetVal() override;

    private:
        pid_t processID_;
        const std::string processName_;
        T* pSysMetricVal_;
    };

}// namespace enjima::metrics

#include "SystemGauge.tpp"

#endif//ENJIMA_SYSTEM_GAUGE_H
