//
// Created by m34ferna on 06/05/24.
//

namespace enjima::metrics {
    template<typename T>
        requires std::is_arithmetic_v<T>
    T SystemGauge<T>::GetVal()
    {
        return *pSysMetricVal_;
    }

    template<typename T>
        requires std::is_arithmetic_v<T>
    SystemGauge<T>::SystemGauge(pid_t processID, std::string processName, T* pSysMetricVal)
        : Gauge<T>(0), processID_(processID), processName_(std::move(processName)), pSysMetricVal_(pSysMetricVal)
    {
    }

    template<typename T>
        requires std::is_arithmetic_v<T>
    pid_t SystemGauge<T>::GetProcessId() const
    {
        return processID_;
    }

    template<typename T>
        requires std::is_arithmetic_v<T>
    const std::string& SystemGauge<T>::GetProcessName() const
    {
        return processName_;
    }
}// namespace enjima::metrics
