//
// Created by m34ferna on 20/06/24.
//

#ifndef ENJIMA_RUNTIME_CONFIGURATION_H
#define ENJIMA_RUNTIME_CONFIGURATION_H

#include "yaml-cpp/yaml.h"

namespace enjima::runtime {

    class RuntimeConfiguration {
    public:
        static long GetProgramDir(char* pBuf, ssize_t len);

        explicit RuntimeConfiguration(std::string configFilePath);
        std::string GetConfigAsString(std::initializer_list<std::string> configNames) const;
        void SetConfigAsString(std::initializer_list<std::string> configNames, const std::string& val);
        void SaveConfigToFile();
        uint32_t GetNumAvailableCpus() const;
        uint64_t GetInternalMetricsUpdateIntervalMillis() const;
        uint8_t GetPreAllocationMode() const;

    private:
        static uint32_t GetNumCpusInList(const std::string& cpuListStr);

        std::string configFilePath_;
        YAML::Node config_;
    };

}// namespace enjima::runtime


#endif//ENJIMA_RUNTIME_CONFIGURATION_H
