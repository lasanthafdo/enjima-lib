//
// Created by m34ferna on 20/06/24.
//

#include "RuntimeConfiguration.h"
#include "ConfigurationException.h"
#include "InitializationException.h"
#include "spdlog/spdlog.h"

#include <filesystem>
#include <fstream>

namespace enjima::runtime {
    RuntimeConfiguration::RuntimeConfiguration(std::string configFilePath) : configFilePath_(std::move(configFilePath))
    {
        try {
            std::filesystem::path fsPathConfig{configFilePath_};
            if (!std::filesystem::exists(fsPathConfig)) {
                char programPathBuf[256];
                if (GetProgramDir(programPathBuf, sizeof(programPathBuf)) > 0) {
                    std::filesystem::path programPath{std::string(programPathBuf)};
                    fsPathConfig = programPath.parent_path().append(configFilePath_);
                    configFilePath_ = fsPathConfig.string();
                }
            }
            config_ = YAML::LoadFile(configFilePath_);
        }
        catch (YAML::BadFile& ex) {
            spdlog::error("Error when loading configuration file from " + configFilePath_);
            throw InitializationException{std::string("Error loading configuration file: ").append(ex.what())};
        }
    }

    std::string RuntimeConfiguration::GetConfigAsString(std::initializer_list<std::string> configNames) const
    {
        std::vector<std::string> configNamesVec{configNames};
        try {
            switch (configNamesVec.size()) {
                case 1:
                    return config_[configNamesVec[0]].as<std::string>();
                case 2:
                    return config_[configNamesVec[0]][configNamesVec[1]].as<std::string>();
                case 3:
                    return config_[configNamesVec[0]][configNamesVec[1]][configNamesVec[2]].as<std::string>();
                case 4:
                    return config_[configNamesVec[0]][configNamesVec[1]][configNamesVec[2]][configNamesVec[3]]
                            .as<std::string>();
                default:
                    return "";
            }
        }
        catch (const YAML::InvalidNode& e) {
            spdlog::error("InvalidNode exception from YAML-CPP: {}", e.what());
            return "";
        }
    }


    void RuntimeConfiguration::SetConfigAsString(std::initializer_list<std::string> configNames, const std::string& val)
    {
        std::vector<std::string> configNamesVec{configNames};
        switch (configNamesVec.size()) {
            case 1:
                config_[configNamesVec[0]] = val;
                break;
            case 2:
                config_[configNamesVec[0]][configNamesVec[1]] = val;
                break;
            case 3:
                config_[configNamesVec[0]][configNamesVec[1]][configNamesVec[2]] = val;
                break;
            case 4:
                config_[configNamesVec[0]][configNamesVec[1]][configNamesVec[2]][configNamesVec[3]] = val;
                break;
            default:
                throw ConfigurationException{"Configuration depth must be between 1 and 4!"};
        }
    }

    uint32_t RuntimeConfiguration::GetNumAvailableCpus() const
    {
        auto eventGenCpuListStr = config_["runtime"]["eventGenCpuList"].as<std::string>();
        auto numEventGenCpus = GetNumCpusInList(eventGenCpuListStr);
        auto workerCpuListStr = config_["runtime"]["workerCpuList"].as<std::string>();
        auto numWorkerCpus = GetNumCpusInList(workerCpuListStr);
        return numEventGenCpus + numWorkerCpus;
    }

    uint32_t RuntimeConfiguration::GetNumCpusInList(const std::string& cpuListStr)
    {
        std::stringstream strStream(cpuListStr);
        auto numCpus = 0u;
        while (strStream.good()) {
            std::string substr;
            getline(strStream, substr, ',');
            if (!substr.empty()) {
                numCpus++;
            }
        }
        return numCpus;
    }

    uint64_t RuntimeConfiguration::GetInternalMetricsUpdateIntervalMillis() const
    {
        auto updateIntervalMs = config_["metrics"]["internalUpdateIntervalMs"].as<uint64_t>();
        return updateIntervalMs;
    }

    long RuntimeConfiguration::GetProgramDir(char* pBuf, ssize_t len)
    {
        auto bytes = std::min(readlink("/proc/self/exe", pBuf, len), len - 1);
        if (bytes >= 0) pBuf[bytes] = '\0';
        return bytes;
    }

    void RuntimeConfiguration::SaveConfigToFile()
    {
        std::ofstream fileOut{configFilePath_};
        fileOut << config_;
    }

    uint8_t RuntimeConfiguration::GetPreAllocationMode() const
    {
        auto preAllocationMode = config_["runtime"]["preAllocationMode"].as<uint8_t>();
        return preAllocationMode;
    }
}// namespace enjima::runtime
