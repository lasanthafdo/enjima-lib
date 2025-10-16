//
// Created by m34ferna on 15/12/23.
//

#ifndef ENJIMA_RECORD_H
#define ENJIMA_RECORD_H

#include <cstdint>
#if ENJIMA_METRICS_LEVEL >= 3
#include <vector>
#endif


namespace enjima::core {
    template<typename T>
    class Record {
    public:
        enum class RecordType { kData, kWatermark, kLatency };

        Record();
        Record(uint64_t ts, T data);
        Record(RecordType recordType, uint64_t ts, T data);
        Record(RecordType recordType, uint64_t ts);
        RecordType GetRecordType() const;
        [[nodiscard]] uint64_t GetTimestamp() const;
        T GetData() const;

#if ENJIMA_METRICS_LEVEL >= 3
        Record(RecordType recordType, uint64_t ts, std::vector<uint64_t>* additionalMetricsPtr);
        [[nodiscard]] std::vector<uint64_t>* GetAdditionalMetricsPtr() const;
#endif

    private:
        RecordType recordType_;
        uint64_t timestamp_;
        T data_;

#if ENJIMA_METRICS_LEVEL >= 3
        std::vector<uint64_t>* additionalMetricsPtr_{nullptr};
#endif
    };
}// namespace enjima::core

#include "Record.tpp"

#endif//ENJIMA_RECORD_H
