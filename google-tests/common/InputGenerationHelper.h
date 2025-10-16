//
// Created by m34ferna on 05/04/24.
//

#ifndef ENJIMA_INPUT_GENERATION_HELPER_H
#define ENJIMA_INPUT_GENERATION_HELPER_H

#include "HelperTypes.h"
#include "enjima/api/data_types/LinearRoadEvent.h"
#include "enjima/api/data_types/YSBAdEvent.h"
#include "enjima/operators/LatencyTrackingSourceOperator.h"
#include "enjima/runtime/ExecutionEngine.h"
#include "enjima/runtime/IllegalStateException.h"
#include "enjima/runtime/InitializationException.h"
#include "enjima/runtime/RuntimeConfiguration.h"

#include <random>
#include <sstream>

using LinearRoadT = enjima::api::data_types::LinearRoadEvent;
using YSBAdT = enjima::api::data_types::YSBAdEvent;
using UniformIntDistParamT = std::uniform_int_distribution<int>::param_type;
using UniformUint64DistParamT = std::uniform_int_distribution<uint64_t>::param_type;

class RateLimitedGeneratingLRSource : public enjima::operators::SourceOperator<LinearRoadT> {
public:
    explicit RateLimitedGeneratingLRSource(enjima::operators::OperatorID opId, const std::string& opName,
            uint64_t maxGenerationRate)
        : enjima::operators::SourceOperator<LinearRoadT>(opId, opName), maxEmitRatePerMs_(maxGenerationRate / 1000)
    {
    }

    bool EmitEvent(enjima::core::OutputCollector* collector) override
    {
        uint64_t currentTimeUs = enjima::runtime::GetSystemTimeMicros();
        if (currentEmittedInMs_ >= maxEmitRatePerMs_) {
            auto sleepTime = currentEmissionStartUs_ + 1000 - currentTimeUs - sleepSlackTime_;
            if (sleepTime > 0) {
                std::this_thread::sleep_for(std::chrono::microseconds(sleepTime));
                currentTimeUs = enjima::runtime::GetSystemTimeMicros();
            }
        }
        if (currentTimeUs >= currentEmissionStartUs_ + 1000) {
            currentEmissionStartUs_ = currentTimeUs;
            currentEmittedInMs_ = 0;
        }
        currentEmittedInMs_++;
        LinearRoadT lrEvent = LinearRoadT(0, GetRandomInt(0, 10799), GetRandomInt(0, INT_MAX), GetRandomInt(0, 100),
                GetRandomInt(0, 2), GetRandomInt(0, 4), GetRandomInt(0, 1), GetRandomInt(0, 99),
                GetRandomInt(0, 527999), 0, 0, 0, GetRandomInt(1, 7), GetRandomInt(1, 1440), GetRandomInt(1, 69));
        collector->Collect<LinearRoadT>(lrEvent);
        return true;
    }

    uint32_t EmitBatch(uint32_t maxRecordsToWrite, void* outputBuffer,
            enjima::core::OutputCollector* collector) override
    {
        uint64_t currentTimeUs = enjima::runtime::GetSystemTimeMicros();
        if (currentEmittedInMs_ >= maxEmitRatePerMs_) {
            auto sleepTime = currentEmissionStartUs_ + 1000 - currentTimeUs - sleepSlackTime_;
            if (sleepTime > 0) {
                std::this_thread::sleep_for(std::chrono::microseconds(sleepTime));
                currentTimeUs = enjima::runtime::GetSystemTimeMicros();
            }
        }
        if (currentTimeUs >= currentEmissionStartUs_ + 1000) {
            currentEmissionStartUs_ = currentTimeUs;
            currentEmittedInMs_ = 0;
        }
        currentEmittedInMs_ += maxRecordsToWrite;
        auto currentTime = enjima::runtime::GetSystemTimeMillis();
        void* outBufferWritePtr = outputBuffer;
        for (auto i = maxRecordsToWrite; i > 0; i--) {
            LinearRoadT lrEvent = LinearRoadT(0, GetRandomInt(0, 10799), GetRandomInt(0, INT_MAX), GetRandomInt(0, 100),
                    GetRandomInt(0, 2), GetRandomInt(0, 4), GetRandomInt(0, 1), GetRandomInt(0, 99),
                    GetRandomInt(0, 527999), 0, 0, 0, GetRandomInt(1, 7), GetRandomInt(1, 1440), GetRandomInt(1, 69));
            new (outBufferWritePtr) enjima::core::Record<LinearRoadT>(currentTime, lrEvent);
            outBufferWritePtr = static_cast<enjima::core::Record<LinearRoadT>*>(outBufferWritePtr) + 1;
        }
        collector->CollectBatch<LinearRoadT>(outputBuffer, maxRecordsToWrite);
        return maxRecordsToWrite;
    }

    LinearRoadT GenerateQueueEvent() override
    {
        return {0, GetRandomUint64(0, 10799), GetRandomInt(0, INT_MAX), GetRandomInt(0, 100), GetRandomInt(0, 2),
                GetRandomInt(0, 4), GetRandomInt(0, 1), GetRandomInt(0, 99), GetRandomInt(0, 527999), 0, 0, 0,
                GetRandomInt(1, 7), GetRandomInt(1, 1440), GetRandomInt(1, 69)};
    }

    bool GenerateQueueRecord(enjima::core::Record<LinearRoadT>& outputRecord) override
    {
        // auto event = GenerateQueueEvent();
        LinearRoadT event;
        auto currentSysTime = enjima::runtime::GetSystemTimeMillis();
        outputRecord = enjima::core::Record<LinearRoadT>(currentSysTime, event);
        return true;
    }

private:
    int GetRandomInt(const int& a, const int& b)
    {
        UniformIntDistParamT pt(a, b);
        uniformIntDistribution.param(pt);
        return uniformIntDistribution(gen);
    }

    uint64_t GetRandomUint64(const uint64_t& a, const uint64_t& b)
    {
        UniformUint64DistParamT pt(a, b);
        uniformUint64Distribution.param(pt);
        return uniformUint64Distribution(gen);
    }

    std::random_device rd;
    std::mt19937 gen{rd()};
    std::uniform_int_distribution<int> uniformIntDistribution;
    std::uniform_int_distribution<uint64_t> uniformUint64Distribution;
    uint64_t maxEmitRatePerMs_;
    uint64_t currentEmittedInMs_{0};
    uint64_t currentEmissionStartUs_{0};
    uint64_t sleepSlackTime_{113};
};

class InMemoryYSBSourceOperator : public enjima::operators::SourceOperator<YSBAdT> {
public:
    explicit InMemoryYSBSourceOperator(enjima::operators::OperatorID opId, const std::string& opName)
        : enjima::operators::SourceOperator<YSBAdT>(opId, opName)
    {
    }


    YSBAdT GenerateQueueEvent() override
    {
        if (++cacheIterator_ == eventCache_.cend()) {
            cacheIterator_ = eventCache_.cbegin();
        }
        auto ysbEvent = *cacheIterator_.base();
        return ysbEvent;
    }

    bool GenerateQueueRecord(enjima::core::Record<YSBAdT>& outputRecord) override
    {
        auto event = GenerateQueueEvent();
        auto currentSysTime = enjima::runtime::GetSystemTimeMillis();
        outputRecord = enjima::core::Record<YSBAdT>(currentSysTime, event);
        return true;
    }

    bool EmitEvent(enjima::core::OutputCollector* collector) override
    {
        if (++cacheIterator_ == eventCache_.cend()) {
            cacheIterator_ = eventCache_.cbegin();
        }
        auto lrEvent = *cacheIterator_.base();
        collector->Collect<YSBAdT>(lrEvent);
        return true;
    }

    uint32_t EmitBatch(uint32_t maxRecordsToWrite, void* outputBuffer,
            enjima::core::OutputCollector* collector) override
    {
        auto currentTime = enjima::runtime::GetSystemTimeMillis();
        void* outBufferWritePtr = outputBuffer;
        for (auto i = maxRecordsToWrite; i > 0; i--) {
            if (++cacheIterator_ == eventCache_.cend()) {
                cacheIterator_ = eventCache_.cbegin();
            }
            auto ysbEvent = *cacheIterator_.base();
            new (outBufferWritePtr) enjima::core::Record<YSBAdT>(currentTime, ysbEvent);
            outBufferWritePtr = static_cast<enjima::core::Record<YSBAdT>*>(outBufferWritePtr) + 1;
        }
        collector->CollectBatch<YSBAdT>(outputBuffer, maxRecordsToWrite);
        return maxRecordsToWrite;
    }

    void PopulateEventCache(uint32_t numEvents, uint32_t numCampaigns, uint32_t numAdsPerCampaign)
    {
        UniformIntDistParamT pt(1, 1000000);
        uniformIntDistribution.param(pt);
        std::unordered_set<uint64_t> campaignIds(numCampaigns);
        auto adId = 0;
        auto numAds = numCampaigns * numAdsPerCampaign;
        while (campaignIds.size() < numCampaigns) {
            auto campaignId = uniformIntDistribution(mtEng);
            if (!campaignIds.contains(campaignId)) {
                campaignIds.emplace(campaignId);
                for (auto j = numAdsPerCampaign; j > 0; j--) {
                    adIdToCampaignIdMap_.emplace(++adId, campaignId);
                }
            }
        }

        eventCache_.reserve(numEvents);
        for (auto i = numEvents; i > 0; i--) {
            auto timestamp = enjima::runtime::GetSystemTimeMillis();
            auto userId = uniformIntDistribution(mtEng);
            auto pageId = uniformIntDistribution(mtEng);
            auto adType = i % 5;
            auto eventType = i % 3;
            YSBAdT lrEvent = YSBAdT(timestamp, userId, pageId, ((i % numAds) + 1), adType, eventType, -1);
            eventCache_.emplace_back(lrEvent);
        }
        cacheIterator_ = eventCache_.cbegin();
    }

    const UnorderedHashMapST<uint64_t, uint64_t>& GetAdIdToCampaignIdMap() const
    {
        return adIdToCampaignIdMap_;
    }

private:
    std::random_device rd;
    std::mt19937 mtEng{rd()};
    std::uniform_int_distribution<int> uniformIntDistribution;
    std::vector<YSBAdT> eventCache_;
    std::vector<YSBAdT>::const_iterator cacheIterator_;
    UnorderedHashMapST<uint64_t, uint64_t> adIdToCampaignIdMap_;
};

class GeneratingLinearRoadSourceOperator : public enjima::operators::SourceOperator<LinearRoadT> {
public:
    explicit GeneratingLinearRoadSourceOperator(enjima::operators::OperatorID opId, const std::string& opName)
        : enjima::operators::SourceOperator<LinearRoadT>(opId, opName)
    {
    }

    bool EmitEvent(enjima::core::OutputCollector* collector) override
    {
        LinearRoadT lrEvent = LinearRoadT(0, GetRandomInt(0, 10799), GetRandomInt(0, INT_MAX), GetRandomInt(0, 100),
                GetRandomInt(0, 2), GetRandomInt(0, 4), GetRandomInt(0, 1), GetRandomInt(0, 99),
                GetRandomInt(0, 527999), 0, 0, 0, GetRandomInt(1, 7), GetRandomInt(1, 1440), GetRandomInt(1, 69));
        collector->Collect<LinearRoadT>(lrEvent);
        return true;
    }

    uint32_t EmitBatch(uint32_t maxRecordsToWrite, void* outputBuffer,
            enjima::core::OutputCollector* collector) override
    {
        auto currentTime = enjima::runtime::GetSystemTimeMillis();
        void* outBufferWritePtr = outputBuffer;
        for (auto i = maxRecordsToWrite; i > 0; i--) {
            LinearRoadT lrEvent = LinearRoadT(0, GetRandomInt(0, 10799), GetRandomInt(0, INT_MAX), GetRandomInt(0, 100),
                    GetRandomInt(0, 2), GetRandomInt(0, 4), GetRandomInt(0, 1), GetRandomInt(0, 99),
                    GetRandomInt(0, 527999), 0, 0, 0, GetRandomInt(1, 7), GetRandomInt(1, 1440), GetRandomInt(1, 69));
            new (outBufferWritePtr) enjima::core::Record<LinearRoadT>(currentTime, lrEvent);
            outBufferWritePtr = static_cast<enjima::core::Record<LinearRoadT>*>(outBufferWritePtr) + 1;
        }
        collector->CollectBatch<LinearRoadT>(outputBuffer, maxRecordsToWrite);
        return maxRecordsToWrite;
    }

    LinearRoadT GenerateQueueEvent() override
    {
        LinearRoadT lrEvent = LinearRoadT(0, GetRandomInt(0, 10799), GetRandomInt(0, INT_MAX), GetRandomInt(0, 100),
                GetRandomInt(0, 2), GetRandomInt(0, 4), GetRandomInt(0, 1), GetRandomInt(0, 99),
                GetRandomInt(0, 527999), 0, 0, 0, GetRandomInt(1, 7), GetRandomInt(1, 1440), GetRandomInt(1, 69));
        return lrEvent;
    }

    bool GenerateQueueRecord(enjima::core::Record<LinearRoadT>& outputRecord) override
    {
        auto event = GenerateQueueEvent();
        auto currentSysTime = enjima::runtime::GetSystemTimeMillis();
        outputRecord = enjima::core::Record<LinearRoadT>(currentSysTime, event);
        return true;
    }

private:
    int GetRandomInt(const int& a, const int& b)
    {
        UniformIntDistParamT pt(a, b);
        uniformIntDistribution.param(pt);
        return uniformIntDistribution(gen);
    }

    std::random_device rd;
    std::mt19937 gen{rd()};
    std::uniform_int_distribution<int> uniformIntDistribution;
};

class InMemoryLinearRoadSourceOperator : public enjima::operators::SourceOperator<LinearRoadT> {
public:
    explicit InMemoryLinearRoadSourceOperator(enjima::operators::OperatorID opId, const std::string& opName)
        : enjima::operators::SourceOperator<LinearRoadT>(opId, opName)
    {
    }


    bool EmitEvent(enjima::core::OutputCollector* collector) override
    {
        if (++cacheIterator_ == eventCache_.cend()) {
            cacheIterator_ = eventCache_.cbegin();
        }
        auto lrEvent = *cacheIterator_.base();
        collector->Collect<LinearRoadT>(lrEvent);
        return true;
    }

    uint32_t EmitBatch(uint32_t maxRecordsToWrite, void* outputBuffer,
            enjima::core::OutputCollector* collector) override
    {
        auto currentTime = enjima::runtime::GetSystemTimeMillis();
        void* outBufferWritePtr = outputBuffer;
        for (auto i = maxRecordsToWrite; i > 0; i--) {
            if (++cacheIterator_ == eventCache_.cend()) {
                cacheIterator_ = eventCache_.cbegin();
            }
            auto lrEvent = *cacheIterator_.base();
            new (outBufferWritePtr) enjima::core::Record<LinearRoadT>(currentTime, lrEvent);
            outBufferWritePtr = static_cast<enjima::core::Record<LinearRoadT>*>(outBufferWritePtr) + 1;
        }
        collector->CollectBatch<LinearRoadT>(outputBuffer, maxRecordsToWrite);
        return maxRecordsToWrite;
    }

    LinearRoadT GenerateQueueEvent() override
    {
        if (++cacheIterator_ == eventCache_.cend()) {
            cacheIterator_ = eventCache_.cbegin();
        }
        auto lrEvent = *cacheIterator_.base();
        return lrEvent;
    }

    bool GenerateQueueRecord(enjima::core::Record<LinearRoadT>& outputRecord) override
    {
        // LinearRoadT event = InMemoryLinearRoadSourceOperator::GenerateQueueEvent();
        LinearRoadT event;
        auto currentSysTime = enjima::runtime::GetSystemTimeMillis();
        outputRecord = enjima::core::Record<LinearRoadT>(currentSysTime, event);
        return true;
    }

    void PopulateEventCache(uint32_t numEventsInCache)
    {
        eventCache_.reserve(numEventsInCache);
        for (auto i = numEventsInCache; i > 0; i--) {
            LinearRoadT lrEvent = LinearRoadT(0, GetRandomInt(0, 10799), GetRandomInt(0, INT_MAX), GetRandomInt(0, 100),
                    GetRandomInt(0, 2), GetRandomInt(0, 4), GetRandomInt(0, 1), GetRandomInt(0, 99),
                    GetRandomInt(0, 527999), 0, 0, 0, GetRandomInt(1, 7), GetRandomInt(1, 1440), GetRandomInt(1, 69));
            eventCache_.emplace_back(lrEvent);
        }
        cacheIterator_ = eventCache_.cbegin();
    }

private:
    int GetRandomInt(const int& a, const int& b)
    {
        UniformIntDistParamT pt(a, b);
        uniformIntDistribution.param(pt);
        return uniformIntDistribution(gen);
    }

    std::random_device rd;
    std::mt19937 gen{rd()};
    std::uniform_int_distribution<int> uniformIntDistribution;
    std::vector<LinearRoadT> eventCache_;
    std::vector<LinearRoadT>::const_iterator cacheIterator_;
};

class UIDBasedYSBSource : public enjima::operators::SourceOperator<UIDBasedYSBAdEvent> {
public:
    explicit UIDBasedYSBSource(enjima::operators::OperatorID opId, const std::string& opName,
            uint64_t latencyRecordEmitPeriodMs)
        : enjima::operators::SourceOperator<UIDBasedYSBAdEvent>(opId, opName),
          latencyRecordEmitPeriodMs_(latencyRecordEmitPeriodMs)
    {
    }


    bool EmitEvent(enjima::core::OutputCollector* collector) override
    {
        auto currentTime = enjima::runtime::GetSystemTimeMillis();
        if (currentTime >= (latencyRecordLastEmittedAt + latencyRecordEmitPeriodMs_)) {
            auto latencyRecord = enjima::core::Record<UIDBasedYSBAdEvent>(
                    enjima::core::Record<UIDBasedYSBAdEvent>::RecordType::kLatency, currentTime);
            collector->Collect(latencyRecord);
            latencyRecordLastEmittedAt = currentTime;
        }
        else {
            if (++cacheIterator_ == eventCache_.cend()) {
                cacheIterator_ = eventCache_.cbegin();
            }
            auto ysbEvent = *cacheIterator_.base();
            ysbEvent.SetUniqueIncrementingId(++numGenEvents_);
            collector->Collect<UIDBasedYSBAdEvent>(ysbEvent);
        }
        return true;
    }

    UIDBasedYSBAdEvent GenerateQueueEvent() override
    {
        if (++cacheIterator_ == eventCache_.cend()) {
            cacheIterator_ = eventCache_.cbegin();
        }
        auto ysbEvent = *cacheIterator_.base();
        ysbEvent.SetUniqueIncrementingId(++numGenEvents_);
        return ysbEvent;
    }

    bool GenerateQueueRecord(enjima::core::Record<UIDBasedYSBAdEvent>& outputRecord) override
    {
        auto currentTime = enjima::runtime::GetSystemTimeMillis();
        if (currentTime >= (latencyRecordLastEmittedAt + latencyRecordEmitPeriodMs_)) {
            outputRecord = enjima::core::Record<UIDBasedYSBAdEvent>(
                    enjima::core::Record<UIDBasedYSBAdEvent>::RecordType::kLatency, currentTime);
            latencyRecordLastEmittedAt = currentTime;
        }
        else {
            auto ysbEvent = GenerateQueueEvent();
            auto currentSysTime = enjima::runtime::GetSystemTimeMillis();
            outputRecord = enjima::core::Record<UIDBasedYSBAdEvent>(currentSysTime, ysbEvent);
        }
        return true;
    }

    uint32_t EmitBatch(uint32_t maxRecordsToWrite, void* outputBuffer,
            enjima::core::OutputCollector* collector) override
    {
        auto currentTime = enjima::runtime::GetSystemTimeMillis();
        void* outBufferWritePtr = outputBuffer;
        uint32_t numLatencyRecordsOut = 0;
        if (currentTime >= (latencyRecordLastEmittedAt + latencyRecordEmitPeriodMs_)) {
            new (outBufferWritePtr) enjima::core::Record<UIDBasedYSBAdEvent>(
                    enjima::core::Record<UIDBasedYSBAdEvent>::RecordType::kLatency, currentTime);
            outBufferWritePtr = static_cast<enjima::core::Record<UIDBasedYSBAdEvent>*>(outBufferWritePtr) + 1;
            latencyRecordLastEmittedAt = currentTime;
            numLatencyRecordsOut = 1;
        }
        for (auto i = (maxRecordsToWrite - numLatencyRecordsOut); i > 0; i--) {
            auto ysbEvent = *cacheIterator_.base();
            if (++cacheIterator_ == eventCache_.cend()) {
                cacheIterator_ = eventCache_.cbegin();
            }
            ysbEvent.SetUniqueIncrementingId(++numGenEvents_);
            new (outBufferWritePtr) enjima::core::Record<UIDBasedYSBAdEvent>(currentTime, ysbEvent);
            outBufferWritePtr = static_cast<enjima::core::Record<UIDBasedYSBAdEvent>*>(outBufferWritePtr) + 1;
        }
        collector->CollectBatch<UIDBasedYSBAdEvent>(outputBuffer, maxRecordsToWrite);
        return maxRecordsToWrite;
    }

    void PopulateEventCache(uint32_t numEvents, uint32_t numCampaigns, uint32_t numAdsPerCampaign)
    {
        UniformIntDistParamT pt(1, 1000000);
        uniformIntDistribution.param(pt);
        std::unordered_set<uint64_t> campaignIds(numCampaigns);
        auto adId = 0;
        auto numAds = numCampaigns * numAdsPerCampaign;
        while (campaignIds.size() < numCampaigns) {
            auto campaignId = uniformIntDistribution(mtEng);
            if (!campaignIds.contains(campaignId)) {
                campaignIds.emplace(campaignId);
                for (auto j = numAdsPerCampaign; j > 0; j--) {
                    adIdToCampaignIdMap_.emplace(++adId, campaignId);
                }
            }
        }

        eventCache_.reserve(numEvents);
        for (auto i = numEvents; i > 0; i--) {
            auto timestamp = enjima::runtime::GetSystemTimeMillis();
            auto userId = uniformIntDistribution(mtEng);
            auto pageId = uniformIntDistribution(mtEng);
            auto adType = i % 5;
            auto eventType = i % 3;
            auto uidBasedYsbAdEvent =
                    UIDBasedYSBAdEvent(timestamp, userId, pageId, ((i % numAds) + 1), adType, eventType, -1, 0);
            eventCache_.emplace_back(uidBasedYsbAdEvent);
        }
        cacheIterator_ = eventCache_.cbegin();
    }

    const UnorderedHashMapST<uint64_t, uint64_t>& GetAdIdToCampaignIdMap() const
    {
        return adIdToCampaignIdMap_;
    }

private:
    std::random_device rd;
    std::mt19937 mtEng{rd()};
    std::uniform_int_distribution<int> uniformIntDistribution;
    std::vector<UIDBasedYSBAdEvent> eventCache_;
    std::vector<UIDBasedYSBAdEvent>::const_iterator cacheIterator_;
    UnorderedHashMapST<uint64_t, uint64_t> adIdToCampaignIdMap_;
    uint64_t numGenEvents_{0};
    uint64_t latencyRecordEmitPeriodMs_{0};
    uint64_t latencyRecordLastEmittedAt{0};
};

template<typename Duration>
class LatencyTrackingUIDBasedYSBSource
    : public enjima::operators::LatencyTrackingSourceOperator<UIDBasedYSBAdEvent, Duration> {
public:
    explicit LatencyTrackingUIDBasedYSBSource(enjima::operators::OperatorID opId, const std::string& opName,
            uint64_t latencyRecordEmitPeriodMs)
        : enjima::operators::LatencyTrackingSourceOperator<UIDBasedYSBAdEvent, Duration>(opId, opName,
                  latencyRecordEmitPeriodMs)
    {
    }


    bool EmitEvent(enjima::core::OutputCollector* collector) override
    {
        auto currentTime = enjima::runtime::GetSystemTimeMillis();
        if (currentTime >= (this->latencyRecordLastEmittedAt_ + this->latencyRecordEmitPeriodMs_)) {
            auto latencyRecord = enjima::core::Record<UIDBasedYSBAdEvent>(
                    enjima::core::Record<UIDBasedYSBAdEvent>::RecordType::kLatency,
                    enjima::runtime::GetSystemTime<Duration>());
            collector->Collect(latencyRecord);
            this->latencyRecordLastEmittedAt_ = currentTime;
        }
        else {
            if (++cacheIterator_ == eventCache_.cend()) {
                cacheIterator_ = eventCache_.cbegin();
            }
            auto ysbEvent = *cacheIterator_.base();
            ysbEvent.SetUniqueIncrementingId(++numGenEvents_);
            collector->Collect<UIDBasedYSBAdEvent>(ysbEvent);
        }
        return true;
    }

    uint32_t EmitBatch(uint32_t maxRecordsToWrite, void* outputBuffer,
            enjima::core::OutputCollector* collector) override
    {
        auto currentTime = enjima::runtime::GetSystemTimeMillis();
        void* outBufferWritePtr = outputBuffer;
        uint32_t numLatencyRecordsOut = 0;
        if (currentTime >= (this->latencyRecordLastEmittedAt_ + this->latencyRecordEmitPeriodMs_)) {
#if ENJIMA_METRICS_LEVEL >= 3
            auto metricsVec = new std::vector<uint64_t>;
            metricsVec->emplace_back(enjima::runtime::GetSystemTimeMicros());
            new (outBufferWritePtr) enjima::core::Record<UIDBasedYSBAdEvent>(
                    enjima::core::Record<UIDBasedYSBAdEvent>::RecordType::kLatency,
                    enjima::runtime::GetSystemTime<Duration>(), metricsVec);
#else
            new (outBufferWritePtr) enjima::core::Record<UIDBasedYSBAdEvent>(
                    enjima::core::Record<UIDBasedYSBAdEvent>::RecordType::kLatency,
                    enjima::runtime::GetSystemTime<Duration>());
#endif
            outBufferWritePtr = static_cast<enjima::core::Record<UIDBasedYSBAdEvent>*>(outBufferWritePtr) + 1;
            this->latencyRecordLastEmittedAt_ = currentTime;
            numLatencyRecordsOut = 1;
        }
        for (auto i = (maxRecordsToWrite - numLatencyRecordsOut); i > 0; i--) {
            auto ysbEvent = *cacheIterator_.base();
            if (++cacheIterator_ == eventCache_.cend()) {
                cacheIterator_ = eventCache_.cbegin();
            }
            ysbEvent.SetUniqueIncrementingId(++numGenEvents_);
            new (outBufferWritePtr) enjima::core::Record<UIDBasedYSBAdEvent>(currentTime, ysbEvent);
            outBufferWritePtr = static_cast<enjima::core::Record<UIDBasedYSBAdEvent>*>(outBufferWritePtr) + 1;
        }
        collector->CollectBatch<UIDBasedYSBAdEvent>(outputBuffer, maxRecordsToWrite);
        return maxRecordsToWrite;
    }

    UIDBasedYSBAdEvent GenerateQueueEvent() override
    {
        if (++cacheIterator_ == eventCache_.cend()) {
            cacheIterator_ = eventCache_.cbegin();
        }
        auto ysbEvent = *cacheIterator_.base();
        ysbEvent.SetUniqueIncrementingId(++numGenEvents_);
        return ysbEvent;
    }

    bool GenerateQueueRecord(enjima::core::Record<UIDBasedYSBAdEvent>& outputRecord) override
    {
        auto currentTime = enjima::runtime::GetSystemTimeMillis();
        if (currentTime >= (this->latencyRecordLastEmittedAt_ + this->latencyRecordEmitPeriodMs_)) {
            outputRecord = enjima::core::Record<UIDBasedYSBAdEvent>(
                    enjima::core::Record<UIDBasedYSBAdEvent>::RecordType::kLatency,
                    enjima::runtime::GetSystemTime<Duration>());
            this->latencyRecordLastEmittedAt_ = currentTime;
        }
        else {
            auto ysbEvent = GenerateQueueEvent();
            auto currentSysTime = enjima::runtime::GetSystemTimeMillis();
            outputRecord = enjima::core::Record<UIDBasedYSBAdEvent>(currentSysTime, ysbEvent);
        }
        return true;
    }

    void PopulateEventCache(uint32_t numEvents, uint32_t numCampaigns, uint32_t numAdsPerCampaign)
    {
        UniformIntDistParamT pt(1, 1000000);
        uniformIntDistribution.param(pt);
        std::unordered_set<uint64_t> campaignIds(numCampaigns);
        auto adId = 0;
        auto numAds = numCampaigns * numAdsPerCampaign;
        while (campaignIds.size() < numCampaigns) {
            auto campaignId = uniformIntDistribution(mtEng);
            if (!campaignIds.contains(campaignId)) {
                campaignIds.emplace(campaignId);
                for (auto j = numAdsPerCampaign; j > 0; j--) {
                    adIdToCampaignIdMap_.emplace(++adId, campaignId);
                }
            }
        }

        eventCache_.reserve(numEvents);
        for (auto i = numEvents; i > 0; i--) {
            auto timestamp = enjima::runtime::GetSystemTimeMillis();
            auto userId = uniformIntDistribution(mtEng);
            auto pageId = uniformIntDistribution(mtEng);
            auto adType = i % 5;
            auto eventType = i % 3;
            auto uidBasedYsbAdEvent =
                    UIDBasedYSBAdEvent(timestamp, userId, pageId, ((i % numAds) + 1), adType, eventType, -1, 0);
            eventCache_.emplace_back(uidBasedYsbAdEvent);
        }
        cacheIterator_ = eventCache_.cbegin();
    }

    const UnorderedHashMapST<uint64_t, uint64_t>& GetAdIdToCampaignIdMap() const
    {
        return adIdToCampaignIdMap_;
    }

private:
    std::random_device rd;
    std::mt19937 mtEng{rd()};
    std::uniform_int_distribution<int> uniformIntDistribution;
    std::vector<UIDBasedYSBAdEvent> eventCache_;
    std::vector<UIDBasedYSBAdEvent>::const_iterator cacheIterator_;
    UnorderedHashMapST<uint64_t, uint64_t> adIdToCampaignIdMap_;
    uint64_t numGenEvents_{0};
};

template<typename Duration>
class FixedRateLatencyTrackingUIDBasedYSBSource
    : public enjima::operators::LatencyTrackingSourceOperator<UIDBasedYSBAdEvent, Duration> {
public:
    using UIDBasedYSBAdRecordT = enjima::core::Record<UIDBasedYSBAdEvent>;
    FixedRateLatencyTrackingUIDBasedYSBSource(enjima::operators::OperatorID opId, const std::string& opName,
            uint64_t latencyRecordEmitPeriodMs, uint64_t maxInputRate, uint64_t srcReservoirCapacity)
        : enjima::operators::LatencyTrackingSourceOperator<UIDBasedYSBAdEvent, Duration>(opId, opName,
                  latencyRecordEmitPeriodMs),
          maxEmitRatePerMs_(maxInputRate / 1000), srcReservoirCapacity_(srcReservoirCapacity),
          eventReservoir_(static_cast<UIDBasedYSBAdRecordT*>(
                  aligned_alloc(ENJIMA_BUFFER_ALIGNMENT, sizeof(UIDBasedYSBAdRecordT) * srcReservoirCapacity)))
    {
        nextEmitStartUs_ = (enjima::runtime::GetSystemTimeMillis() + 1) * 1000;
    }

    ~FixedRateLatencyTrackingUIDBasedYSBSource()
    {
        genTaskRunning_.store(false, std::memory_order::release);
        eventGenThread_.join();
        free(static_cast<void*>(eventReservoir_));
    }

    bool EmitEvent(enjima::core::OutputCollector* collector) override
    {
        if (cachedWriteIdx_.load(std::memory_order::acquire) > cachedReadIdx_.load(std::memory_order::acquire)) {
            auto readBeginIdx = cachedReadIdx_.load(std::memory_order::acquire) % srcReservoirCapacity_;
            auto nextRecord = eventReservoir_[readBeginIdx];
            cachedReadIdx_.fetch_add(1, std::memory_order::acq_rel);
            collector->Collect(nextRecord);
            return true;
        }
        return false;
    }

    uint32_t EmitBatch(uint32_t maxRecordsToWrite, void* outputBuffer,
            enjima::core::OutputCollector* collector) override
    {
        auto numRecordsToCopy =
                std::min(maxRecordsToWrite, static_cast<uint32_t>(cachedWriteIdx_.load(std::memory_order::acquire) -
                                                                  cachedReadIdx_.load(std::memory_order::acquire)));
        if (numRecordsToCopy > 0) {
            auto readBeginIdx = cachedReadIdx_.load(std::memory_order::acquire) % srcReservoirCapacity_;
            auto readEndIdx = readBeginIdx + numRecordsToCopy;
            auto srcBeginPtr = static_cast<void*>(&eventReservoir_[readBeginIdx]);
            if (readEndIdx > srcReservoirCapacity_) {
                auto numRecordsToReadFromBeginning = readEndIdx - srcReservoirCapacity_;
                auto numRecordsToReadFromEnd = numRecordsToCopy - numRecordsToReadFromBeginning;
                collector->CollectBatch<UIDBasedYSBAdEvent>(srcBeginPtr, numRecordsToReadFromEnd);
                // We have to circle back to read the rest of the events
                srcBeginPtr = static_cast<void*>(eventReservoir_);
                collector->CollectBatch<UIDBasedYSBAdEvent>(srcBeginPtr, numRecordsToReadFromBeginning);
            }
            else {
                collector->CollectBatch<UIDBasedYSBAdEvent>(srcBeginPtr, numRecordsToCopy);
            }
            cachedReadIdx_.fetch_add(numRecordsToCopy, std::memory_order::acq_rel);
            assert(numRecordsToCopy <= maxRecordsToWrite);
#if ENJIMA_METRICS_LEVEL >= 3
            batchSizeGauge_->UpdateVal(static_cast<double>(numRecordsToCopy));
#endif
        }
        return numRecordsToCopy;
    }

    UIDBasedYSBAdEvent GenerateQueueEvent() override
    {
        throw enjima::runtime::IllegalStateException{"Unsupported method called!"};
    }

    bool GenerateQueueRecord(enjima::core::Record<UIDBasedYSBAdEvent>& outputRecord) override
    {
        if (cachedWriteIdx_.load(std::memory_order::acquire) > cachedReadIdx_.load(std::memory_order::acquire)) {
            auto readBeginIdx = cachedReadIdx_.load(std::memory_order::acquire) % srcReservoirCapacity_;
            outputRecord = eventReservoir_[readBeginIdx];
            cachedReadIdx_.fetch_add(1, std::memory_order::acq_rel);
            return true;
        }
        return false;
    }

    void PopulateEventCache(uint32_t numEvents, uint32_t numCampaigns, uint32_t numAdsPerCampaign)
    {
        UniformIntDistParamT pt(1, 1000000);
        uniformIntDistribution.param(pt);
        std::unordered_set<uint64_t> campaignIds(numCampaigns);
        auto adId = 0;
        auto numAds = numCampaigns * numAdsPerCampaign;
        while (campaignIds.size() < numCampaigns) {
            auto campaignId = uniformIntDistribution(mtEng);
            if (!campaignIds.contains(campaignId)) {
                campaignIds.emplace(campaignId);
                for (auto j = numAdsPerCampaign; j > 0; j--) {
                    adIdToCampaignIdMap_.emplace(++adId, campaignId);
                }
            }
        }

        eventCache_.reserve(numEvents);
        for (auto i = numEvents; i > 0; i--) {
            auto timestamp = enjima::runtime::GetSystemTimeMillis();
            auto userId = uniformIntDistribution(mtEng);
            auto pageId = uniformIntDistribution(mtEng);
            auto adType = i % 5;
            auto eventType = i % 3;
            auto uidBasedYsbAdEvent =
                    UIDBasedYSBAdEvent(timestamp, userId, pageId, ((i % numAds) + 1), adType, eventType, -1, 0);
            eventCache_.emplace_back(uidBasedYsbAdEvent);
        }
    }

    const UnorderedHashMapST<uint64_t, uint64_t>& GetAdIdToCampaignIdMap() const
    {
        return adIdToCampaignIdMap_;
    }

    void GenerateEvents()
    {
        readyPromise_.get_future().wait();
        // Environment initialization
        this->pExecutionEngine_->PinThreadToCpuListFromConfig(enjima::runtime::SupportedThreadType::kEventGen);
        pthread_setname_np(pthread_self(),
                std::string("event_gen_").append(std::to_string(this->GetOperatorId())).c_str());
        // End of environment initialization
        cacheIterator_ = eventCache_.cbegin();
        auto blockSize = this->pMemoryManager_->GetDefaultNumEventsPerBlock();
        auto recordSize = sizeof(UIDBasedYSBAdRecordT);
        genWriteBufferBeginPtr_ = aligned_alloc(ENJIMA_BUFFER_ALIGNMENT, blockSize * recordSize);
        size_t numRecordsToWrite;
        while (genTaskRunning_.load(std::memory_order::acquire)) {
            SleepIfNeeded();
            auto eventReservoirSize = srcReservoirCapacity_ - (cachedWriteIdx_.load(std::memory_order::acquire) -
                                                                      cachedReadIdx_.load(std::memory_order::acquire));
            numRecordsToWrite = std::min(eventReservoirSize, blockSize);
            if (numRecordsToWrite > 0) {
                auto* writeBuffer = static_cast<UIDBasedYSBAdRecordT*>(genWriteBufferBeginPtr_);
                auto currentTime = enjima::runtime::GetSystemTimeMillis();
                size_t numLatencyRecordsWritten = 0;
                if (currentTime >= (this->latencyRecordLastEmittedAt_ + this->latencyRecordEmitPeriodMs_)) {
#if ENJIMA_METRICS_LEVEL >= 3
                    auto metricsVec = new std::vector<uint64_t>;
                    metricsVec->emplace_back(enjima::runtime::GetSystemTimeMicros());
                    new (writeBuffer++) UIDBasedYSBAdRecordT(UIDBasedYSBAdRecordT::RecordType::kLatency,
                            enjima::runtime::GetSystemTime<Duration>(), metricsVec);
#else
                    new (writeBuffer++) UIDBasedYSBAdRecordT(UIDBasedYSBAdRecordT::RecordType::kLatency,
                            enjima::runtime::GetSystemTime<Duration>());
#endif
                    this->latencyRecordLastEmittedAt_ = currentTime;
                    numLatencyRecordsWritten = 1;
                }
                for (auto i = (numRecordsToWrite - numLatencyRecordsWritten); i > 0; i--) {
                    if (++cacheIterator_ == eventCache_.cend()) {
                        cacheIterator_ = eventCache_.cbegin();
                    }
                    auto ysbEvent = *cacheIterator_.base();
                    ysbEvent.SetUniqueIncrementingId(++numGenEvents_);
                    new (writeBuffer++) UIDBasedYSBAdRecordT(currentTime, ysbEvent);
                }
                auto writeBeginIndex = cachedWriteIdx_.load(std::memory_order::acquire) % srcReservoirCapacity_;
                // Note writeEndIdx is the index after the last element's index (e.g., cap=10, beginIdx=6, write to=6,7,8,9, endIdx=10, wrote 4)
                auto writeEndIdx = writeBeginIndex + numRecordsToWrite;
                void* destBeginPtr = static_cast<void*>(&eventReservoir_[writeBeginIndex]);
                if (writeEndIdx > srcReservoirCapacity_) {
                    auto numRecordsToWriteAtBeginning = writeEndIdx - srcReservoirCapacity_;
                    auto numRecordsToWriteAtEnd = numRecordsToWrite - numRecordsToWriteAtBeginning;
                    std::memcpy(destBeginPtr, genWriteBufferBeginPtr_, numRecordsToWriteAtEnd * recordSize);
                    // We have to circle back to write the rest of the events
                    auto currentGenWriteBufferPtr = static_cast<void*>(
                            static_cast<UIDBasedYSBAdRecordT*>(genWriteBufferBeginPtr_) + numRecordsToWriteAtEnd);
                    destBeginPtr = static_cast<void*>(eventReservoir_);
                    std::memcpy(destBeginPtr, currentGenWriteBufferPtr, numRecordsToWriteAtBeginning * recordSize);
                }
                else {
                    std::memcpy(destBeginPtr, genWriteBufferBeginPtr_, numRecordsToWrite * recordSize);
                }
                currentEmittedInMs_ += numRecordsToWrite;
                cachedWriteIdx_.fetch_add(numRecordsToWrite, std::memory_order::acq_rel);
            }
        }
        free(genWriteBufferBeginPtr_);
    }

    void Initialize(enjima::runtime::ExecutionEngine* executionEngine, enjima::memory::MemoryManager* memoryManager,
            enjima::metrics::Profiler* profiler) override
    {
        enjima::operators::SourceOperator<UIDBasedYSBAdEvent>::Initialize(executionEngine, memoryManager, profiler);
#if ENJIMA_METRICS_LEVEL >= 3
        batchSizeGauge_ = profiler->GetOrCreateDoubleAverageGauge(
                this->GetOperatorName() + enjima::metrics::kBatchSizeAverageGaugeSuffix);
#endif
        readyPromise_.set_value();
    }

private:
    void SleepIfNeeded()
    {
        uint64_t currentTimeUs = enjima::runtime::GetSystemTimeMicros();
        if (currentEmittedInMs_ >= maxEmitRatePerMs_) {
            if (nextEmitStartUs_ > currentTimeUs) {
                std::this_thread::sleep_until(
                        enjima::runtime::GetSystemTimePoint<std::chrono::microseconds>(nextEmitStartUs_));
                currentTimeUs = enjima::runtime::GetSystemTimeMicros();
            }
        }
        if (currentTimeUs >= nextEmitStartUs_) {
            nextEmitStartUs_ += 1000;
            currentEmittedInMs_ = 0;
        }
    }

    std::random_device rd;
    std::mt19937 mtEng{rd()};
    std::uniform_int_distribution<int> uniformIntDistribution;
    std::vector<UIDBasedYSBAdEvent> eventCache_;
    std::vector<UIDBasedYSBAdEvent>::const_iterator cacheIterator_;
    UnorderedHashMapST<uint64_t, uint64_t> adIdToCampaignIdMap_;
#if ENJIMA_METRICS_LEVEL >= 3
    enjima::metrics::DoubleAverageGauge* batchSizeGauge_{nullptr};
#endif
    uint64_t numGenEvents_{0};
    uint64_t maxEmitRatePerMs_;
    uint64_t currentEmittedInMs_{0};
    uint64_t nextEmitStartUs_{0};
    std::promise<void> readyPromise_;
    std::thread eventGenThread_{&FixedRateLatencyTrackingUIDBasedYSBSource::GenerateEvents, this};
    std::atomic<bool> genTaskRunning_{true};
    std::atomic<size_t> cachedReadIdx_{0};
    std::atomic<size_t> cachedWriteIdx_{0};
    uint64_t srcReservoirCapacity_;
    UIDBasedYSBAdRecordT* eventReservoir_;
    void* genWriteBufferBeginPtr_{nullptr};
};
#endif//ENJIMA_INPUT_GENERATION_HELPER_H
