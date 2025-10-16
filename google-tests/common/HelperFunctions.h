//
// Created by m34ferna on 05/03/24.
//

#ifndef ENJIMA_HELPER_FUNCTIONS_H
#define ENJIMA_HELPER_FUNCTIONS_H

#include "HelperTypes.h"
#include "InputGenerationHelper.h"
#include "enjima/api/CoGroupFunction.h"
#include "enjima/api/JoinFunction.h"
#include "enjima/api/KeyExtractionFunction.h"
#include "enjima/api/KeyedAggregateFunction.h"
#include "enjima/api/MapFunction.h"
#include "enjima/api/MergeableKeyedAggregateFunction.h"
#include "enjima/api/SinkFunction.h"
#include "enjima/api/data_types/YSBInterimEventTypes.h"
#include "spdlog/spdlog.h"

using YSBProjT = enjima::api::data_types::YSBProjectedEvent;
using YSBWinT = enjima::api::data_types::YSBWinEmitEvent;
using YSBCampT = enjima::api::data_types::YSBCampaignAdEvent;
using YSBDummyCampT = enjima::api::data_types::YSBDummyCampaignAdEvent;

class YSBAssertingSinkFunction : public enjima::api::SinkFunction<YSBAdT> {
public:
    void Execute(uint64_t timestamp, YSBAdT inputEvent) override
    {
        assert(inputEvent.GetTimestamp() > 11000 && inputEvent.GetEventType() < 3);
    }
};

class YSBProjectFunction : public enjima::api::MapFunction<YSBAdT, YSBProjT> {
public:
    YSBProjT operator()(const YSBAdT& inputEvent) override
    {
        return YSBProjT(inputEvent.GetTimestamp(), inputEvent.GetAdId());
    }
};

class YSBAdTypeAggFunction : public enjima::api::KeyedAggregateFunction<YSBAdT, YSBWinT> {
private:
    std::vector<YSBWinT> resultVec_;
    UnorderedHashMapST<uint64_t, uint64_t> countMap_;

public:
    void operator()(const YSBAdT& inputEvent) override
    {
        auto adType = inputEvent.GetAdType();
        if (!countMap_.contains(adType)) {
            countMap_.emplace(adType, 0);
        }
        countMap_.at(adType)++;
    }

    std::vector<YSBWinT>& GetResult() override
    {
        if (resultVec_.size() < countMap_.size()) {
            resultVec_.reserve(countMap_.size());
        }
        resultVec_.clear();
        for (const auto& countPair: countMap_) {
            auto campaignId = countPair.first;
            resultVec_.emplace_back(enjima::runtime::GetSystemTimeMillis(), campaignId, countPair.second);
            countMap_.insert_or_assign(campaignId, 0);
        }
        return resultVec_;
    }
};

class YSBCampaignDummyAggFunction : public enjima::api::KeyedAggregateFunction<YSBCampT, YSBWinT> {
private:
    std::vector<YSBWinT> resultVec_;
    UnorderedHashMapST<uint64_t, uint64_t> countMap_;

public:
    void operator()(const YSBCampT& inputEvent) override
    {
        auto campaignId = inputEvent.GetCampaignId();
        if (!countMap_.contains(campaignId)) {
            countMap_.emplace(campaignId, 0);
        }
        countMap_.at(campaignId)++;
    }

    std::vector<YSBWinT>& GetResult() override
    {
        if (resultVec_.size() < countMap_.size()) {
            resultVec_.reserve(countMap_.size());
        }
        resultVec_.clear();
        for (const auto& countPair: countMap_) {
            auto campaignId = countPair.first;
            assert(campaignId > 0 && campaignId <= 1000000);
            resultVec_.emplace_back(enjima::runtime::GetSystemTimeMillis(), campaignId, countPair.second);
            countMap_.insert_or_assign(campaignId, 0);
        }
        return resultVec_;
    }
};

template<>
class enjima::api::MergeableKeyedAggregateFunction<YSBAdT, YSBWinT> : public KeyedAggregateFunction<YSBAdT, YSBWinT> {

    using YSBMergeableAdTypeAggFunction = enjima::api::MergeableKeyedAggregateFunction<YSBAdT, YSBWinT>;

private:
    std::vector<YSBWinT> resultVec_;
    UnorderedHashMapST<long, uint64_t> countMap_;

public:
    void operator()(const YSBAdT& inputEvent) override
    {
        auto adType = inputEvent.GetAdType();
        if (!countMap_.contains(adType)) {
            countMap_.emplace(adType, 0);
        }
        countMap_.at(adType)++;
    }

    std::vector<YSBWinT>& GetResult() override
    {
        if (resultVec_.size() < countMap_.size()) {
            resultVec_.reserve(countMap_.size());
        }
        resultVec_.clear();
        for (const auto& countPair: countMap_) {
            auto adTypeAsCampaignId = countPair.first;
            resultVec_.emplace_back(enjima::runtime::GetSystemTimeMillis(), adTypeAsCampaignId, countPair.second);
        }
        return resultVec_;
    }

    void Reset()
    {
        countMap_.clear();
    }

    void InitializeActivePane(YSBMergeableAdTypeAggFunction& activePane) {}

    void Merge(const std::vector<YSBMergeableAdTypeAggFunction>& vecToMerge)
    {
        for (const auto& functorRef: vecToMerge) {
            for (const auto& kvPair: functorRef.countMap_) {
                auto key = kvPair.first;
                auto value = kvPair.second;
                if (!countMap_.contains(key)) {
                    countMap_.emplace(key, value);
                }
                else {
                    countMap_[key] += value;
                }
            }
        }
    }
};

template<typename T>
class NoOpYSBSinkFunction : public enjima::api::SinkFunction<T> {
public:
    void Execute(uint64_t timestamp, T inputEvent) override {}
};

class NoOpYSBWinSinkFunction : public enjima::api::SinkFunction<YSBWinT> {
public:
    void Execute(uint64_t timestamp, YSBWinT inputEvent) override
    {
        spdlog::info("Received window event {}", inputEvent);
    }
};

class NoOpYSBWinJoinSinkFunction : public enjima::api::SinkFunction<YSBDummyCampT> {
public:
    void Execute(uint64_t timestamp, YSBDummyCampT inputEvent) override {}
};

class YSBKeyExtractFunction : public enjima::api::KeyExtractionFunction<YSBProjT, uint64_t> {
public:
    uint64_t operator()(const YSBProjT& inputEvent) override
    {
        assert(inputEvent.GetAdId() > 0 && inputEvent.GetAdId() <= 1000);
        return inputEvent.GetAdId();
    }
};

class YSBProjTKeyExtractFunction : public enjima::api::KeyExtractionFunction<YSBProjT, uint64_t> {
public:
    uint64_t operator()(const YSBProjT& inputEvent) override
    {
        assert(inputEvent.GetAdId() > 0 && inputEvent.GetAdId() <= 1000);
        return inputEvent.GetAdId();
    }
};

class YSBCampTKeyExtractFunction : public enjima::api::KeyExtractionFunction<YSBCampT, uint64_t> {
public:
    uint64_t operator()(const YSBCampT& inputEvent) override
    {
        assert(inputEvent.GetAdId() > 0 && inputEvent.GetAdId() <= 1000);
        return inputEvent.GetCampaignId();
    }
};

template<>
struct std::hash<std::tuple<uint64_t, uint64_t>> {
    std::size_t operator()(const std::tuple<uint64_t, uint64_t>& k) const
    {
        return (std::hash<enjima::operators::OperatorID>()(get<0>(k)) ^
                       (std::hash<enjima::operators::OperatorID>()(get<1>(k)) << 1)) >>
               1;
    }
};

class YSBCampTMultiKeyExtractFunction
    : public enjima::api::KeyExtractionFunction<YSBCampT, std::tuple<uint64_t, uint64_t>> {
public:
    std::tuple<uint64_t, uint64_t> operator()(const YSBCampT& inputEvent) override
    {
        assert(inputEvent.GetAdId() > 0 && inputEvent.GetAdId() <= 1000);
        return std::make_tuple(inputEvent.GetCampaignId(), inputEvent.GetAdId());
    }
};

class YSBWinTKeyExtractFunction : public enjima::api::KeyExtractionFunction<YSBWinT, uint64_t> {
public:
    uint64_t operator()(const YSBWinT& inputEvent) override
    {
        assert(inputEvent.GetCampaignId() > 0 && inputEvent.GetCampaignId() <= 1000000);
        return inputEvent.GetCampaignId();
    }
};

class YSBWinTMultiKeyExtractFunction
    : public enjima::api::KeyExtractionFunction<YSBWinT, std::tuple<uint64_t, uint64_t>> {
public:
    std::tuple<uint64_t, uint64_t> operator()(const YSBWinT& inputEvent) override
    {
        assert(inputEvent.GetCampaignId() > 0 && inputEvent.GetCampaignId() <= 1000000);
        dummyAdId_ = ((dummyAdId_ + 1) % 1000) + 1;
        return std::make_tuple(inputEvent.GetCampaignId(), dummyAdId_);
    }

private:
    uint64_t dummyAdId_{0};
};

class YSBJoinPredicate : public enjima::api::JoinPredicate<YSBProjT, std::pair<uint64_t, uint64_t>> {
public:
    inline bool operator()(const YSBProjT& leftInputEvent,
            const std::pair<uint64_t, uint64_t>& rightInputEvent) override
    {
        auto adId = leftInputEvent.GetAdId();
        assert(adId > 0 && adId <= 1000);
        return adId == rightInputEvent.first;
    }
};

class YSBJoinFunction : public enjima::api::JoinFunction<YSBProjT, std::pair<uint64_t, uint64_t>, YSBCampT> {
public:
    YSBCampT operator()(const YSBProjT& leftInputEvent, const std::pair<uint64_t, uint64_t>& rightInputEvent) override
    {
        return YSBCampT(leftInputEvent.GetTimestamp(), leftInputEvent.GetAdId(), rightInputEvent.second);
    }
};

class YSBEqJoinFunction : public enjima::api::JoinFunction<YSBProjT, uint64_t, YSBCampT> {
public:
    YSBCampT operator()(const YSBProjT& leftInputEvent, const uint64_t& rightInputEvent) override
    {
        return YSBCampT(leftInputEvent.GetTimestamp(), leftInputEvent.GetAdId(), rightInputEvent);
    }
};

class YSBDummyEqJoinFunction : public enjima::api::JoinFunction<YSBCampT, YSBWinT, YSBDummyCampT> {
public:
    YSBDummyCampT operator()(const YSBCampT& leftInputEvent, const YSBWinT& rightInputEvent) override
    {
        return YSBDummyCampT(leftInputEvent.GetTimestamp(), leftInputEvent.GetAdId(), rightInputEvent.GetCampaignId(),
                rightInputEvent.GetCount());
    }
};

class YSBDummyCoGroupFunction : public enjima::api::CoGroupFunction<YSBCampT, YSBWinT, YSBDummyCampT> {
public:
    void operator()(const std::vector<enjima::core::Record<YSBCampT>>& leftInputEvents,
            const std::vector<enjima::core::Record<YSBWinT>>& rightInputEvents,
            std::queue<YSBDummyCampT>& outputEvents) override
    {
        auto minOut = std::min(leftInputEvents.size(), rightInputEvents.size());
        for (auto i = 0ul; i < minOut; i++) {
            auto leftEvent = leftInputEvents.at(i).GetData();
            auto rightEvent = rightInputEvents.at(i).GetData();
            outputEvents.emplace(leftEvent.GetTimestamp(), leftEvent.GetAdId(), rightEvent.GetCampaignId(),
                    rightEvent.GetCount());
        }
        auto numRemaining = std::max(leftInputEvents.size(), rightInputEvents.size()) - minOut;
        auto numOut = minOut + numRemaining;
        if (leftInputEvents.size() > rightInputEvents.size()) {
            for (auto j = minOut; j < numOut; j++) {
                auto leftEvent = leftInputEvents.at(j).GetData();
                outputEvents.emplace(leftEvent.GetTimestamp(), leftEvent.GetAdId(), 0, 0);
            }
        }
        else if (rightInputEvents.size() > leftInputEvents.size()) {
            for (auto k = minOut; k < numOut; k++) {
                auto rightEvent = rightInputEvents.at(k).GetData();
                outputEvents.emplace(rightEvent.GetTimestamp(), 0, rightEvent.GetCampaignId(), rightEvent.GetCount());
            }
        }
    }
};

class UIDBasedYSBProjectFunction : public enjima::api::MapFunction<UIDBasedYSBAdEvent, UIDBasedYSBProjectedEvent> {
public:
    UIDBasedYSBProjectedEvent operator()(const UIDBasedYSBAdEvent& inputEvent) override
    {
        ++rxCnt_;
        assert(rxCnt_ == inputEvent.GetUniqueIncrementingId());
        return UIDBasedYSBProjectedEvent{inputEvent.GetTimestamp(), inputEvent.GetAdId(),
                inputEvent.GetUniqueIncrementingId()};
    }

private:
    uint64_t rxCnt_{0};
};

class UIDBasedYSBTypedProjectFunction
    : public enjima::api::MapFunction<UIDBasedYSBAdEvent, UIDBasedYSBTypedProjectedEvent> {
public:
    UIDBasedYSBTypedProjectedEvent operator()(const UIDBasedYSBAdEvent& inputEvent) override
    {
        ++rxCnt_;
        assert(rxCnt_ == inputEvent.GetUniqueIncrementingId());
        return UIDBasedYSBTypedProjectedEvent{inputEvent.GetTimestamp(), inputEvent.GetAdId(),
                inputEvent.GetEventType(), inputEvent.GetUniqueIncrementingId()};
    }

private:
    uint64_t rxCnt_{0};
};

class UIDBasedYSBCampaignAggFunction
    : public enjima::api::KeyedAggregateFunction<UIDBasedYSBCampaignAdEvent, UIDBasedYSBWinEmitEvent> {
public:
    void operator()(const UIDBasedYSBCampaignAdEvent& inputEvent) override
    {
        ++rxCnt_;
        auto campaignId = inputEvent.GetCampaignId();
        if (!countMap_.contains(campaignId)) {
            countMap_.emplace(campaignId, 0);
        }
        countMap_.at(campaignId)++;

        assert(rxCnt_ == inputEvent.GetUiId());
    }

    std::vector<UIDBasedYSBWinEmitEvent>& GetResult() override
    {
        if (resultVec_.size() < countMap_.size()) {
            resultVec_.reserve(countMap_.size());
        }
        resultVec_.clear();
        for (const auto& countPair: countMap_) {
            auto campaignId = countPair.first;
            resultVec_.emplace_back(enjima::runtime::GetSystemTimeMillis(), campaignId, countPair.second,
                    ++uiIdWindow_);
            countMap_.insert_or_assign(campaignId, 0);
        }
        return resultVec_;
    }

private:
    std::vector<UIDBasedYSBWinEmitEvent> resultVec_;
    UnorderedHashMapST<uint64_t, uint64_t> countMap_;
    uint64_t uiIdWindow_{0};
    uint64_t rxCnt_{0};
};

template<>
class enjima::api::MergeableKeyedAggregateFunction<UIDBasedYSBCampaignAdEvent, UIDBasedYSBWinEmitEvent>
    : public enjima::api::KeyedAggregateFunction<UIDBasedYSBCampaignAdEvent, UIDBasedYSBWinEmitEvent> {
    using UIDBasedMergeableYSBCampaignAggFunction =
            enjima::api::MergeableKeyedAggregateFunction<UIDBasedYSBCampaignAdEvent, UIDBasedYSBWinEmitEvent>;

public:
    void operator()(const UIDBasedYSBCampaignAdEvent& inputEvent) override
    {
        if ((rxCnt_ + 1) < inputEvent.GetUiId()) {
            rxCnt_ = inputEvent.GetUiId();
        }
        else {
            ++rxCnt_;
        }
        auto campaignId = inputEvent.GetCampaignId();
        if (!countMap_.contains(campaignId)) {
            countMap_.emplace(campaignId, 0);
        }
        countMap_.at(campaignId)++;

        assert(rxCnt_ == inputEvent.GetUiId());
    }

    std::vector<UIDBasedYSBWinEmitEvent>& GetResult() override
    {
        if (resultVec_.size() < countMap_.size()) {
            resultVec_.reserve(countMap_.size());
        }
        resultVec_.clear();
        for (const auto& countPair: countMap_) {
            auto campaignId = countPair.first;
            resultVec_.emplace_back(enjima::runtime::GetSystemTimeMillis(), campaignId, countPair.second,
                    ++uiIdWindow_);
        }
        return resultVec_;
    }

    void Reset()
    {
        countMap_.clear();
    }

    void InitializeActivePane(UIDBasedMergeableYSBCampaignAggFunction& activePane) {}

    void Merge(const std::vector<UIDBasedMergeableYSBCampaignAggFunction>& vecToMerge)
    {
        for (const auto& functorRef: vecToMerge) {
            for (const auto& kvPair: functorRef.countMap_) {
                auto key = kvPair.first;
                auto value = kvPair.second;
                if (!countMap_.contains(key)) {
                    countMap_.emplace(key, value);
                }
                else {
                    countMap_[key] += value;
                }
            }
        }
    }

private:
    std::vector<UIDBasedYSBWinEmitEvent> resultVec_;
    UnorderedHashMapST<uint64_t, uint64_t> countMap_;
    uint64_t uiIdWindow_{0};
    uint64_t rxCnt_{0};
};

class UIDBasedYSBFilteredCampaignAggFunction
    : public enjima::api::KeyedAggregateFunction<UIDBasedYSBFilteredCampaignAdEvent, UIDBasedYSBFilteredWinEmitEvent> {
public:
    explicit UIDBasedYSBFilteredCampaignAggFunction(long eventType) : eventType_(eventType) {}

    void operator()(const UIDBasedYSBFilteredCampaignAdEvent& inputEvent) override
    {
        ++rxCnt_;
        auto campaignId = inputEvent.GetCampaignId();
        if (!countMap_.contains(campaignId)) {
            countMap_.emplace(campaignId, 0);
        }
        countMap_.at(campaignId)++;

        assert(rxCnt_ == inputEvent.GetUiId());
    }

    std::vector<UIDBasedYSBFilteredWinEmitEvent>& GetResult() override
    {
        if (resultVec_.size() < countMap_.size()) {
            resultVec_.reserve(countMap_.size());
        }
        resultVec_.clear();
        for (const auto& countPair: countMap_) {
            auto campaignId = countPair.first;
            resultVec_.emplace_back(enjima::runtime::GetSystemTimeMillis(), campaignId, eventType_, countPair.second,
                    ++uiIdWindow_);
            countMap_.insert_or_assign(campaignId, 0);
        }
        return resultVec_;
    }

private:
    std::vector<UIDBasedYSBFilteredWinEmitEvent> resultVec_;
    UnorderedHashMapST<uint64_t, uint64_t> countMap_;
    uint64_t uiIdWindow_{0};
    uint64_t rxCnt_{0};
    long eventType_;
};

template<typename T>
class UIDBasedNoOpSinkFunction : public enjima::api::SinkFunction<T> {
public:
    void Execute(uint64_t timestamp, T inputEvent) override
    {
        rxCnt_++;
        assert(rxCnt_ == inputEvent.GetUiId());
        if (rxCnt_ % 1'000'000 == 0) {
            spdlog::info("Received sink event {}", inputEvent);
            auto latency = enjima::runtime::GetSystemTimeMillis() - timestamp;
            spdlog::info("Latency (ms): {}", latency);
        }
    }

private:
    uint64_t rxCnt_{0};
};

class UIDBasedYSBAdIdExtractFunction : public enjima::api::KeyExtractionFunction<UIDBasedYSBProjectedEvent, uint64_t> {
public:
    uint64_t operator()(const UIDBasedYSBProjectedEvent& inputEvent) override
    {
        return inputEvent.GetAdId();
    }
};

class UIDBasedYSBTypedAdIdExtractFunction
    : public enjima::api::KeyExtractionFunction<UIDBasedYSBTypedProjectedEvent, uint64_t> {
public:
    uint64_t operator()(const UIDBasedYSBTypedProjectedEvent& inputEvent) override
    {
        return inputEvent.GetAdId();
    }
};

class UIDBasedYSBCampaignIdExtractFunction
    : public enjima::api::KeyExtractionFunction<UIDBasedYSBWinEmitEvent, uint64_t> {
public:
    uint64_t operator()(const UIDBasedYSBWinEmitEvent& inputEvent) override
    {
        return inputEvent.GetCampaignId();
    }
};

class UIDBasedYSBFilteredCampaignIdExtractFunction
    : public enjima::api::KeyExtractionFunction<UIDBasedYSBFilteredWinEmitEvent, uint64_t> {
public:
    uint64_t operator()(const UIDBasedYSBFilteredWinEmitEvent& inputEvent) override
    {
        return inputEvent.GetCampaignId();
    }
};

class UIDBasedYSBEqJoinFunction
    : public enjima::api::JoinFunction<UIDBasedYSBProjectedEvent, uint64_t, UIDBasedYSBCampaignAdEvent> {
public:
    UIDBasedYSBCampaignAdEvent operator()(const UIDBasedYSBProjectedEvent& leftInputEvent,
            const uint64_t& rightInputEvent) override
    {
        return UIDBasedYSBCampaignAdEvent(leftInputEvent.GetTimestamp(), leftInputEvent.GetAdId(), rightInputEvent,
                leftInputEvent.GetUiId());
    }
};

class UIDBasedYSBTypedEqJoinFunction
    : public enjima::api::JoinFunction<UIDBasedYSBTypedProjectedEvent, uint64_t, UIDBasedYSBCampaignAdEvent> {
public:
    UIDBasedYSBCampaignAdEvent operator()(const UIDBasedYSBTypedProjectedEvent& leftInputEvent,
            const uint64_t& rightInputEvent) override
    {
        return UIDBasedYSBCampaignAdEvent(leftInputEvent.GetTimestamp(), leftInputEvent.GetAdId(), rightInputEvent,
                leftInputEvent.GetUiId());
    }
};

class UIDBasedYSBFilteredEqJoinFunction
    : public enjima::api::JoinFunction<UIDBasedYSBTypedProjectedEvent, uint64_t, UIDBasedYSBFilteredCampaignAdEvent> {
public:
    explicit UIDBasedYSBFilteredEqJoinFunction(long eventType) : eventType_(eventType) {}

    UIDBasedYSBFilteredCampaignAdEvent operator()(const UIDBasedYSBTypedProjectedEvent& leftInputEvent,
            const uint64_t& rightInputEvent) override
    {
        return UIDBasedYSBFilteredCampaignAdEvent(leftInputEvent.GetTimestamp(), leftInputEvent.GetAdId(),
                rightInputEvent, eventType_, leftInputEvent.GetUiId());
    }

private:
    long eventType_;
};

class UIDBasedYSBWindowJoinFunction : public enjima::api::JoinFunction<UIDBasedYSBWinEmitEvent,
                                              UIDBasedYSBFilteredWinEmitEvent, UIDBasedYSBWinJoinEmitEvent> {
public:
    UIDBasedYSBWinJoinEmitEvent operator()(const UIDBasedYSBWinEmitEvent& leftInputEvent,
            const UIDBasedYSBFilteredWinEmitEvent& rightInputEvent) override
    {
        return UIDBasedYSBWinJoinEmitEvent(leftInputEvent.GetTimestamp(), leftInputEvent.GetCampaignId(),
                leftInputEvent.GetCount(), rightInputEvent.GetCount(), ++uiIdWindowJoin_);
    }

private:
    uint64_t uiIdWindowJoin_{0};
};

class UIDBasedYSBWindowCoGroupFunction : public enjima::api::CoGroupFunction<UIDBasedYSBWinEmitEvent,
                                                 UIDBasedYSBFilteredWinEmitEvent, UIDBasedYSBWinJoinEmitEvent> {
public:
    void operator()(const std::vector<enjima::core::Record<UIDBasedYSBWinEmitEvent>>& leftInputEvents,
            const std::vector<enjima::core::Record<UIDBasedYSBFilteredWinEmitEvent>>& rightInputEvents,
            std::queue<UIDBasedYSBWinJoinEmitEvent>& outputEvents) override
    {
        auto minOut = std::min(leftInputEvents.size(), rightInputEvents.size());
        for (auto i = 0ul; i < minOut; i++) {
            auto leftInputEvent = leftInputEvents.at(i).GetData();
            auto rightInputEvent = rightInputEvents.at(i).GetData();
            outputEvents.emplace(leftInputEvent.GetTimestamp(), leftInputEvent.GetCampaignId(),
                    leftInputEvent.GetCount(), rightInputEvent.GetCount(), ++uiIdWindowJoin_);
        }
    }

private:
    uint64_t uiIdWindowJoin_{0};
};

class ValidatingSinkFunction : public enjima::api::SinkFunction<LinearRoadT> {
public:
    void Execute(uint64_t timestamp, enjima::api::data_types::LinearRoadEvent inputEvent) override
    {
        assert(inputEvent.GetTimestamp() < 11000 && inputEvent.GetDow() <= 7);
    }
};

#endif//ENJIMA_HELPER_FUNCTIONS_H
