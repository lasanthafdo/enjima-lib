//
// Created by m34ferna on 15/01/24.
//

#ifndef ENJIMA_HELPER_TYPES_H
#define ENJIMA_HELPER_TYPES_H

#include "spdlog/fmt/fmt.h"
#include <cstdint>

class UIDBasedYSBAdEvent {
public:
    UIDBasedYSBAdEvent()
        : timestamp_(0), userId_(0), pageId_(0), adId_(0), adType_(0), eventType_(0), ipAddress_(0),
          uniqueIncrementingId_(0)
    {
    }

    UIDBasedYSBAdEvent(uint64_t timestamp, uint64_t userId, uint64_t pageId, uint64_t adId, long adType, long eventType,
            long ipAddress, uint64_t uiId)
        : timestamp_(timestamp), userId_(userId), pageId_(pageId), adId_(adId), adType_(adType), eventType_(eventType),
          ipAddress_(ipAddress), uniqueIncrementingId_(uiId)
    {
    }

    [[nodiscard]] uint64_t GetTimestamp() const
    {
        return timestamp_;
    }

    [[nodiscard]] uint64_t GetUserId() const
    {
        return userId_;
    }

    [[nodiscard]] uint64_t GetPageId() const
    {
        return pageId_;
    }

    [[nodiscard]] uint64_t GetAdId() const
    {
        return adId_;
    }

    [[nodiscard]] long GetAdType() const
    {
        return adType_;
    }

    [[nodiscard]] long GetEventType() const
    {
        return eventType_;
    }

    [[nodiscard]] long GetIpAddress() const
    {
        return ipAddress_;
    }

    [[nodiscard]] uint64_t GetUniqueIncrementingId() const
    {
        return uniqueIncrementingId_;
    }

    void SetTimestamp(uint64_t timestamp)
    {
        timestamp_ = timestamp;
    }

    void SetUniqueIncrementingId(uint64_t uniqueIncrementingId)
    {
        uniqueIncrementingId_ = uniqueIncrementingId;
    }

private:
    uint64_t timestamp_;
    uint64_t userId_;
    uint64_t pageId_;
    uint64_t adId_;
    long adType_;
    long eventType_;
    long ipAddress_;
    uint64_t uniqueIncrementingId_;
};

template<>
struct fmt::formatter<UIDBasedYSBAdEvent> : formatter<string_view> {
    // parse is inherited from formatter<string_view>.
    template<typename FormatContext>
    auto format(UIDBasedYSBAdEvent ysbAdEvent, FormatContext& ctx)
    {
        std::string desc = "{event_type: " + std::to_string(ysbAdEvent.GetEventType()) +
                           ", timestamp: " + std::to_string(ysbAdEvent.GetTimestamp()) +
                           ", uiId: " + std::to_string(ysbAdEvent.GetUniqueIncrementingId()) +
                           ", ad_type: " + std::to_string(ysbAdEvent.GetAdType()) +
                           ", user_id: " + std::to_string(ysbAdEvent.GetUserId()) + "}";
        return formatter<string_view>::format(desc, ctx);
    }
};

class UIDBasedYSBProjectedEvent {
public:
    UIDBasedYSBProjectedEvent() : timestamp_(0), adId_(0), uiId_(0) {}

    explicit UIDBasedYSBProjectedEvent(uint64_t timestamp, uint64_t adId, uint64_t uiId)
        : timestamp_(timestamp), adId_(adId), uiId_(uiId)
    {
    }

    [[nodiscard]] uint64_t GetTimestamp() const
    {
        return timestamp_;
    }

    [[nodiscard]] uint64_t GetAdId() const
    {
        return adId_;
    }

    [[nodiscard]] uint64_t GetUiId() const
    {
        return uiId_;
    }

private:
    uint64_t timestamp_;
    uint64_t adId_;
    uint64_t uiId_;
};

class UIDBasedYSBTypedProjectedEvent {
public:
    UIDBasedYSBTypedProjectedEvent() : timestamp_(0), adId_(0), eventType_(0), uiId_(0) {}

    explicit UIDBasedYSBTypedProjectedEvent(uint64_t timestamp, uint64_t adId, long eventType, uint64_t uiId)
        : timestamp_(timestamp), adId_(adId), eventType_(eventType), uiId_(uiId)
    {
    }

    [[nodiscard]] uint64_t GetTimestamp() const
    {
        return timestamp_;
    }

    [[nodiscard]] uint64_t GetAdId() const
    {
        return adId_;
    }

    [[nodiscard]] uint64_t GetUiId() const
    {
        return uiId_;
    }

    [[nodiscard]] long GetEventType() const
    {
        return eventType_;
    }

private:
    uint64_t timestamp_;
    uint64_t adId_;
    long eventType_;
    uint64_t uiId_;
};

class UIDBasedYSBCampaignAdEvent {
public:
    UIDBasedYSBCampaignAdEvent() : timestamp_(0), adId_(0), campaignId_(0), uiId_(0) {}

    explicit UIDBasedYSBCampaignAdEvent(uint64_t timestamp, uint64_t adId, uint64_t campaignId, uint64_t uiId)
        : timestamp_(timestamp), adId_(adId), campaignId_(campaignId), uiId_(uiId)
    {
    }

    [[nodiscard]] uint64_t GetTimestamp() const
    {
        return timestamp_;
    }

    [[nodiscard]] uint64_t GetAdId() const
    {
        return adId_;
    }

    [[nodiscard]] uint64_t GetCampaignId() const
    {
        return campaignId_;
    }

    [[nodiscard]] uint64_t GetUiId() const
    {
        return uiId_;
    }

private:
    uint64_t timestamp_;
    uint64_t adId_;
    uint64_t campaignId_;
    uint64_t uiId_;
};

template<>
struct fmt::formatter<UIDBasedYSBCampaignAdEvent> : formatter<string_view> {
    // parse is inherited from formatter<string_view>.
    template<typename FormatContext>
    auto format(UIDBasedYSBCampaignAdEvent campaignAdEvent, FormatContext& ctx)
    {
        std::string desc = "{timestamp: " + std::to_string(campaignAdEvent.GetTimestamp()) +
                           ", ui_id: " + std::to_string(campaignAdEvent.GetUiId()) +
                           ", campaign_id: " + std::to_string(campaignAdEvent.GetCampaignId()) +
                           ", ad_id: " + std::to_string(campaignAdEvent.GetAdId()) + "}";
        return formatter<string_view>::format(desc, ctx);
    }
};

class UIDBasedYSBFilteredCampaignAdEvent {
public:
    UIDBasedYSBFilteredCampaignAdEvent() : timestamp_(0), adId_(0), campaignId_(0), eventType_(0), uiId_(0) {}

    explicit UIDBasedYSBFilteredCampaignAdEvent(uint64_t timestamp, uint64_t adId, uint64_t campaignId, long eventType,
            uint64_t uiId)
        : timestamp_(timestamp), adId_(adId), campaignId_(campaignId), eventType_(eventType), uiId_(uiId)
    {
    }

    [[nodiscard]] uint64_t GetTimestamp() const
    {
        return timestamp_;
    }

    [[nodiscard]] uint64_t GetAdId() const
    {
        return adId_;
    }

    [[nodiscard]] uint64_t GetCampaignId() const
    {
        return campaignId_;
    }

    [[nodiscard]] long GetEventType() const
    {
        return eventType_;
    }

    [[nodiscard]] uint64_t GetUiId() const
    {
        return uiId_;
    }

private:
    uint64_t timestamp_;
    uint64_t adId_;
    uint64_t campaignId_;
    long eventType_;
    uint64_t uiId_;
};

class UIDBasedYSBWinEmitEvent {
public:
    UIDBasedYSBWinEmitEvent() : timestamp_(0ul), campaignId_(0ul), count_(0ul), uiId_(0) {}

    explicit UIDBasedYSBWinEmitEvent(uint64_t timestamp, uint64_t campaignId, uint64_t count, uint64_t uiId)
        : timestamp_(timestamp), campaignId_(campaignId), count_(count), uiId_(uiId)
    {
    }

    [[nodiscard]] uint64_t GetTimestamp() const
    {
        return timestamp_;
    }

    [[nodiscard]] uint64_t GetCampaignId() const
    {
        return campaignId_;
    }

    [[nodiscard]] uint64_t GetCount() const
    {
        return count_;
    }

    [[nodiscard]] uint64_t GetUiId() const
    {
        return uiId_;
    }

private:
    uint64_t timestamp_;
    uint64_t campaignId_;
    uint64_t count_;
    uint64_t uiId_;
};

template<>
struct fmt::formatter<UIDBasedYSBWinEmitEvent> : formatter<string_view> {
    // parse is inherited from formatter<string_view>.
    template<typename FormatContext>
    auto format(UIDBasedYSBWinEmitEvent winEvent, FormatContext& ctx)
    {
        std::string desc = "{timestamp: " + std::to_string(winEvent.GetTimestamp()) +
                           ", ui_id: " + std::to_string(winEvent.GetUiId()) +
                           ", campaign_id: " + std::to_string(winEvent.GetCampaignId()) +
                           ", count: " + std::to_string(winEvent.GetCount()) + "}";
        return formatter<string_view>::format(desc, ctx);
    }
};

class UIDBasedYSBFilteredWinEmitEvent {
public:
    UIDBasedYSBFilteredWinEmitEvent() : timestamp_(0ul), campaignId_(0ul), eventType_(0), count_(0ul), uiId_(0) {}

    explicit UIDBasedYSBFilteredWinEmitEvent(uint64_t timestamp, uint64_t campaignId, long eventType, uint64_t count,
            uint64_t uiId)
        : timestamp_(timestamp), campaignId_(campaignId), eventType_(eventType), count_(count), uiId_(uiId)
    {
    }

    [[nodiscard]] uint64_t GetTimestamp() const
    {
        return timestamp_;
    }

    [[nodiscard]] uint64_t GetCampaignId() const
    {
        return campaignId_;
    }

    [[nodiscard]] long GetEventType() const
    {
        return eventType_;
    }

    [[nodiscard]] uint64_t GetCount() const
    {
        return count_;
    }

    [[nodiscard]] uint64_t GetUiId() const
    {
        return uiId_;
    }

private:
    uint64_t timestamp_;
    uint64_t campaignId_;
    long eventType_;
    uint64_t count_;
    uint64_t uiId_;
};

class UIDBasedYSBWinJoinEmitEvent {
public:
    UIDBasedYSBWinJoinEmitEvent() : timestamp_(0ul), campaignId_(0ul), fullCount_(0ul), filteredCount_(0ul), uiId_(0) {}

    explicit UIDBasedYSBWinJoinEmitEvent(uint64_t timestamp, uint64_t campaignId, uint64_t fullCount,
            uint64_t filteredCount, uint64_t uiId)
        : timestamp_(timestamp), campaignId_(campaignId), fullCount_(fullCount), filteredCount_(filteredCount),
          uiId_(uiId)
    {
    }

    [[nodiscard]] uint64_t GetTimestamp() const
    {
        return timestamp_;
    }

    [[nodiscard]] uint64_t GetCampaignId() const
    {
        return campaignId_;
    }

    [[nodiscard]] uint64_t GetFullCount() const
    {
        return fullCount_;
    }

    [[nodiscard]] uint64_t GetFilteredCount() const
    {
        return filteredCount_;
    }

    [[nodiscard]] uint64_t GetUiId() const
    {
        return uiId_;
    }

private:
    uint64_t timestamp_;
    uint64_t campaignId_;
    uint64_t fullCount_;
    uint64_t filteredCount_;
    uint64_t uiId_;
};

template<>
struct fmt::formatter<UIDBasedYSBWinJoinEmitEvent> : formatter<string_view> {
    // parse is inherited from formatter<string_view>.
    template<typename FormatContext>
    auto format(UIDBasedYSBWinJoinEmitEvent winJoinEvent, FormatContext& ctx)
    {
        std::string desc = "{timestamp: " + std::to_string(winJoinEvent.GetTimestamp()) +
                           ", ui_id: " + std::to_string(winJoinEvent.GetUiId()) +
                           ", campaign_id: " + std::to_string(winJoinEvent.GetCampaignId()) +
                           ", filtered_count: " + std::to_string(winJoinEvent.GetFilteredCount()) +
                           ", full_count: " + std::to_string(winJoinEvent.GetFullCount()) + "}";
        return formatter<string_view>::format(desc, ctx);
    }
};

#endif//ENJIMA_HELPER_TYPES_H
