//
// Created by m34ferna on 05/03/24.
//

#ifndef ENJIMA_YSB_INTERIM_EVENT_TYPES_H
#define ENJIMA_YSB_INTERIM_EVENT_TYPES_H

#include <cstdint>

namespace enjima::api::data_types {
    class YSBProjectedEvent {
    public:
        YSBProjectedEvent() : timestamp_(0ul), adId_(0ul) {}

        explicit YSBProjectedEvent(uint64_t timestamp, uint64_t adId) : timestamp_(timestamp), adId_(adId) {}

        [[nodiscard]] uint64_t GetTimestamp() const
        {
            return timestamp_;
        }

        [[nodiscard]] uint64_t GetAdId() const
        {
            return adId_;
        }

    private:
        uint64_t timestamp_;
        uint64_t adId_;
    };

    class YSBWinEmitEvent {
    public:
        YSBWinEmitEvent() : timestamp_(0ul), campaignId_(0ul), count_(0ul) {}

        explicit YSBWinEmitEvent(uint64_t timestamp, uint64_t campaignId, uint64_t count)
            : timestamp_(timestamp), campaignId_(campaignId), count_(count)
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

    private:
        uint64_t timestamp_;
        uint64_t campaignId_;
        uint64_t count_;
    };

    class YSBCampaignAdEvent {
    public:
        YSBCampaignAdEvent() : timestamp_(0ul), adId_(0ul), campaignId_(0ul) {}

        explicit YSBCampaignAdEvent(uint64_t timestamp, uint64_t adId, uint64_t campaignId)
            : timestamp_(timestamp), adId_(adId), campaignId_(campaignId)
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

    private:
        uint64_t timestamp_;
        uint64_t adId_;
        uint64_t campaignId_;
    };

    class YSBDummyCampaignAdEvent {
    public:
        YSBDummyCampaignAdEvent() : timestamp_(0ul), adId_(0ul), campaignId_(0ul), count_(0ul) {}

        explicit YSBDummyCampaignAdEvent(uint64_t timestamp, uint64_t adId, uint64_t campaignId, uint64_t count)
            : timestamp_(timestamp), adId_(adId), campaignId_(campaignId), count_(count)
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

        [[nodiscard]] uint64_t GetCount() const
        {
            return count_;
        }

    private:
        uint64_t timestamp_;
        uint64_t adId_;
        uint64_t campaignId_;
        uint64_t count_;
    };

}// namespace enjima::api::data_types


template<>
struct fmt::formatter<enjima::api::data_types::YSBProjectedEvent> : formatter<string_view> {
    // parse is inherited from formatter<string_view>.
    template<typename FormatContext>
    auto format(enjima::api::data_types::YSBProjectedEvent ysbProjectedEvent, FormatContext& ctx)
    {
        std::string desc = "{timestamp: " + std::to_string(ysbProjectedEvent.GetTimestamp()) +
                           ", ad_id: " + std::to_string(ysbProjectedEvent.GetAdId()) + "}";
        return formatter<string_view>::format(desc, ctx);
    }
};

template<>
struct fmt::formatter<enjima::api::data_types::YSBWinEmitEvent> : formatter<string_view> {
    // parse is inherited from formatter<string_view>.
    template<typename FormatContext>
    auto format(enjima::api::data_types::YSBWinEmitEvent ysbWinEvent, FormatContext& ctx)
    {
        std::string desc = "{timestamp: " + std::to_string(ysbWinEvent.GetTimestamp()) +
                           ", campaign_id: " + std::to_string(ysbWinEvent.GetCampaignId()) +
                           ", count: " + std::to_string(ysbWinEvent.GetCount()) + "}";
        return formatter<string_view>::format(desc, ctx);
    }
};

template<>
struct fmt::formatter<enjima::api::data_types::YSBCampaignAdEvent> : formatter<string_view> {
    // parse is inherited from formatter<string_view>.
    template<typename FormatContext>
    auto format(enjima::api::data_types::YSBCampaignAdEvent ysbCampaignAdEvent, FormatContext& ctx)
    {
        std::string desc = "{timestamp: " + std::to_string(ysbCampaignAdEvent.GetTimestamp()) +
                           ", campaign_id: " + std::to_string(ysbCampaignAdEvent.GetCampaignId()) +
                           ", ad_id: " + std::to_string(ysbCampaignAdEvent.GetAdId()) + "}";
        return formatter<string_view>::format(desc, ctx);
    }
};

#endif//ENJIMA_YSB_INTERIM_EVENT_TYPES_H
