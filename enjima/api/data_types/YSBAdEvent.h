//
// Created by m34ferna on 15/01/24.
//

#ifndef ENJIMA_YSB_AD_EVENT_H
#define ENJIMA_YSB_AD_EVENT_H

#include "spdlog/fmt/fmt.h"

namespace enjima::api::data_types {

    class YSBAdEvent {
    public:
        YSBAdEvent() : timestamp_(0), userId_(0), pageId_(0), adId_(0), adType_(0), eventType_(0), ipAddress_(0) {}

        YSBAdEvent(uint64_t timestamp, uint64_t userId, uint64_t pageId, uint64_t adId, long adType, long eventType,
                long ipAddress)
            : timestamp_(timestamp), userId_(userId), pageId_(pageId), adId_(adId), adType_(adType),
              eventType_(eventType), ipAddress_(ipAddress)
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

    private:
        uint64_t timestamp_;
        uint64_t userId_;
        uint64_t pageId_;
        uint64_t adId_;
        long adType_;
        long eventType_;
        long ipAddress_;
        long _padding{0};
    };

}// namespace enjima::api::data_types

template<>
struct fmt::formatter<enjima::api::data_types::YSBAdEvent> : formatter<string_view> {
    // parse is inherited from formatter<string_view>.
    template<typename FormatContext>
    auto format(enjima::api::data_types::YSBAdEvent ysbAdEvent, FormatContext& ctx)
    {
        std::string desc = "{event_type: " + std::to_string(ysbAdEvent.GetEventType()) +
                           ", timestamp: " + std::to_string(ysbAdEvent.GetTimestamp()) +
                           ", ad_type: " + std::to_string(ysbAdEvent.GetAdType()) +
                           ", user_id: " + std::to_string(ysbAdEvent.GetUserId()) + "}";
        return formatter<string_view>::format(desc, ctx);
    }
};

#endif//ENJIMA_YSB_AD_EVENT_H
