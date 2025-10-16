//
// Created by m34ferna on 15/01/24.
//

#ifndef ENJIMA_LINEAR_ROAD_EVENT_H
#define ENJIMA_LINEAR_ROAD_EVENT_H

#include "spdlog/fmt/fmt.h"

namespace enjima::api::data_types {

    class LinearRoadEvent {
    public:
        LinearRoadEvent()
            : m_iTime(0), m_iType(0), m_iVid(0), m_iSpeed(0), m_iXway(0), m_iLane(0), m_iDir(0), m_iSeg(0), m_iPos(0),
              m_iQid(0), m_iSinit(0), m_iSend(0), m_iDow(0), m_iTod(0), m_iDay(0)
        {
        }

        LinearRoadEvent(int type, uint64_t time, int vid, int speed, int xWay, int lane, int dir, int seg, int pos,
                int qId, int sInit, int sEnd, int dayOfWeek, int timeOfDayMinutes, int day)
            : m_iTime(time), m_iType(type), m_iVid(vid), m_iSpeed(speed), m_iXway(xWay), m_iLane(lane), m_iDir(dir),
              m_iSeg(seg), m_iPos(pos), m_iQid(qId), m_iSinit(sInit), m_iSend(sEnd), m_iDow(dayOfWeek),
              m_iTod(timeOfDayMinutes), m_iDay(day)
        {
        }

        void SetTimestamp(uint64_t time)
        {
            m_iTime = time;
        }

        [[nodiscard]] uint64_t GetTimestamp() const
        {
            return m_iTime;
        }

        [[nodiscard]] int GetType() const
        {
            return m_iType;
        }

        [[nodiscard]] int GetVid() const
        {
            return m_iVid;
        }

        [[nodiscard]] int GetSpeed() const
        {
            return m_iSpeed;
        }

        [[nodiscard]] int GetXway() const
        {
            return m_iXway;
        }

        [[nodiscard]] int GetLane() const
        {
            return m_iLane;
        }

        [[nodiscard]] int GetDir() const
        {
            return m_iDir;
        }

        [[nodiscard]] int GetSeg() const
        {
            return m_iSeg;
        }

        [[nodiscard]] int GetPos() const
        {
            return m_iPos;
        }

        [[nodiscard]] int GetQid() const
        {
            return m_iQid;
        }

        [[nodiscard]] int GetSinit() const
        {
            return m_iSinit;
        }

        [[nodiscard]] int GetSend() const
        {
            return m_iSend;
        }

        [[nodiscard]] int GetDow() const
        {
            return m_iDow;
        }

        [[nodiscard]] int GetTod() const
        {
            return m_iTod;
        }

        [[nodiscard]] int GetDay() const
        {
            return m_iDay;
        }

    private:
        uint64_t m_iTime{
                0};// 0...10799 (second), timestamp position report emitted (will be set to milliseconds by source operator)
        int m_iType{0}; // Type:
                        //	. 0: position report
                        //	. 2: account balance request
                        //	. 3: daily expenditure request
                        //	. 4: travel time request
        int m_iVid;     // 0...MAXINT, vehicle identifier
        int m_iSpeed{0};// 0...100, speed of the vehicle
        int m_iXway;    // 0...L-1, express way
        int m_iLane;    // 0...4, lane
        int m_iDir;     // 0..1, direction
        int m_iSeg;     // 0...99, segment
        int m_iPos;     // 0...527999, position of the vehicle
        int m_iQid;     // query identifier
        int m_iSinit;   // start segment
        int m_iSend;    // end segment
        int m_iDow;     // 1..7, day of week
        int m_iTod;     // 1...1440, minute number in the day
        int m_iDay;     // 1..69, 1: yesterday, 69: 10 weeks ago
    };

}// namespace enjima::api::data_types

template<>
struct fmt::formatter<enjima::api::data_types::LinearRoadEvent> : formatter<string_view> {
    // parse is inherited from formatter<string_view>.
    template<typename FormatContext>
    auto format(enjima::api::data_types::LinearRoadEvent linearRoadEvent, FormatContext& ctx)
    {
        std::string desc = "{type: " + std::to_string(linearRoadEvent.GetType()) +
                           ", time: " + std::to_string(linearRoadEvent.GetTimestamp()) +
                           ", speed: " + std::to_string(linearRoadEvent.GetSpeed()) +
                           ", day_of_week: " + std::to_string(linearRoadEvent.GetDow()) + "}";
        return formatter<string_view>::format(desc, ctx);
    }
};

#endif//ENJIMA_LINEAR_ROAD_EVENT_H
