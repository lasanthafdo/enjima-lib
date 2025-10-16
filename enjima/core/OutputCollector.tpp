//
// Created by m34ferna on 02/02/24.
//

namespace enjima::core {
    template<class T, class... Args>
    void OutputCollector::Collect(Args&&... args)
    {
        auto currentSysTime = enjima::runtime::GetSystemTimeMillis();
        outputBlk_->Emplace<T>(currentSysTime, std::forward<Args>(args)...);
        outCounter_->IncRelaxed();
    }

    template<class T, class... Args>
    void OutputCollector::CollectWithTimestamp(uint64_t timestamp, Args&&... args)
    {
        outputBlk_->Emplace<T>(timestamp, std::forward<Args>(args)...);
        outCounter_->IncRelaxed();
    }

    template<class T>
    void OutputCollector::Collect(Record<T> outputRecord)
    {
        outputBlk_->Write<T>(outputRecord);
        outCounter_->IncRelaxed();
    }

    template<class T>
    void OutputCollector::CollectBatch(void* dataBuffer, uint32_t numEvents)
    {
        outputBlk_->WriteBatch<T>(dataBuffer, numEvents);
        outCounter_->IncRelaxed(numEvents);
    }
}// namespace enjima::core