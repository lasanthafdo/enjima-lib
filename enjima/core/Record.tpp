namespace enjima::core {
    template<typename T>
    Record<T>::Record() : recordType_(RecordType::kData), timestamp_(0), data_()
    {
    }

    template<typename T>
    Record<T>::Record(uint64_t ts, T data) : recordType_(RecordType::kData), timestamp_(ts), data_(data)
    {
    }

    template<typename T>
    Record<T>::Record(Record::RecordType recordType, uint64_t ts, T data)
        : recordType_(recordType), timestamp_(ts), data_(data)
    {
    }

    template<typename T>
    Record<T>::Record(Record::RecordType recordType, uint64_t ts) : recordType_(recordType), timestamp_(ts), data_(T{})
    {
    }

    template<typename T>
    Record<T>::RecordType Record<T>::GetRecordType() const
    {
        return recordType_;
    }

    template<typename T>
    uint64_t Record<T>::GetTimestamp() const
    {
        return timestamp_;
    }

    template<typename T>
    T Record<T>::GetData() const
    {
        return data_;
    }

#if ENJIMA_METRICS_LEVEL >= 3
    template<typename T>
    Record<T>::Record(Record::RecordType recordType, uint64_t ts, std::vector<uint64_t>* additionalMetricsPtr)
        : recordType_(recordType), timestamp_(ts), data_(T{}), additionalMetricsPtr_(additionalMetricsPtr)
    {
    }

    template<typename T>
    std::vector<uint64_t>* Record<T>::GetAdditionalMetricsPtr() const
    {
        return additionalMetricsPtr_;
    }
#endif

}// namespace enjima::core