namespace enjima::memory {
    template<typename T>
    core::Record<T>* MemoryBlock::Read()
    {
        if (CanRead()) {
            auto pRead = pRead_.load(std::memory_order::acquire);
            auto* pNextRecord = static_cast<core::Record<T>*>(pRead);
            pRead_.store(pNextRecord + 1, std::memory_order::release);
            return pNextRecord;
        }
        return nullptr;
    }

    template<typename T>
    uint32_t MemoryBlock::ReadBatch(void* inputBuffer, uint32_t numRecordsToRead)
    {
        auto bytesToRead = numRecordsToRead * sizeof(core::Record<T>);
        auto pRead = pRead_.load(std::memory_order::acquire);
        auto pWrite = pWrite_.load(std::memory_order::acquire);
        uint32_t numRecordsRead = 0;
        void* ptrAfterRead = (static_cast<char*>(pRead) + bytesToRead);
        assert(pWrite <= pEnd_ && numRecordsToRead <= GetNumReadableEvents<T>());
        if (pWrite >= ptrAfterRead) {
            std::memcpy(inputBuffer, pRead, bytesToRead);
            pRead_.store(ptrAfterRead, std::memory_order::release);
            numRecordsRead = bytesToRead / sizeof(core::Record<T>);
        }
        return numRecordsRead;
    }

    template<typename T>
    void MemoryBlock::Write(const core::Record<T>& record)
    {
        if (CanWrite()) {
            auto pWrite = pWrite_.load(std::memory_order::acquire);
            new (pWrite) core::Record<T>(record);
            pWrite_.store(static_cast<core::Record<T>*>(pWrite) + 1, std::memory_order::release);
        }
    }

    template<typename T>
    void MemoryBlock::WriteBatch(void* outputBuffer, uint32_t numRecordsToWrite)
    {
        auto bytesToWrite = numRecordsToWrite * sizeof(core::Record<T>);
        auto pWrite = pWrite_.load(std::memory_order::acquire);
        assert((static_cast<char*>(pWrite) + bytesToWrite) <= static_cast<char*>(pEnd_));
        if ((static_cast<char*>(pWrite) + bytesToWrite) <= static_cast<char*>(pEnd_)) {
#if ENJIMA_MEM_CPY_METHOD == 1
            enjima::memory::memcpy_avx_unaligned(pWrite, outputBuffer, bytesToWrite);
#elif ENJIMA_MEM_CPY_METHOD == 2
            enjima::memory::memcpy_avx_nt(pWrite, outputBuffer, bytesToWrite);
#elif ENJIMA_MEM_CPY_METHOD == 3
            enjima::memory::memcpy_sse2(pWrite, outputBuffer, bytesToWrite);
#elif ENJIMA_MEM_CPY_METHOD == 4
            enjima::memory::memcpy_avx_unroll(pWrite, outputBuffer, bytesToWrite);
#else
            std::memcpy(pWrite, outputBuffer, bytesToWrite);
#endif
            pWrite_.store(static_cast<char*>(pWrite) + bytesToWrite, std::memory_order::release);
        }
    }

    template<typename T>
    uint32_t MemoryBlock::GetNumWritableEvents() const
    {
        auto writableBytes = static_cast<char*>(pEnd_) - static_cast<char*>(pWrite_.load(std::memory_order::acquire));
        auto numWritableEvents = writableBytes / sizeof(core::Record<T>);
        return numWritableEvents;
    }

    template<typename T>
    uint32_t MemoryBlock::GetNumReadableEvents() const
    {
        auto readableBytes = static_cast<char*>(pWrite_.load(std::memory_order::acquire)) -
                             static_cast<char*>(pRead_.load(std::memory_order::acquire));
        auto numReadableEvents = readableBytes / sizeof(core::Record<T>);
        return numReadableEvents;
    }

    template<typename T>
    uint32_t MemoryBlock::GetTotalEventCapacity() const
    {
        auto totalWritableBytes = static_cast<char*>(pEnd_) - static_cast<char*>(pData_);
        auto totalWritableEvents = totalWritableBytes / sizeof(core::Record<T>);
        return totalWritableEvents;
    }

    template<class T, class... Args>
    void MemoryBlock::Emplace(Args&&... args)
    {
        if (CanWrite()) {
            auto pWrite = pWrite_.load(std::memory_order::acquire);
            new (pWrite) core::Record<T>(std::forward<Args>(args)...);
            pWrite_.store(static_cast<core::Record<T>*>(pWrite) + 1, std::memory_order::release);
        }
    }
}// namespace enjima::memory