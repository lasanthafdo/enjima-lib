//
// Created by m34ferna on 09/01/24.
//

namespace enjima::metrics {

    template<std::unsigned_integral T>
    T Counter<T>::GetCount() const
    {
        return count_;
    }

    template<std::unsigned_integral T>
    const Counter<T> Counter<T>::operator++(int)
    {
        Counter<T> old = *this;
        count_++;
        return old;
    }

    template<std::unsigned_integral T>
    Counter<T>& Counter<T>::operator++()
    {
        count_++;
        return *this;
    }

    template<std::unsigned_integral T>
    void Counter<T>::Inc()
    {
        count_++;
    }

    template<std::unsigned_integral T>
    void Counter<T>::Inc(T delta)
    {
        count_ += delta;
    }
}// namespace enjima::metrics
