//
// Created by m34ferna on 09/01/24.
//

namespace enjima::metrics {

    template<typename T>
        requires std::is_arithmetic_v<T>
    Gauge<T>::Gauge(T initialVal)
    {
        val_ = initialVal;
    }

    template<typename T>
        requires std::is_arithmetic_v<T>
    T Gauge<T>::GetVal()
    {
        return val_;
    }

    template<typename T>
        requires std::is_arithmetic_v<T>
    void Gauge<T>::UpdateVal(T val)
    {
        val_ = val;
    }

}// namespace enjima::metrics
