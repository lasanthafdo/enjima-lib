//
// Created by m34ferna on 09/01/24.
//

namespace enjima::core {
    template<typename T>
        requires std::unsigned_integral<T>
    InstanceID<T>::InstanceID(T instanceID) : instanceID_(instanceID)
    {
    }

    template<typename T>
        requires std::unsigned_integral<T>
    T InstanceID<T>::GetId() const
    {
        return instanceID_;
    }

    template<typename T>
        requires std::unsigned_integral<T>
    bool InstanceID<T>::operator==(const InstanceID<T>& other) const
    {
        return instanceID_ == other.instanceID_;
    }
}// namespace enjima::core
