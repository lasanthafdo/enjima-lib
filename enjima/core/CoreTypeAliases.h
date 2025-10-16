//
// Created by m34ferna on 02/02/24.
//

#ifndef ENJIMA_CORE_TYPE_ALIASES_H
#define ENJIMA_CORE_TYPE_ALIASES_H

#include "InstanceID.h"
#include <cstdint>

namespace enjima::core {
    using JobID = enjima::core::InstanceID<uint64_t>;
}

#endif//ENJIMA_CORE_TYPE_ALIASES_H
