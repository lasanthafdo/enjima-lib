//
// Created by m34ferna on 10/06/24.
//

#ifndef ENJIMA_TYPE_ALIASES_H
#define ENJIMA_TYPE_ALIASES_H

#include "moodycamel/concurrentqueue.h"
#include "oneapi/tbb/concurrent_hash_map.h"
#include "oneapi/tbb/concurrent_queue.h"
#include "oneapi/tbb/concurrent_unordered_map.h"
#include "oneapi/tbb/concurrent_unordered_set.h"
#include "oneapi/tbb/concurrent_vector.h"
#include "rigtorp/SPSCQueue.h"
#include "xenium/reclamation/generic_epoch_based.hpp"
#include "xenium/vyukov_hash_map.hpp"

#include <unordered_map>

template<typename T>
using MPMCQueue = moodycamel::ConcurrentQueue<T>;

template<typename Key, typename T, typename HashFunc = std::hash<Key>>
using UnorderedHashMapST = std::unordered_map<Key, T, HashFunc>;

template<typename Key, typename T>
using ConcurrentUnorderedHashMap =
        xenium::vyukov_hash_map<Key, T, xenium::policy::reclaimer<xenium::reclamation::debra<>>>;

template<typename Key, typename T>
using ConcurrentUnorderedMapTBB = oneapi::tbb::concurrent_unordered_map<Key, T>;

template<typename Key, typename T>
using ConcurrentHashMapTBB = oneapi::tbb::concurrent_hash_map<Key, T>;

template<typename T>
using ConcurrentVectorTBB = oneapi::tbb::concurrent_vector<T>;

template<typename T>
using ConcurrentUnorderedSetTBB = oneapi::tbb::concurrent_unordered_set<T>;

template<typename T>
using ConcurrentBoundedQueueTBB = oneapi::tbb::concurrent_bounded_queue<T>;

#endif//ENJIMA_TYPE_ALIASES_H
