//
// Created by m34ferna on 19/02/24.
//

#include "MemoryAllocationCoordinator.h"
#include "MemoryManager.h"
#include "enjima/runtime/RuntimeUtil.h"
#include "spdlog/spdlog.h"

namespace enjima::memory {
    MemoryAllocationCoordinator::MemoryAllocationCoordinator(MemoryManager* pMemoryManager)
        : pMemoryManager_(pMemoryManager)
    {
    }

    void MemoryAllocationCoordinator::Start()
    {
        readyPromise_.set_value();
    }

    void MemoryAllocationCoordinator::Process()
    {
        try {
            readyPromise_.get_future().wait();
            pthread_setname_np(pthread_self(), "mem_alloc_1");
            spdlog::info("Started MemoryAllocationCoordinator");
            while (running_.load(std::memory_order::acquire)) {
                core::StreamingPipeline* pipelinePtr = preAllocationPipelinePtr_.load(std::memory_order::acquire);
                if (pipelinePtr != nullptr) {
                    pMemoryManager_->PreAllocatePipeline(pipelinePtr);
                    preAllocationPipelinePtr_.store(nullptr, std::memory_order::release);
                }
                operators::ChannelID channelId;
                auto requestQNotEmpty = chunkRequestQueue_.try_dequeue(channelId);
                if (requestQNotEmpty) {
                    if (!pMemoryManager_->PreAllocateMemoryChunk(channelId)) {
                        auto currentTime = enjima::runtime::GetSystemTimeMillis();
                        if (currentTime - lastMemoryWarningAt_ > 1000) {
                            spdlog::warn("Memory pre-allocation failed for channel ID {}", channelId);
                            lastMemoryWarningAt_ = currentTime;
                        }
                        chunkRequestQueue_.enqueue(channelId);
                    }
                }
                ChunkID chunkId;
                auto reclaimQNotEmpty = chunkReclamationQueue_.try_dequeue(chunkId);
                if (reclaimQNotEmpty) {
                    if (!pMemoryManager_->ReleaseChunk(chunkId)) {
                        if (!failureLoggedChunkIDs_.contains(chunkId)) {
                            spdlog::warn("Chunk reclamation failed for ChunkID {}", chunkId);
                            failureLoggedChunkIDs_.emplace(chunkId);
                        }
                        chunkReclamationQueue_.enqueue(chunkId);
                    }
                }
                if (!requestQNotEmpty && !reclaimQNotEmpty) {
                    std::unique_lock<std::mutex> lock(cvMutex_);
                    // Observed a bug that causes the memory allocation thread to hang here if a timed_wait
                    // is not employed. Thread is blocked waiting when chunkReclamationQueue_ has about 600-610 elements.
                    // Could observe only in release mode with -O2/-O3 flags. Could not observe with Clang-18.
                    // Possibly due to a compiler bug similar to what is described in
                    // https://stackoverflow.com/questions/78424303/g-optimizes-away-check-for-int-min-in-release-build
                    // Current hacky solution is to have a timed wait that will wake the thread in 1ms if bug triggered.
                    cv_.wait_for(lock, std::chrono::milliseconds(1));
                }
            }
        }
        catch (const std::exception& e) {
            spdlog::error("Memory allocation thread raised exception : {}", e.what());
            memAllocExceptionPtr_ = std::current_exception();
        }
    }

    void MemoryAllocationCoordinator::Stop()
    {
        spdlog::info("Stopping MemoryAllocationCoordinator...");
        running_.store(false, std::memory_order::release);
        operators::OperatorID opId;
        while (chunkRequestQueue_.try_dequeue(opId)) {}
        {
            std::lock_guard<std::mutex> lockGuard{cvMutex_};
        }
        cv_.notify_all();
    }

    bool MemoryAllocationCoordinator::EnqueueChunkAllocationRequest(operators::ChannelID channelId)
    {
        if (chunkRequestQueue_.enqueue(channelId)) {
            {
                std::lock_guard<std::mutex> lockGuard{cvMutex_};
            }
            cv_.notify_all();
            return true;
        }
        return false;
    }

    bool MemoryAllocationCoordinator::EnqueueChunkReclamationRequest(ChunkID chunkId)
    {
        if (chunkId > 1 && chunkReclamationQueue_.enqueue(chunkId)) {
            {
                std::lock_guard<std::mutex> lockGuard{cvMutex_};
            }
            cv_.notify_all();
            return true;
        }
        return false;
    }

    void MemoryAllocationCoordinator::PreAllocatePipeline(core::StreamingPipeline* pipelinePtr)
    {
        core::StreamingPipeline* expectedPipelinePtr = nullptr;
        if (pipelinePtr != nullptr && preAllocationPipelinePtr_.compare_exchange_strong(expectedPipelinePtr,
                                              pipelinePtr, std::memory_order::acq_rel)) {
            // We will only return when the pipeline pre-allocation is complete. However, we need to do the operation
            // using the coordinator thread to avoid data races. Performance is not a concern since this is initialization
            while (preAllocationPipelinePtr_.load(std::memory_order::acquire) != nullptr) {
                {
                    std::lock_guard<std::mutex> lockGuard{cvMutex_};
                }
                cv_.notify_all();
                std::this_thread::yield();
            }
        }
    }

    void MemoryAllocationCoordinator::JoinThread()
    {
        if (running_.load(std::memory_order::acquire)) {
            Stop();
        }
        t_.join();
        assert(!running_.load(std::memory_order::acquire) && !t_.joinable());
        if (memAllocExceptionPtr_ != nullptr) {
            std::rethrow_exception(memAllocExceptionPtr_);
        }
    }
}// namespace enjima::memory
