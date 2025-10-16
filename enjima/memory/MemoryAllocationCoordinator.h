//
// Created by m34ferna on 19/02/24.
//

#ifndef ENJIMA_MEMORY_ALLOCATION_COORDINATOR_H
#define ENJIMA_MEMORY_ALLOCATION_COORDINATOR_H

#include "MemoryManager.fwd.h"
#include "MemoryTypeAliases.h"
#include "enjima/common/TypeAliases.h"
#include "enjima/core/CoreInternals.fwd.h"
#include "enjima/operators/OperatorsTypeAliases.h"

#include <future>
#include <queue>
#include <unordered_set>

namespace enjima::memory {

    class MemoryAllocationCoordinator {
    public:
        explicit MemoryAllocationCoordinator(MemoryManager* pMemoryManager);
        void Start();
        void Stop();
        void Process();
        void JoinThread();
        bool EnqueueChunkAllocationRequest(operators::ChannelID channelId);
        bool EnqueueChunkReclamationRequest(ChunkID chunkId);
        void PreAllocatePipeline(core::StreamingPipeline* pipelinePtr);

    private:
        std::promise<void> readyPromise_;
        std::thread t_{&MemoryAllocationCoordinator::Process, this};
        std::exception_ptr memAllocExceptionPtr_{nullptr};

        MemoryManager* pMemoryManager_;
        std::atomic<bool> running_{true};
        std::condition_variable cv_;
        std::mutex cvMutex_;
        std::atomic<core::StreamingPipeline*> preAllocationPipelinePtr_{nullptr};
        MPMCQueue<operators::ChannelID> chunkRequestQueue_;
        MPMCQueue<ChunkID> chunkReclamationQueue_;

        uint64_t lastMemoryWarningAt_{0};
        std::unordered_set<ChunkID> failureLoggedChunkIDs_;
    };

}// namespace enjima::memory


#endif//ENJIMA_MEMORY_ALLOCATION_COORDINATOR_H
