//
// Created by m34ferna on 05/01/24.
//

#ifndef ENJIMA_EXECUTION_ENGINE_H
#define ENJIMA_EXECUTION_ENGINE_H

#include "RuntimeTypes.h"
#include "StreamingTask.h"
#include "enjima/common/TypeAliases.h"
#include "enjima/core/CoreInternals.fwd.h"
#include "enjima/core/CoreTypeAliases.h"
#include "enjima/memory/MemoryManager.fwd.h"
#include "enjima/metrics/Profiler.h"
#include "enjima/operators/OperatorsInternals.fwd.h"
#include "enjima/operators/OperatorsTypeAliases.h"
#include "enjima/runtime/RuntimeInternals.fwd.h"
#include "enjima/runtime/scheduling/SchedulingTypes.h"

#include <thread>
#include <vector>

template<>
struct std::hash<enjima::core::InstanceID<uint64_t>> {
    std::size_t operator()(const enjima::core::InstanceID<uint64_t>& instanceId) const noexcept
    {
        return hash<uint64_t>{}(instanceId.GetId());
    }
};

namespace enjima::runtime {

    class ExecutionEngine {
    public:
        ~ExecutionEngine();
        void Init(size_t maxMemory, size_t numBlocksPerChunk, int32_t defaultNumEventsPerBlock,
                std::string systemIDString, SchedulingMode schedulingMode = SchedulingMode::kThreadBased,
                uint32_t numStateBasedWorkers = 8,
                runtime::StreamingTask::ProcessingMode processingMode =
                        runtime::StreamingTask::ProcessingMode::kBlockBasedSingle,
                PreemptMode preemptMode = PreemptMode::kNonPreemptive);
        void InitializeNonPreemptiveScheduling(runtime::StreamingTask::ProcessingMode processingMode);
        void InitializePreemptiveScheduling(runtime::StreamingTask::ProcessingMode processingMode);
        void Init(memory::MemoryManager* pMemoryManager, metrics::Profiler* pProfiler,
                SchedulingMode schedulingMode = SchedulingMode::kThreadBased, uint32_t numStateBasedWorkers = 8,
                runtime::StreamingTask::ProcessingMode processingMode =
                        runtime::StreamingTask::ProcessingMode::kBlockBasedSingle,
                enjima::runtime::PreemptMode preemptMode = PreemptMode::kNonPreemptive);
        void Start();
        void Shutdown();
        core::JobID Submit(StreamingJob& newStreamingJob, size_t outputQueueSize = 1'000'000);
        void Cancel(core::JobID jobId);
        void Cancel(core::JobID jobId, std::chrono::milliseconds waitMs);
        operators::OperatorID GetNextOperatorId();

        [[nodiscard]] bool IsInitialized() const;
        [[nodiscard]] bool IsRunning() const;
        [[nodiscard]] size_t GetNumActiveJobs() const;

        void SetSchedulingPeriodMs(uint64_t schedulingPeriodMs);
        void SetIdleThresholdMs(uint64_t idleThresholdMs);
        void SetEventThreshold(uint64_t eventThreshold);
        void SetPreemptMode(PreemptMode preemptMode);
        void SetPriorityType(PriorityType priorityType);
        void PinThreadToCpuListFromConfig(SupportedThreadType threadType);

    private:
        core::ExecutionPlan* GeneratePlan(StreamingJob& streamingJob);
        void ValidatePlan(core::ExecutionPlan* execPlan);
        void DeployPipeline(core::StreamingPipeline* downstreamOpPtr);
        void StartPipeline(core::StreamingPipeline* pipeline);
        void CancelPipeline(core::StreamingPipeline* pipeline, std::chrono::milliseconds waitMs);
        static std::vector<size_t> GetCpuIdVector(const std::string& cpuIdListStr);
        void PrintSystemStatus();

        UnorderedHashMapST<core::JobID, JobHandler*> jobHandlersByOpId_;
        memory::MemoryManager* pMemoryManager_{nullptr};
        Scheduler* pScheduler_{nullptr};
        metrics::Profiler* profilerPtr_{nullptr};
        std::vector<std::thread> workerThreads_;
        MPMCQueue<size_t> availableWorkerCpus_{std::thread::hardware_concurrency()};
        UnorderedHashMapST<enjima::operators::OperatorID, operators::StreamingOperator*> registeredOperators_;
        UnorderedHashMapST<enjima::operators::OperatorID, ThreadBasedTask*> registeredTBTasksByOpId_;
        std::vector<StateBasedTask*> registeredSBTasks_;
        uint64_t totalJobsAllocated{0};
        operators::OperatorID nextOperatorId_{1};
        std::atomic<bool> initialized_{false};
        bool running_{false};
        std::shared_ptr<spdlog::logger> logger_;
        RuntimeConfiguration* pConfig_{nullptr};

        // Following are configuration properties
        std::string systemIDString_{"EnjimaDefault"};
        SchedulingMode schedulingMode_{SchedulingMode::kThreadBased};
        PreemptMode preemptMode_{PreemptMode::kUnassigned};
        PriorityType priorityType_{PriorityType::kUnassigned};
        uint64_t schedulingPeriodMs_{0};
        uint64_t eventThreshold_{1000};
        uint64_t idleThresholdMs_{0};
    };
}// namespace enjima::runtime

#endif// ENJIMA_EXECUTION_ENGINE_H
