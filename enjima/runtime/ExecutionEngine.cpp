//
// Created by m34ferna on 05/01/24.
//

#include "ExecutionEngine.h"
#include "IllegalStateException.h"
#include "InitializationException.h"
#include "JobValidationException.h"
#include "RuntimeConfiguration.h"
#include "StreamingTask.h"
#include "enjima/core/JobHandler.h"
#include "enjima/memory/MemoryManager.h"
#include "enjima/runtime/scheduling/PriorityScheduler.h"
#include "enjima/runtime/scheduling/StateBasedTask.h"
#include "enjima/runtime/scheduling/ThreadBasedTask.h"
#include "enjima/runtime/scheduling/policy/AdaptivePriorityCalculator.h"
#include "enjima/runtime/scheduling/policy/HighestRatePriorityCalculator.h"
#include "enjima/runtime/scheduling/policy/InputSizeBasedPriorityCalculator.h"
#include "enjima/runtime/scheduling/policy/LatencyOptimizedPriorityCalculator.h"
#include "enjima/runtime/scheduling/policy/MinLatencyPriorityCalculator.h"
#include "enjima/runtime/scheduling/policy/SPLatencyOptimizedPriorityCalculator.h"
#include "enjima/runtime/scheduling/policy/SimpleThroughputPriorityCalculator.h"
#include "scheduling/policy/MinCostPriorityCalculator.h"
#include "spdlog/sinks/rotating_file_sink.h"
#include "yaml-cpp/yaml.h"

#include <algorithm>
#include <iostream>

using namespace enjima::core;

namespace enjima::runtime {
    static const int kMaxLogFileSize = 1048576;
    static const int kMaxLogFiles = 10;
    static const int kDefaultLoggingPeriodInSeconds = 10;

    ExecutionEngine::~ExecutionEngine()
    {
        if (running_) {
            Shutdown();
        }
        delete pMemoryManager_;
        delete pScheduler_;
        delete profilerPtr_;
        delete pConfig_;
    }

    void ExecutionEngine::InitializePreemptiveScheduling(runtime::StreamingTask::ProcessingMode processingMode)
    {
        switch (priorityType_) {
            case PriorityType::kInputQueueSize:
                pScheduler_ = new PriorityScheduler<PreemptiveMode, InputSizeBasedPriorityCalculator>{
                        schedulingPeriodMs_, profilerPtr_, processingMode, eventThreshold_, idleThresholdMs_};
                break;
            case PriorityType::kLatencyOptimized:
                pScheduler_ = new PriorityScheduler<PreemptiveMode, LatencyOptimizedPriorityCalculator>{
                        schedulingPeriodMs_, profilerPtr_, processingMode, eventThreshold_, idleThresholdMs_};
                break;
            case PriorityType::kSPLatencyOptimized:
                pScheduler_ = new PriorityScheduler<PreemptiveMode, SPLatencyOptimizedPriorityCalculator>{
                        schedulingPeriodMs_, profilerPtr_, processingMode, eventThreshold_, idleThresholdMs_};
                break;
            case PriorityType::kLeastRecent:
                pScheduler_ = new PriorityScheduler<PreemptiveMode, LeastRecentOperatorPriorityCalculator>{
                        schedulingPeriodMs_, profilerPtr_, processingMode, eventThreshold_, idleThresholdMs_};
                break;
            case PriorityType::kThroughputOptimized:
                pScheduler_ = new PriorityScheduler<PreemptiveMode, ThroughputOptimizedPriorityCalculator>{
                        schedulingPeriodMs_, profilerPtr_, processingMode, eventThreshold_, idleThresholdMs_};
                break;
            case PriorityType::kSimpleThroughput:
                pScheduler_ = new PriorityScheduler<PreemptiveMode, SimpleThroughputPriorityCalculator>{
                        schedulingPeriodMs_, profilerPtr_, processingMode, eventThreshold_, idleThresholdMs_};
                break;
            case PriorityType::kRoundRobin:
                pScheduler_ = new PriorityScheduler<PreemptiveMode, RoundRobinPriorityCalculator>{schedulingPeriodMs_,
                        profilerPtr_, processingMode, eventThreshold_, idleThresholdMs_};
                break;
            case PriorityType::kAdaptive:
                pScheduler_ = new PriorityScheduler<PreemptiveMode, AdaptivePriorityCalculator>{schedulingPeriodMs_,
                        profilerPtr_, processingMode, eventThreshold_, idleThresholdMs_};
                break;
            default:
                throw InitializationException{std::string("Unrecognized scheduling policy selected!")};
        }
    }

    void ExecutionEngine::InitializeNonPreemptiveScheduling(runtime::StreamingTask::ProcessingMode processingMode)
    {
        auto defaultBlockSize = pMemoryManager_->GetDefaultNumEventsPerBlock();
        switch (priorityType_) {
            case PriorityType::kInputQueueSize:
                pScheduler_ =
                        new PriorityScheduler<NonPreemptiveMode, InputSizeBasedPriorityCalculator>{schedulingPeriodMs_,
                                profilerPtr_, processingMode, eventThreshold_, idleThresholdMs_, defaultBlockSize};
                break;
            case PriorityType::kLatencyOptimized:
                pScheduler_ = new PriorityScheduler<NonPreemptiveMode, LatencyOptimizedPriorityCalculator>{
                        schedulingPeriodMs_, profilerPtr_, processingMode, eventThreshold_, idleThresholdMs_,
                        defaultBlockSize};
                break;
            case PriorityType::kSPLatencyOptimized:
                pScheduler_ = new PriorityScheduler<NonPreemptiveMode, SPLatencyOptimizedPriorityCalculator>{
                        schedulingPeriodMs_, profilerPtr_, processingMode, eventThreshold_, idleThresholdMs_,
                        defaultBlockSize};
                break;
            case PriorityType::kHighestRate:
                pScheduler_ = new PriorityScheduler<NonPreemptiveBasicMode, HighestRatePriorityCalculator>{
                        schedulingPeriodMs_, profilerPtr_, processingMode, eventThreshold_, idleThresholdMs_,
                        defaultBlockSize};
                break;
            case PriorityType::kMinLatency:
                pScheduler_ =
                        new PriorityScheduler<NonPreemptiveBasicMode, MinLatencyPriorityCalculator>{schedulingPeriodMs_,
                                profilerPtr_, processingMode, eventThreshold_, idleThresholdMs_, defaultBlockSize};
                break;
            case PriorityType::kMinCost:
                pScheduler_ = new PriorityScheduler<NonPreemptiveThroughputOptimizedMode, MinCostPriorityCalculator>{
                        schedulingPeriodMs_, profilerPtr_, processingMode, eventThreshold_, idleThresholdMs_,
                        defaultBlockSize};
                break;
            case PriorityType::kLeastRecent:
                pScheduler_ = new PriorityScheduler<NonPreemptiveMode, LeastRecentOperatorPriorityCalculator>{
                        schedulingPeriodMs_, profilerPtr_, processingMode, eventThreshold_, idleThresholdMs_,
                        defaultBlockSize};
                break;
            case PriorityType::kThroughputOptimized:
                pScheduler_ = new PriorityScheduler<NonPreemptiveThroughputOptimizedMode,
                        ThroughputOptimizedPriorityCalculator>{schedulingPeriodMs_, profilerPtr_, processingMode,
                        eventThreshold_, idleThresholdMs_, defaultBlockSize};
                break;
            case PriorityType::kSimpleThroughput:
                pScheduler_ = new PriorityScheduler<NonPreemptiveMode, SimpleThroughputPriorityCalculator>{
                        schedulingPeriodMs_, profilerPtr_, processingMode, eventThreshold_, idleThresholdMs_,
                        defaultBlockSize};
                break;
            case PriorityType::kFirstComeFirstServed:
                pScheduler_ = new PriorityScheduler<NonPreemptiveBasicMode, FCFSPriorityCalculator>{schedulingPeriodMs_,
                        profilerPtr_, processingMode, eventThreshold_, idleThresholdMs_, defaultBlockSize};
                break;
            case PriorityType::kRoundRobin:
                pScheduler_ =
                        new PriorityScheduler<NonPreemptiveBasicMode, RoundRobinPriorityCalculator>{schedulingPeriodMs_,
                                profilerPtr_, processingMode, eventThreshold_, idleThresholdMs_, defaultBlockSize};
                break;
            case PriorityType::kAdaptive:
                pScheduler_ = new PriorityScheduler<NonPreemptiveMode, AdaptivePriorityCalculator>{schedulingPeriodMs_,
                        profilerPtr_, processingMode, eventThreshold_, idleThresholdMs_, defaultBlockSize};
                break;
            default:
                throw InitializationException{std::string("Unrecognized scheduling policy selected!")};
        }
    }

    // public scope
    void ExecutionEngine::Init(size_t maxMemory, size_t numBlocksPerChunk, int32_t defaultNumEventsPerBlock,
            std::string systemIDString, SchedulingMode schedulingMode, uint32_t numStateBasedWorkers,
            runtime::StreamingTask::ProcessingMode processingMode, PreemptMode preemptMode)
    {
        auto pMemoryManager = new memory::MemoryManager(maxMemory, numBlocksPerChunk, defaultNumEventsPerBlock,
                memory::MemoryManager::AllocatorType::kBasic);
        systemIDString_ = std::move(systemIDString);
        auto pProfiler = new metrics::Profiler(kDefaultLoggingPeriodInSeconds * 1000, true, systemIDString_);
        Init(pMemoryManager, pProfiler, schedulingMode, numStateBasedWorkers, processingMode, preemptMode);
    }

    // public scope
    void ExecutionEngine::Init(memory::MemoryManager* pMemoryManager, metrics::Profiler* pProfiler,
            SchedulingMode schedulingMode, uint32_t numStateBasedWorkers,
            runtime::StreamingTask::ProcessingMode processingMode, PreemptMode preemptMode)
    {
        try {
            auto rotatingSink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
                    "logs/enjima-execution-engine.log", kMaxLogFileSize, kMaxLogFiles, false);
            logger_ = std::make_shared<spdlog::logger>("execution_engine_logger", rotatingSink);
            spdlog::set_default_logger(logger_);
            pConfig_ = new RuntimeConfiguration("conf/enjima-config.yaml");
            pMemoryManager_ = pMemoryManager;
            profilerPtr_ = pProfiler;
            schedulingMode_ = schedulingMode;
            preemptMode_ = preemptMode;
            profilerPtr_->UpdateNumAvailableCpus(pConfig_->GetNumAvailableCpus());
            profilerPtr_->SetInternalMetricsUpdateIntervalMs(pConfig_->GetInternalMetricsUpdateIntervalMillis());
            profilerPtr_->SetTotalNumSBWorkers(numStateBasedWorkers);

            if (schedulingMode_ != SchedulingMode::kThreadBased) {
                if (schedulingPeriodMs_ == 0) {
                    throw InitializationException(std::string("Please set the scheduling period to a value > 0 ms!"));
                }
                switch (schedulingMode_) {
                    case SchedulingMode::kStateBasedPriority:
                        // TODO Find a way to simplify this initialization
                        if (preemptMode_ == PreemptMode::kPreemptive) {
                            InitializePreemptiveScheduling(processingMode);
                        }
                        else {
                            InitializeNonPreemptiveScheduling(processingMode);
                        }
                        break;
                    default:
                        throw InitializationException{std::string("Unrecognized scheduling mode selected!")};
                }
                auto workerCpuList = pConfig_->GetConfigAsString({"runtime", "workerCpuList"});
                for (auto cpuId: GetCpuIdVector(workerCpuList)) {
                    availableWorkerCpus_.enqueue(cpuId);
                }
                if (availableWorkerCpus_.size_approx() != numStateBasedWorkers) {
                    throw InitializationException("Number of workers must be same as the number of configured worker "
                                                  "CPUs in enjima-config.yaml!");
                }
                for (uint32_t i = 0; i < numStateBasedWorkers; i++) {
                    auto pTask = new StateBasedTask{pScheduler_, i + 1, processingMode, this};
                    registeredSBTasks_.emplace_back(pTask);
                }
                pScheduler_->Init();
            }
            else {
                pScheduler_ = nullptr;
            }
            auto preAllocationMode = pConfig_->GetPreAllocationMode();
            auto preAllocationEnabled =
                    (preAllocationMode == 1) ||
                    (preAllocationMode == 2 && schedulingMode_ == SchedulingMode::kStateBasedPriority &&
                            processingMode == StreamingTask::ProcessingMode::kBlockBasedBatch &&
                            (priorityType_ == PriorityType::kLatencyOptimized ||
                                    priorityType_ == PriorityType::kAdaptive));
            pMemoryManager_->Init(preAllocationEnabled);
            auto memManPreAllocationEnabled = pMemoryManager->IsPreAllocationEnabled();
            if ((systemIDString_ != "EnjimaDefault") &&
                    ((memManPreAllocationEnabled && systemIDString_.find("PreAllocate") == std::string::npos) ||
                            (!memManPreAllocationEnabled && systemIDString_.find("OnDemand") == std::string::npos))) {
                auto memManPreAllocationEnabledStr = memManPreAllocationEnabled ? "true" : "false";
                throw InitializationException{std::string("System ID ")
                                .append(systemIDString_)
                                .append(" does not match pre-allocation status: ")
                                .append(memManPreAllocationEnabledStr)};
            }
            logger_->info("Initialized memory manager with pre-allocation enabled: {}", preAllocationEnabled);
            pProfiler->StartMetricsLogger();
            // Since the variable 'initialized_' is atomic, the following store should introduce a memory fence,
            // pushing any loads/stores above to by synchronized
            initialized_.store(true, std::memory_order::seq_cst);
        }
        catch (const spdlog::spdlog_ex& ex) {
            std::cout << "Log initialization failed: " << ex.what() << std::endl;
        }
    }

    void ExecutionEngine::Start()
    {
        if (!initialized_) {
            throw IllegalStateException(std::string("Execution engine has not being initialized!"));
        }
        pMemoryManager_->StartMemoryPreAllocator();
        if (schedulingMode_ != SchedulingMode::kThreadBased) {
            for (const auto pSBTask: registeredSBTasks_) {
                pSBTask->Start();
            }
        }
        running_ = true;
        spdlog::info("Started execution engine.");
        spdlog::flush_on(spdlog::level::off);
    }

    void ExecutionEngine::Shutdown()
    {
        PrintSystemStatus();
        // Make sure to release all memory allocated and clean up
        spdlog::flush_on(spdlog::level::info);
        logger_->info("Shutting down execution engine...");
        if (schedulingMode_ != SchedulingMode::kThreadBased) {
            for (auto sbTaskPtr: registeredSBTasks_) {
                try {
                    sbTaskPtr->Cancel();
                    sbTaskPtr->WaitUntilFinished();
                }
                catch (const CancellationException& e) {
                    spdlog::error("Error when stopping state-based task : {}", e.what());
                }
                delete sbTaskPtr;
            }
            pScheduler_->Shutdown();
        }
        assert(registeredTBTasksByOpId_.empty());
        pMemoryManager_->ReleaseAllResources();
        PrintSystemStatus();
        profilerPtr_->ShutdownMetricsLogger();
        running_ = false;
    }

    JobID ExecutionEngine::Submit(StreamingJob& newStreamingJob, size_t outputQueueSize)
    {
        if (!running_) {
            throw IllegalStateException(std::string("Execution engine has not being started!"));
        }
        ExecutionPlan* generatedExecPlan = GeneratePlan(newStreamingJob);
        ValidatePlan(generatedExecPlan);
        JobID jobId{++totalJobsAllocated};
        auto pJobHandler = new JobHandler(jobId, generatedExecPlan);
        auto retValPair = jobHandlersByOpId_.try_emplace(jobId, pJobHandler);
        if (retValPair.second) {
            auto streamPipeline = generatedExecPlan->GetPipeline();
            pMemoryManager_->InitializePipeline(streamPipeline);
            streamPipeline->Initialize(this, pMemoryManager_, profilerPtr_, outputQueueSize);
            DeployPipeline(streamPipeline);
            StartPipeline(streamPipeline);
            return jobId;
        }
        return JobID{0};
    }

    void ExecutionEngine::Cancel(core::JobID jobId)
    {
        Cancel(jobId, std::chrono::milliseconds{1000});
    }

    void ExecutionEngine::Cancel(JobID jobId, std::chrono::milliseconds waitMs)
    {
        if (jobHandlersByOpId_.contains(jobId)) {
            auto pJobHandler = jobHandlersByOpId_.at(jobId);
            auto* pStreamingPipeline = pJobHandler->GetExecutionPlan()->GetPipeline();
            CancelPipeline(pStreamingPipeline, waitMs);
            jobHandlersByOpId_.erase(jobId);
            delete pJobHandler;
        }
    }

    ExecutionPlan* ExecutionEngine::GeneratePlan(StreamingJob& streamingJob)
    {
        auto* pStreamPipe = new StreamingPipeline;
        auto operatorIdVec = streamingJob.GetOperatorIDsInTopologicalOrder();
        for (auto opId: operatorIdVec) {
            auto& opInfoTuple = streamingJob.GetOperatorInfo(opId);
            auto* pStreamOp = get<0>(opInfoTuple).release();
            registeredOperators_.emplace(opId, pStreamOp);
            pStreamPipe->AddOperatorAtLevel(pStreamOp, streamingJob.GetOperatorDAGLevel(opId));
            auto upstreamOpIdVec = get<1>(opInfoTuple);
            auto downstreamOpIdVec = get<2>(opInfoTuple);
            pStreamPipe->UpdateUpstreamAndDownstreamOperators(opId, upstreamOpIdVec, downstreamOpIdVec);
        }
        pStreamPipe->SetProcessingMode(streamingJob.GetProcessingMode());
        auto* pExecutionPlan = new ExecutionPlan(pStreamPipe);
        return pExecutionPlan;
    }

    void ExecutionEngine::ValidatePlan(core::ExecutionPlan* execPlan)
    {
        auto pStreamPipe = execPlan->GetPipeline();
        if (pStreamPipe != nullptr) {
            auto opVec = pStreamPipe->GetOperatorsInTopologicalOrder();
            if (std::ranges::any_of(opVec.begin(), opVec.end(), [this](operators::StreamingOperator* streamOp) {
                    return streamOp->GetOperatorId() <= 0 ||
                           !this->registeredOperators_.contains(streamOp->GetOperatorId());
                })) {
                throw JobValidationException{"At least one of the operators is not registered!"};
            }
            if (schedulingMode_ != SchedulingMode::kThreadBased) {
                if (pStreamPipe->GetProcessingMode() != StreamingTask::ProcessingMode::kUnassigned) {
                    throw JobValidationException{
                            "Processing mode cannot be assigned to job when state-based scheduling is "
                            "used! Please assign using execution engine configuration"};
                }
                else {
                    pStreamPipe->SetProcessingMode(pScheduler_->GetProcessingMode());
                }
            }
        }
        else {
            throw JobValidationException{"The streaming pipeline is null for the submitted execution plan!"};
        }
    }

    bool ExecutionEngine::IsInitialized() const
    {
        return initialized_;
    }

    bool ExecutionEngine::IsRunning() const
    {
        return running_;
    }

    size_t ExecutionEngine::GetNumActiveJobs() const
    {
        return jobHandlersByOpId_.size();
    }

    operators::OperatorID ExecutionEngine::GetNextOperatorId()
    {
        return nextOperatorId_++;
    }

    void ExecutionEngine::DeployPipeline(core::StreamingPipeline* pipeline)
    {
        if (schedulingMode_ != SchedulingMode::kThreadBased) {
            pScheduler_->RegisterPipeline(pipeline);
        }
        else {
            for (auto* opPtr: pipeline->GetOperatorsInTopologicalOrder()) {
                registeredTBTasksByOpId_.emplace(opPtr->GetOperatorId(),
                        new ThreadBasedTask(opPtr, pipeline->GetProcessingMode(), profilerPtr_, this));
            }
            for (auto* opPtr: pipeline->GetOperatorsInTopologicalOrder()) {
                auto opId = opPtr->GetOperatorId();
                auto currentTask = registeredTBTasksByOpId_.at(opId);
                auto upstreamOps = pipeline->GetUpstreamOperators(opId);
                if (!upstreamOps.empty()) {
                    std::vector<std::pair<StreamingTask*, operators::ChannelID>> upstreamTaskChannelIdPairVec;
                    std::transform(upstreamOps.cbegin(), upstreamOps.cend(),
                            std::back_inserter(upstreamTaskChannelIdPairVec),
                            [&](operators::StreamingOperator* upstreamOpPtr) {
                                auto channelId = pMemoryManager_->GetChannelID(upstreamOpPtr->GetOperatorId(), opId);
                                return std::make_pair(registeredTBTasksByOpId_.at(upstreamOpPtr->GetOperatorId()),
                                        channelId);
                            });
                    currentTask->SetUpstreamTasks(upstreamTaskChannelIdPairVec);
                }
                auto downstreamOps = pipeline->GetDownstreamOperators(opId);
                if (!downstreamOps.empty()) {
                    std::vector<std::pair<StreamingTask*, operators::ChannelID>> downstreamTaskChannelIdPairVec;
                    std::transform(downstreamOps.cbegin(), downstreamOps.cend(),
                            std::back_inserter(downstreamTaskChannelIdPairVec),
                            [&](operators::StreamingOperator* downstreamOpPtr) {
                                auto channelId = pMemoryManager_->GetChannelID(opId, downstreamOpPtr->GetOperatorId());
                                return std::make_pair(registeredTBTasksByOpId_.at(downstreamOpPtr->GetOperatorId()),
                                        channelId);
                            });
                    currentTask->SetDownstreamTasks(downstreamTaskChannelIdPairVec);
                }
            }
        }
    }

    void ExecutionEngine::StartPipeline(core::StreamingPipeline* pipeline)
    {
        for (auto* opPtr: pipeline->GetOperatorsInTopologicalOrder()) {
            if (schedulingMode_ != SchedulingMode::kThreadBased) {
                pScheduler_->ActivateOperator(opPtr);
            }
            else {
                auto* tbTask = registeredTBTasksByOpId_.at(opPtr->GetOperatorId());
                tbTask->Start();
            }
        }
    }

    void ExecutionEngine::CancelPipeline(core::StreamingPipeline* pipeline, std::chrono::milliseconds waitMs)
    {
        pipeline->SetWaitingToCancel();
        for (auto* srcOpPtr: pipeline->GetSourceOperators()) {
            if (schedulingMode_ != SchedulingMode::kThreadBased) {
                pScheduler_->DeactivateOperator(srcOpPtr, waitMs);
            }
            else {
                auto* srcTask = registeredTBTasksByOpId_.at(srcOpPtr->GetOperatorId());
                srcTask->Cancel();
            }
        }
        for (auto* opPtr: pipeline->GetOperatorsInTopologicalOrder()) {
            if (schedulingMode_ == SchedulingMode::kThreadBased) {
                auto* task = registeredTBTasksByOpId_.at(opPtr->GetOperatorId());
                if (!task->IsTaskComplete(waitMs)) {
                    logger_->error("Could not gracefully shutdown task running operator {} (ID: {})! Forcibly shutting "
                                   "down task.",
                            task->GetStreamingOperator()->GetOperatorName(),
                            task->GetStreamingOperator()->GetOperatorId());
                    if (!task->IsTaskComplete()) {
                        task->ForceCancel();
                    }
                }
                task->WaitUntilFinished();
            }
            else {
                if (!opPtr->IsSourceOperator()) {
                    pScheduler_->DeactivateOperator(opPtr, waitMs);
                }
            }
        }
        if (schedulingMode_ != SchedulingMode::kThreadBased) {
            pScheduler_->DeRegisterPipeline(pipeline);
        }
        for (auto* opPtr: pipeline->GetOperatorsInTopologicalOrder()) {
            if (schedulingMode_ == SchedulingMode::kThreadBased) {
                auto* tbTaskPtr = registeredTBTasksByOpId_.at(opPtr->GetOperatorId());
                if (!tbTaskPtr->IsTaskComplete()) {
                    logger_->error("Task running operator '{}' was not properly shutdown!",
                            tbTaskPtr->GetStreamingOperator()->GetOperatorName());
                }
                registeredTBTasksByOpId_.erase(opPtr->GetOperatorId());
                delete tbTaskPtr;
            }
            delete opPtr;
        }
    }

    void ExecutionEngine::SetSchedulingPeriodMs(uint64_t schedulingPeriodMs)
    {
        if (initialized_) {
            throw IllegalStateException{std::string("Scheduling period has to be set before initialization!")};
        }
        schedulingPeriodMs_ = schedulingPeriodMs;
    }

    void ExecutionEngine::SetIdleThresholdMs(uint64_t idleThresholdMs)
    {
        if (initialized_) {
            throw IllegalStateException{std::string("Idle threshold has to be set before initialization!")};
        }
        idleThresholdMs_ = idleThresholdMs;
    }

    void ExecutionEngine::SetEventThreshold(uint64_t eventThreshold)
    {
        if (initialized_) {
            throw IllegalStateException{std::string("Event threshold has to be set before initialization!")};
        }
        eventThreshold_ = eventThreshold;
    }

    void ExecutionEngine::SetPreemptMode(PreemptMode preemptMode)
    {
        if (initialized_) {
            throw IllegalStateException{std::string("Preempt mode has to be set before initialization!")};
        }
        preemptMode_ = preemptMode;
    }

    void ExecutionEngine::SetPriorityType(PriorityType priorityType)
    {
        if (initialized_) {
            throw IllegalStateException{std::string("Priority type has to be set before initialization!")};
        }
        priorityType_ = priorityType;
    }

    void ExecutionEngine::PinThreadToCpuListFromConfig(SupportedThreadType threadType)
    {
        std::string cpuList;
        switch (threadType) {
            case SupportedThreadType::kTBWorker:
                cpuList = pConfig_->GetConfigAsString({"runtime", "workerCpuList"});
                break;
            case SupportedThreadType::kSBWorker:
                cpuList = "";
                break;
            case SupportedThreadType::kEventGen:
                cpuList = pConfig_->GetConfigAsString({"runtime", "eventGenCpuList"});
                break;
            case SupportedThreadType::kOther:
            default:
                cpuList = pConfig_->GetConfigAsString({"runtime", "otherCpuList"});
                break;
        }

        cpu_set_t cpuSet;
        CPU_ZERO(&cpuSet);
        if (threadType == SupportedThreadType::kSBWorker) {
            size_t assignedCpuId;
            if (!availableWorkerCpus_.try_dequeue(assignedCpuId)) {
                char threadName[16];
                pthread_getname_np(pthread_self(), threadName, 16);
                throw enjima::runtime::InitializationException{
                        std::string("Could not get assigned CPU id for SB worker : ").append(threadName)};
            }
            CPU_SET(assignedCpuId, &cpuSet);
        }
        else {
            std::vector<size_t> cpuIdVector = GetCpuIdVector(cpuList);
            for (auto cpuId: cpuIdVector) {
                CPU_SET(cpuId, &cpuSet);
            }
        }
        auto pthreadCurrent = pthread_self();
        auto affinityRet = pthread_setaffinity_np(pthreadCurrent, sizeof(cpuSet), &cpuSet);
        if (affinityRet != 0) {
            throw enjima::runtime::InitializationException{
                    std::string("Could not set this thread affinity to CPUs: ").append(cpuList)};
        }
    }

    std::vector<size_t> ExecutionEngine::GetCpuIdVector(const std::string& cpuIdListStr)
    {
        std::stringstream strStream(cpuIdListStr);
        std::vector<size_t> result;
        while (strStream.good()) {
            std::string substr;
            getline(strStream, substr, ',');
            if (!substr.empty()) {
                result.push_back(std::stoul(substr));
            }
        }
        return result;
    }

    void ExecutionEngine::PrintSystemStatus()
    {
        spdlog::info("Printing available system metrics...");
        spdlog::info("Memory : [Allocated (MB): {}, Max (MB): {}, Events/block (Default): {}]",
                pMemoryManager_->GetCurrentlyAllocatedMemory() / (1024 * 1024),
                pMemoryManager_->GetMaxMemory() / (1024 * 1024), pMemoryManager_->GetDefaultNumEventsPerBlock());
#if ENJIMA_METRICS_LEVEL >= 2
        if (schedulingMode_ == SchedulingMode::kStateBasedPriority) {
            auto schedTime = profilerPtr_->GetCounter(metrics::kSchedulingTimeCounterLabel)->GetCount();
            auto schedCount = profilerPtr_->GetCounter(metrics::kSchedulingCountCounterLabel)->GetCount();
            auto updateTime = profilerPtr_->GetCounter(metrics::kPriorityUpdateTimeCounterLabel)->GetCount();
            auto updateCount = profilerPtr_->GetCounter(metrics::kPriorityUpdateCountCounterLabel)->GetCount();
            spdlog::info("Scheduling Stats: [Total time (ms): {:.2f}, Count: {}, Avg (us): {:.2f}]",
                    static_cast<double>(schedTime) / 1000.0, schedCount, (double) schedTime / (double) schedCount);
            spdlog::info("Priority Update Stats: [Total time (ms): {:.2f}, Count: {}, Avg (us): {:.2f}]",
                    static_cast<double>(updateTime) / 1000, updateCount, (double) updateTime / (double) updateCount);
#if ENJIMA_METRICS_LEVEL >= 3
            auto avgSchedVecLockTime =
                    profilerPtr_->GetDoubleAverageGauge(metrics::kSchedVecLockTimeGaugeLabel)->GetVal();
            auto avgSchedTotCalcTime =
                    profilerPtr_->GetDoubleAverageGauge(metrics::kSchedTotCalcTimeGaugeLabel)->GetVal();
            auto avgSchedPriorityUpdateTime =
                    profilerPtr_->GetDoubleAverageGauge(metrics::kSchedPriorityUpdateTimeGaugeLabel)->GetVal();
            spdlog::info("Additional Scheduling Stats (Avg Time (us)): [VecLock : {:.2f}, PriorityUpdate: {:.2f}, "
                         "TotCalc: {:.2f}]",
                    avgSchedVecLockTime, avgSchedTotCalcTime, avgSchedPriorityUpdateTime);
#endif
        }
#endif
    }
}// namespace enjima::runtime