//
// Created by m34ferna on 12/10/24.
//

template<typename Duration>
JobT SetUpBasicWindowlessPipeline(enjima::runtime::ExecutionEngine* pExecEngine, const std::string& srcOpName,
        const std::string& projectOpName, const std::string& statEqJoinOpName, const std::string& sinkOpName,
        enjima::runtime::StreamingTask::ProcessingMode processingMode)
{
    JobT streamingJob;
    OpIDT srcOpId = pExecEngine->GetNextOperatorId();
    auto uPtrSrcOp = std::make_unique<LatencyTrackingUIDBasedYSBSource<Duration>>(srcOpId, srcOpName, 50);
    uPtrSrcOp->PopulateEventCache(1048576, 100, 10);
    auto adIdCampaignIdMap = uPtrSrcOp->GetAdIdToCampaignIdMap();

    enjima::runtime::DataStream<LatencyTrackingUIDBasedYSBSource<Duration>> srcDataStream(std::move(uPtrSrcOp),
            streamingJob);
    OpIDT projectOpId = pExecEngine->GetNextOperatorId();
    using MapOpT = MapOperator<UIDBasedYSBAdEvent, UIDBasedYSBProjectedEvent, UIDBasedYSBProjectFunction>;
    auto uPtrProjectOp = std::make_unique<MapOpT>(projectOpId, projectOpName, UIDBasedYSBProjectFunction{});

    OpIDT statJoinOpId = pExecEngine->GetNextOperatorId();
    using JoinOpT = StaticEquiJoinOperator<UIDBasedYSBProjectedEvent, uint64_t, UIDBasedYSBCampaignAdEvent, uint64_t,
            UIDBasedYSBAdIdExtractFunction, UIDBasedYSBEqJoinFunction>;
    auto uPtrStatEquiJoinOp = std::make_unique<JoinOpT>(statJoinOpId, statEqJoinOpName,
            UIDBasedYSBAdIdExtractFunction{}, UIDBasedYSBEqJoinFunction{}, adIdCampaignIdMap);

    OpIDT sinkOpId = pExecEngine->GetNextOperatorId();
    auto sinkFn = UIDBasedNoOpSinkFunction<UIDBasedYSBCampaignAdEvent>{};
    using SinkOpT = GenericSinkOperator<UIDBasedYSBCampaignAdEvent,
            UIDBasedNoOpSinkFunction<UIDBasedYSBCampaignAdEvent>, Duration>;
    auto uPtrSinkOp = std::make_unique<SinkOpT>(sinkOpId, sinkOpName, sinkFn);

    srcDataStream.AddOperator(std::move(uPtrProjectOp))
            .AddOperator(std::move(uPtrStatEquiJoinOp))
            .template AddSinkOperator<Duration>(std::move(uPtrSinkOp));
    streamingJob.SetProcessingMode(processingMode);
    return streamingJob;
}

template<typename Duration>
JobT SetUpBasicWindowedPipeline(enjima::runtime::ExecutionEngine* pExecEngine, const std::string& srcOpName,
        const std::string& projectOpName, const std::string& statEqJoinOpName, const std::string& windowOpName,
        const std::string& sinkOpName, enjima::runtime::StreamingTask::ProcessingMode processingMode)
{
    JobT streamingJob;
    OpIDT srcOpId = pExecEngine->GetNextOperatorId();
    auto uPtrSrcOp = std::make_unique<LatencyTrackingUIDBasedYSBSource<Duration>>(srcOpId, srcOpName, 50);
    uPtrSrcOp->PopulateEventCache(1048576, 100, 10);
    auto adIdCampaignIdMap = uPtrSrcOp->GetAdIdToCampaignIdMap();

    enjima::runtime::DataStream<LatencyTrackingUIDBasedYSBSource<Duration>> srcDataStream(std::move(uPtrSrcOp),
            streamingJob);
    OpIDT projectOpId = pExecEngine->GetNextOperatorId();
    using MapOpT = MapOperator<UIDBasedYSBAdEvent, UIDBasedYSBProjectedEvent, UIDBasedYSBProjectFunction>;
    auto uPtrProjectOp = std::make_unique<MapOpT>(projectOpId, projectOpName, UIDBasedYSBProjectFunction{});

    OpIDT statJoinOpId = pExecEngine->GetNextOperatorId();
    using JoinOpT = StaticEquiJoinOperator<UIDBasedYSBProjectedEvent, uint64_t, UIDBasedYSBCampaignAdEvent, uint64_t,
            UIDBasedYSBAdIdExtractFunction, UIDBasedYSBEqJoinFunction>;
    auto uPtrStatEquiJoinOp = std::make_unique<JoinOpT>(statJoinOpId, statEqJoinOpName,
            UIDBasedYSBAdIdExtractFunction{}, UIDBasedYSBEqJoinFunction{}, adIdCampaignIdMap);

    OpIDT windowOpId = pExecEngine->GetNextOperatorId();
    using WindowOpT = FixedEventTimeWindowOperator<UIDBasedYSBCampaignAdEvent, UIDBasedYSBWinEmitEvent, true,
            UIDBasedYSBCampaignAggFunction>;
    auto uPtrWindowOp = std::make_unique<WindowOpT>(windowOpId, windowOpName, UIDBasedYSBCampaignAggFunction{},
            std::chrono::seconds(10));

    OpIDT sinkOpId = pExecEngine->GetNextOperatorId();
    auto sinkFn = UIDBasedNoOpSinkFunction<UIDBasedYSBWinEmitEvent>{};
    using SinkOpT =
            GenericSinkOperator<UIDBasedYSBWinEmitEvent, UIDBasedNoOpSinkFunction<UIDBasedYSBWinEmitEvent>, Duration>;
    auto uPtrSinkOp = std::make_unique<SinkOpT>(sinkOpId, sinkOpName, sinkFn);

    srcDataStream.AddOperator(std::move(uPtrProjectOp))
            .AddOperator(std::move(uPtrStatEquiJoinOp))
            .AddOperator(std::move(uPtrWindowOp))
            .template AddSinkOperator<Duration>(std::move(uPtrSinkOp));
    streamingJob.SetProcessingMode(processingMode);
    return streamingJob;
}

template<typename Duration>
JobT SetUpFixedRateWindowedPipeline(enjima::runtime::ExecutionEngine* pExecEngine, const std::string& srcOpName,
        const std::string& projectOpName, const std::string& statEqJoinOpName, const std::string& windowOpName,
        const std::string& sinkOpName, uint64_t inputRate,
        enjima::runtime::StreamingTask::ProcessingMode processingMode)
{
    JobT streamingJob;
    OpIDT srcOpId = pExecEngine->GetNextOperatorId();
    auto uPtrSrcOp = std::make_unique<FixedRateLatencyTrackingUIDBasedYSBSource<Duration>>(srcOpId, srcOpName, 50,
            inputRate, 10'000'000);
    uPtrSrcOp->PopulateEventCache(1048576, 100, 10);
    auto adIdCampaignIdMap = uPtrSrcOp->GetAdIdToCampaignIdMap();

    enjima::runtime::DataStream<FixedRateLatencyTrackingUIDBasedYSBSource<Duration>> srcDataStream(std::move(uPtrSrcOp),
            streamingJob);
    OpIDT projectOpId = pExecEngine->GetNextOperatorId();
    using MapOpT = MapOperator<UIDBasedYSBAdEvent, UIDBasedYSBProjectedEvent, UIDBasedYSBProjectFunction>;
    auto uPtrProjectOp = std::make_unique<MapOpT>(projectOpId, projectOpName, UIDBasedYSBProjectFunction{});

    OpIDT statJoinOpId = pExecEngine->GetNextOperatorId();
    using JoinOpT = StaticEquiJoinOperator<UIDBasedYSBProjectedEvent, uint64_t, UIDBasedYSBCampaignAdEvent, uint64_t,
            UIDBasedYSBAdIdExtractFunction, UIDBasedYSBEqJoinFunction>;
    auto uPtrStatEquiJoinOp = std::make_unique<JoinOpT>(statJoinOpId, statEqJoinOpName,
            UIDBasedYSBAdIdExtractFunction{}, UIDBasedYSBEqJoinFunction{}, adIdCampaignIdMap);

    OpIDT windowOpId = pExecEngine->GetNextOperatorId();
    using WindowOpT = FixedEventTimeWindowOperator<UIDBasedYSBCampaignAdEvent, UIDBasedYSBWinEmitEvent, true,
            UIDBasedYSBCampaignAggFunction>;
    auto uPtrWindowOp = std::make_unique<WindowOpT>(windowOpId, windowOpName, UIDBasedYSBCampaignAggFunction{},
            std::chrono::seconds(10));

    OpIDT sinkOpId = pExecEngine->GetNextOperatorId();
    auto sinkFn = UIDBasedNoOpSinkFunction<UIDBasedYSBWinEmitEvent>{};
    using SinkOpT =
            GenericSinkOperator<UIDBasedYSBWinEmitEvent, UIDBasedNoOpSinkFunction<UIDBasedYSBWinEmitEvent>, Duration>;
    auto uPtrSinkOp = std::make_unique<SinkOpT>(sinkOpId, sinkOpName, sinkFn);

    srcDataStream.AddOperator(std::move(uPtrProjectOp))
            .AddOperator(std::move(uPtrStatEquiJoinOp))
            .AddOperator(std::move(uPtrWindowOp))
            .template AddSinkOperator<Duration>(std::move(uPtrSinkOp));
    streamingJob.SetProcessingMode(processingMode);
    return streamingJob;
}

template<typename Duration>
JobT SetUpFixedRateWindowedPipelineForMultiQueryRun(enjima::runtime::ExecutionEngine* pExecEngine,
        const std::string& srcOpName, const std::string& projectOpName, const std::string& statEqJoinOpName,
        const std::string& windowOpName, const std::string& sinkOpName, uint64_t inputRate,
        const std::string& jobIdSuffix, enjima::runtime::StreamingTask::ProcessingMode processingMode,
        uint64_t reservoirCapacity)
{
    JobT streamingJob;
    OpIDT srcOpId = pExecEngine->GetNextOperatorId();
    auto uPtrSrcOp = std::make_unique<FixedRateLatencyTrackingUIDBasedYSBSource<Duration>>(srcOpId,
            GetSuffixedOperatorName(srcOpName, jobIdSuffix), 50, inputRate, reservoirCapacity);
    uPtrSrcOp->PopulateEventCache(1048576, 100, 10);
    auto adIdCampaignIdMap = uPtrSrcOp->GetAdIdToCampaignIdMap();

    enjima::runtime::DataStream<FixedRateLatencyTrackingUIDBasedYSBSource<Duration>> srcDataStream(std::move(uPtrSrcOp),
            streamingJob);
    OpIDT projectOpId = pExecEngine->GetNextOperatorId();
    using MapOpT = MapOperator<UIDBasedYSBAdEvent, UIDBasedYSBProjectedEvent, UIDBasedYSBProjectFunction>;
    auto uPtrProjectOp = std::make_unique<MapOpT>(projectOpId, GetSuffixedOperatorName(projectOpName, jobIdSuffix),
            UIDBasedYSBProjectFunction{});

    OpIDT statJoinOpId = pExecEngine->GetNextOperatorId();
    using JoinOpT = StaticEquiJoinOperator<UIDBasedYSBProjectedEvent, uint64_t, UIDBasedYSBCampaignAdEvent, uint64_t,
            UIDBasedYSBAdIdExtractFunction, UIDBasedYSBEqJoinFunction>;
    auto uPtrStatEqJoinOp =
            std::make_unique<JoinOpT>(statJoinOpId, GetSuffixedOperatorName(statEqJoinOpName, jobIdSuffix),
                    UIDBasedYSBAdIdExtractFunction{}, UIDBasedYSBEqJoinFunction{}, adIdCampaignIdMap);

    OpIDT windowOpId = pExecEngine->GetNextOperatorId();
    using WindowOpT = FixedEventTimeWindowOperator<UIDBasedYSBCampaignAdEvent, UIDBasedYSBWinEmitEvent, true,
            UIDBasedYSBCampaignAggFunction>;
    auto uPtrWindowOp = std::make_unique<WindowOpT>(windowOpId, GetSuffixedOperatorName(windowOpName, jobIdSuffix),
            UIDBasedYSBCampaignAggFunction{}, std::chrono::seconds(10));

    OpIDT sinkOpId = pExecEngine->GetNextOperatorId();
    auto sinkFn = UIDBasedNoOpSinkFunction<UIDBasedYSBWinEmitEvent>{};
    using SinkOpT =
            GenericSinkOperator<UIDBasedYSBWinEmitEvent, UIDBasedNoOpSinkFunction<UIDBasedYSBWinEmitEvent>, Duration>;
    auto uPtrSinkOp = std::make_unique<SinkOpT>(sinkOpId, GetSuffixedOperatorName(sinkOpName, jobIdSuffix), sinkFn);

    srcDataStream.AddOperator(std::move(uPtrProjectOp))
            .AddOperator(std::move(uPtrStatEqJoinOp))
            .AddOperator(std::move(uPtrWindowOp))
            .template AddSinkOperator<Duration>(std::move(uPtrSinkOp));
    streamingJob.SetProcessingMode(processingMode);
    return streamingJob;
}

template<typename Duration>
JobT SetUpFixedRateYSBWindowedPipelineForMultiQueryRun(enjima::runtime::ExecutionEngine* pExecEngine,
        const std::string& srcOpName, const std::string& filterOpName, const std::string& projectOpName,
        const std::string& statEqJoinOpName, const std::string& windowOpName, const std::string& sinkOpName,
        uint64_t inputRate, const std::string& jobIdSuffix,
        enjima::runtime::StreamingTask::ProcessingMode processingMode, uint64_t reservoirCapacity)
{
    JobT streamingJob;
    OpIDT srcOpId = pExecEngine->GetNextOperatorId();
    auto uPtrSrcOp = std::make_unique<FixedRateLatencyTrackingUIDBasedYSBSource<Duration>>(srcOpId,
            GetSuffixedOperatorName(srcOpName, jobIdSuffix), 50, inputRate, reservoirCapacity);
    uPtrSrcOp->PopulateEventCache(1048576, 100, 10);
    auto adIdCampaignIdMap = uPtrSrcOp->GetAdIdToCampaignIdMap();

    enjima::runtime::DataStream<FixedRateLatencyTrackingUIDBasedYSBSource<Duration>> srcDataStream(std::move(uPtrSrcOp),
            streamingJob);


    OpIDT filterOpId = pExecEngine->GetNextOperatorId();
    auto filterFn = [](const UIDBasedYSBAdEvent& ysbTypeEvent) { return ysbTypeEvent.GetEventType() == 0; };
    using FilterOpT = FilterOperator<UIDBasedYSBAdEvent, decltype(filterFn)>;
    auto uPtrFilterOp =
            std::make_unique<FilterOpT>(filterOpId, GetSuffixedOperatorName(filterOpName, jobIdSuffix), filterFn);

    OpIDT projectOpId = pExecEngine->GetNextOperatorId();
    using MapOpT = MapOperator<UIDBasedYSBAdEvent, UIDBasedYSBProjectedEvent, UIDBasedYSBProjectFunction>;
    auto uPtrProjectOp = std::make_unique<MapOpT>(projectOpId, GetSuffixedOperatorName(projectOpName, jobIdSuffix),
            UIDBasedYSBProjectFunction{});

    OpIDT statJoinOpId = pExecEngine->GetNextOperatorId();
    using JoinOpT = StaticEquiJoinOperator<UIDBasedYSBProjectedEvent, uint64_t, UIDBasedYSBCampaignAdEvent, uint64_t,
            UIDBasedYSBAdIdExtractFunction, UIDBasedYSBEqJoinFunction>;
    auto uPtrStatEqJoinOp =
            std::make_unique<JoinOpT>(statJoinOpId, GetSuffixedOperatorName(statEqJoinOpName, jobIdSuffix),
                    UIDBasedYSBAdIdExtractFunction{}, UIDBasedYSBEqJoinFunction{}, adIdCampaignIdMap);

    OpIDT windowOpId = pExecEngine->GetNextOperatorId();
    using WindowOpT = FixedEventTimeWindowOperator<UIDBasedYSBCampaignAdEvent, UIDBasedYSBWinEmitEvent, true,
            UIDBasedYSBCampaignAggFunction>;
    auto uPtrWindowOp = std::make_unique<WindowOpT>(windowOpId, GetSuffixedOperatorName(windowOpName, jobIdSuffix),
            UIDBasedYSBCampaignAggFunction{}, std::chrono::seconds(10));

    OpIDT sinkOpId = pExecEngine->GetNextOperatorId();
    auto sinkFn = UIDBasedNoOpSinkFunction<UIDBasedYSBWinEmitEvent>{};
    using SinkOpT =
            GenericSinkOperator<UIDBasedYSBWinEmitEvent, UIDBasedNoOpSinkFunction<UIDBasedYSBWinEmitEvent>, Duration>;
    auto uPtrSinkOp = std::make_unique<SinkOpT>(sinkOpId, GetSuffixedOperatorName(sinkOpName, jobIdSuffix), sinkFn);

    srcDataStream.AddOperator(std::move(uPtrProjectOp))
            .AddOperator(std::move(uPtrStatEqJoinOp))
            .AddOperator(std::move(uPtrWindowOp))
            .template AddSinkOperator<Duration>(std::move(uPtrSinkOp));
    streamingJob.SetProcessingMode(processingMode);
    return streamingJob;
}

template<typename Duration>
JobT SetUpFixedRateWindowJoinedPipelineForMultiQueryRun(enjima::runtime::ExecutionEngine* pExecEngine,
        const std::string& srcOpName, const std::string& projectOpName, const std::string& statEqJoinOpName,
        const std::string& windowOpName, const std::string& windowJoinOpName, const std::string& filterOpName,
        const std::string& filteredStatEqJoinOpName, const std::string& filteredWindowOpName,
        const std::string& sinkOpName, uint64_t inputRate, const std::string& jobIdSuffix,
        enjima::runtime::StreamingTask::ProcessingMode processingMode, uint64_t reservoirCapacity)
{
    JobT streamingJob;
    OpIDT srcOpId = pExecEngine->GetNextOperatorId();
    auto uPtrSrcOp = std::make_unique<FixedRateLatencyTrackingUIDBasedYSBSource<Duration>>(srcOpId,
            GetSuffixedOperatorName(srcOpName, jobIdSuffix), 50, inputRate, reservoirCapacity);
    uPtrSrcOp->PopulateEventCache(1048576, 100, 10);
    auto adIdCampaignIdMap = uPtrSrcOp->GetAdIdToCampaignIdMap();
    enjima::runtime::DataStream<FixedRateLatencyTrackingUIDBasedYSBSource<Duration>> srcDataStream(std::move(uPtrSrcOp),
            streamingJob);

    OpIDT projectOpId = pExecEngine->GetNextOperatorId();
    using MapOpT = MapOperator<UIDBasedYSBAdEvent, UIDBasedYSBTypedProjectedEvent, UIDBasedYSBTypedProjectFunction, 2>;
    auto uPtrProjectOp = std::make_unique<MapOpT>(projectOpId, GetSuffixedOperatorName(projectOpName, jobIdSuffix),
            UIDBasedYSBTypedProjectFunction{});

    OpIDT statJoinOpId = pExecEngine->GetNextOperatorId();
    using JoinOpT = StaticEquiJoinOperator<UIDBasedYSBTypedProjectedEvent, uint64_t, UIDBasedYSBCampaignAdEvent,
            uint64_t, UIDBasedYSBTypedAdIdExtractFunction, UIDBasedYSBTypedEqJoinFunction>;
    auto uPtrStatEqJoinOp =
            std::make_unique<JoinOpT>(statJoinOpId, GetSuffixedOperatorName(statEqJoinOpName, jobIdSuffix),
                    UIDBasedYSBTypedAdIdExtractFunction{}, UIDBasedYSBTypedEqJoinFunction{}, adIdCampaignIdMap);

    OpIDT windowOpId = pExecEngine->GetNextOperatorId();
    using WindowOpT = FixedEventTimeWindowOperator<UIDBasedYSBCampaignAdEvent, UIDBasedYSBWinEmitEvent, true,
            UIDBasedYSBCampaignAggFunction>;
    auto uPtrWindowOp = std::make_unique<WindowOpT>(windowOpId, GetSuffixedOperatorName(windowOpName, jobIdSuffix),
            UIDBasedYSBCampaignAggFunction{}, std::chrono::seconds(10));

    OpIDT filterOpId = pExecEngine->GetNextOperatorId();
    auto filterFn = [](const UIDBasedYSBTypedProjectedEvent& typedProjectedAdEvent) {
        return typedProjectedAdEvent.GetEventType() == 0;
    };
    typedef bool (*FilterPredT)(const UIDBasedYSBTypedProjectedEvent&);
    using FilterOpT = FilterOperator<UIDBasedYSBTypedProjectedEvent, FilterPredT>;
    auto uPtrFilterOp = std::make_unique<FilterOpT>(filterOpId, filterOpName, filterFn);

    OpIDT filteredStatJoinOpId = pExecEngine->GetNextOperatorId();
    using FilteredJoinOpT =
            StaticEquiJoinOperator<UIDBasedYSBTypedProjectedEvent, uint64_t, UIDBasedYSBFilteredCampaignAdEvent,
                    uint64_t, UIDBasedYSBTypedAdIdExtractFunction, UIDBasedYSBFilteredEqJoinFunction>;
    auto uPtrFilteredStatEqJoinOp = std::make_unique<FilteredJoinOpT>(filteredStatJoinOpId,
            GetSuffixedOperatorName(filteredStatEqJoinOpName, jobIdSuffix), UIDBasedYSBTypedAdIdExtractFunction{},
            UIDBasedYSBFilteredEqJoinFunction{0}, adIdCampaignIdMap);

    OpIDT filteredWindowOpId = pExecEngine->GetNextOperatorId();
    using FilteredWindowOpT = FixedEventTimeWindowOperator<UIDBasedYSBFilteredCampaignAdEvent,
            UIDBasedYSBFilteredWinEmitEvent, true, UIDBasedYSBFilteredCampaignAggFunction>;
    auto uPtrFilteredWindowOp = std::make_unique<FilteredWindowOpT>(filteredWindowOpId,
            GetSuffixedOperatorName(filteredWindowOpName, jobIdSuffix), UIDBasedYSBFilteredCampaignAggFunction{0},
            std::chrono::seconds(10));

    OpIDT windowJoinOpId = pExecEngine->GetNextOperatorId();
    using WindowJoinOpT = FixedEventTimeWindowJoinOperator<UIDBasedYSBWinEmitEvent, UIDBasedYSBFilteredWinEmitEvent,
            uint64_t, UIDBasedYSBCampaignIdExtractFunction, UIDBasedYSBFilteredCampaignIdExtractFunction,
            UIDBasedYSBWinJoinEmitEvent, UIDBasedYSBWindowJoinFunction>;
    auto uPtrWindowJoinOp = std::make_unique<WindowJoinOpT>(windowJoinOpId,
            GetSuffixedOperatorName(windowJoinOpName, jobIdSuffix), UIDBasedYSBCampaignIdExtractFunction{},
            UIDBasedYSBFilteredCampaignIdExtractFunction{}, UIDBasedYSBWindowJoinFunction{}, std::chrono::seconds(10));

    OpIDT sinkOpId = pExecEngine->GetNextOperatorId();
    auto sinkFn = UIDBasedNoOpSinkFunction<UIDBasedYSBWinJoinEmitEvent>{};
    using SinkOpT = GenericSinkOperator<UIDBasedYSBWinJoinEmitEvent,
            UIDBasedNoOpSinkFunction<UIDBasedYSBWinJoinEmitEvent>, Duration>;
    auto uPtrSinkOp = std::make_unique<SinkOpT>(sinkOpId, GetSuffixedOperatorName(sinkOpName, jobIdSuffix), sinkFn);

    auto projectedStream = srcDataStream.AddOperator(std::move(uPtrProjectOp));
    auto leftStream = projectedStream.AddOperator(std::move(uPtrStatEqJoinOp)).AddOperator(std::move(uPtrWindowOp));
    auto rightStream = projectedStream.AddOperator(std::move(uPtrFilterOp))
                               .AddOperator(std::move(uPtrFilteredStatEqJoinOp))
                               .AddOperator(std::move(uPtrFilteredWindowOp));
    static_assert(std::is_same_v<decltype(rightStream), enjima::runtime::DataStream<FilteredWindowOpT>>);
    leftStream.template AddJoinOperator<WindowJoinOpT, FilteredWindowOpT>(std::move(uPtrWindowJoinOp), rightStream)
            .template AddSinkOperator<Duration, SinkOpT>(std::move(uPtrSinkOp));
    streamingJob.SetProcessingMode(processingMode);
    return streamingJob;
}

template<typename Duration>
JobT SetUpFixedRateWindowCoGroupedPipelineForMultiQueryRun(enjima::runtime::ExecutionEngine* pExecEngine,
        const std::string& srcOpName, const std::string& projectOpName, const std::string& statEqJoinOpName,
        const std::string& windowOpName, const std::string& windowCoGroupOpName, const std::string& filterOpName,
        const std::string& filteredStatEqJoinOpName, const std::string& filteredWindowOpName,
        const std::string& sinkOpName, uint64_t inputRate, const std::string& jobIdSuffix,
        enjima::runtime::StreamingTask::ProcessingMode processingMode, uint64_t reservoirCapacity)
{
    JobT streamingJob;
    OpIDT srcOpId = pExecEngine->GetNextOperatorId();
    auto uPtrSrcOp = std::make_unique<FixedRateLatencyTrackingUIDBasedYSBSource<Duration>>(srcOpId,
            GetSuffixedOperatorName(srcOpName, jobIdSuffix), 50, inputRate, reservoirCapacity);
    uPtrSrcOp->PopulateEventCache(1048576, 100, 10);
    auto adIdCampaignIdMap = uPtrSrcOp->GetAdIdToCampaignIdMap();
    enjima::runtime::DataStream<FixedRateLatencyTrackingUIDBasedYSBSource<Duration>> srcDataStream(std::move(uPtrSrcOp),
            streamingJob);

    OpIDT projectOpId = pExecEngine->GetNextOperatorId();
    using MapOpT = MapOperator<UIDBasedYSBAdEvent, UIDBasedYSBTypedProjectedEvent, UIDBasedYSBTypedProjectFunction, 2>;
    auto uPtrProjectOp = std::make_unique<MapOpT>(projectOpId, GetSuffixedOperatorName(projectOpName, jobIdSuffix),
            UIDBasedYSBTypedProjectFunction{});

    OpIDT statJoinOpId = pExecEngine->GetNextOperatorId();
    using JoinOpT = StaticEquiJoinOperator<UIDBasedYSBTypedProjectedEvent, uint64_t, UIDBasedYSBCampaignAdEvent,
            uint64_t, UIDBasedYSBTypedAdIdExtractFunction, UIDBasedYSBTypedEqJoinFunction>;
    auto uPtrStatEqJoinOp =
            std::make_unique<JoinOpT>(statJoinOpId, GetSuffixedOperatorName(statEqJoinOpName, jobIdSuffix),
                    UIDBasedYSBTypedAdIdExtractFunction{}, UIDBasedYSBTypedEqJoinFunction{}, adIdCampaignIdMap);

    OpIDT windowOpId = pExecEngine->GetNextOperatorId();
    using WindowOpT = FixedEventTimeWindowOperator<UIDBasedYSBCampaignAdEvent, UIDBasedYSBWinEmitEvent, true,
            UIDBasedYSBCampaignAggFunction>;
    auto uPtrWindowOp = std::make_unique<WindowOpT>(windowOpId, GetSuffixedOperatorName(windowOpName, jobIdSuffix),
            UIDBasedYSBCampaignAggFunction{}, std::chrono::seconds(10));

    OpIDT filterOpId = pExecEngine->GetNextOperatorId();
    auto filterFn = [](const UIDBasedYSBTypedProjectedEvent& typedProjectedAdEvent) {
        return typedProjectedAdEvent.GetEventType() == 0;
    };
    typedef bool (*FilterPredT)(const UIDBasedYSBTypedProjectedEvent&);
    using FilterOpT = FilterOperator<UIDBasedYSBTypedProjectedEvent, FilterPredT>;
    auto uPtrFilterOp = std::make_unique<FilterOpT>(filterOpId, filterOpName, filterFn);

    OpIDT filteredStatJoinOpId = pExecEngine->GetNextOperatorId();
    using FilteredJoinOpT =
            StaticEquiJoinOperator<UIDBasedYSBTypedProjectedEvent, uint64_t, UIDBasedYSBFilteredCampaignAdEvent,
                    uint64_t, UIDBasedYSBTypedAdIdExtractFunction, UIDBasedYSBFilteredEqJoinFunction>;
    auto uPtrFilteredStatEqJoinOp = std::make_unique<FilteredJoinOpT>(filteredStatJoinOpId,
            GetSuffixedOperatorName(filteredStatEqJoinOpName, jobIdSuffix), UIDBasedYSBTypedAdIdExtractFunction{},
            UIDBasedYSBFilteredEqJoinFunction{0}, adIdCampaignIdMap);

    OpIDT filteredWindowOpId = pExecEngine->GetNextOperatorId();
    using FilteredWindowOpT = FixedEventTimeWindowOperator<UIDBasedYSBFilteredCampaignAdEvent,
            UIDBasedYSBFilteredWinEmitEvent, true, UIDBasedYSBFilteredCampaignAggFunction>;
    auto uPtrFilteredWindowOp = std::make_unique<FilteredWindowOpT>(filteredWindowOpId,
            GetSuffixedOperatorName(filteredWindowOpName, jobIdSuffix), UIDBasedYSBFilteredCampaignAggFunction{0},
            std::chrono::seconds(10));

    OpIDT windowCoGroupOpId = pExecEngine->GetNextOperatorId();
    using WindowCoGroupOpT = FixedEventTimeWindowCoGroupOperator<UIDBasedYSBWinEmitEvent,
            UIDBasedYSBFilteredWinEmitEvent, UIDBasedYSBWinJoinEmitEvent, UIDBasedYSBWindowCoGroupFunction>;
    auto uPtrWindowCoGroupOp = std::make_unique<WindowCoGroupOpT>(windowCoGroupOpId,
            GetSuffixedOperatorName(windowCoGroupOpName, jobIdSuffix), UIDBasedYSBWindowCoGroupFunction{},
            std::chrono::seconds(10));

    OpIDT sinkOpId = pExecEngine->GetNextOperatorId();
    auto sinkFn = UIDBasedNoOpSinkFunction<UIDBasedYSBWinJoinEmitEvent>{};
    using SinkOpT = GenericSinkOperator<UIDBasedYSBWinJoinEmitEvent,
            UIDBasedNoOpSinkFunction<UIDBasedYSBWinJoinEmitEvent>, Duration>;
    auto uPtrSinkOp = std::make_unique<SinkOpT>(sinkOpId, GetSuffixedOperatorName(sinkOpName, jobIdSuffix), sinkFn);

    auto projectedStream = srcDataStream.AddOperator(std::move(uPtrProjectOp));
    auto leftStream = projectedStream.AddOperator(std::move(uPtrStatEqJoinOp)).AddOperator(std::move(uPtrWindowOp));
    auto rightStream = projectedStream.AddOperator(std::move(uPtrFilteredStatEqJoinOp))
                               .AddOperator(std::move(uPtrFilteredWindowOp));
    static_assert(std::is_same_v<decltype(rightStream), enjima::runtime::DataStream<FilteredWindowOpT>>);
    leftStream
            .template AddJoinOperator<WindowCoGroupOpT, FilteredWindowOpT>(std::move(uPtrWindowCoGroupOp), rightStream)
            .template AddSinkOperator<Duration, SinkOpT>(std::move(uPtrSinkOp));
    streamingJob.SetProcessingMode(processingMode);
    return streamingJob;
}

template<typename Duration>
JobT SetUpFixedRateSlidingWindowedPipelineForMultiQueryRun(enjima::runtime::ExecutionEngine* pExecEngine,
        const std::string& srcOpName, const std::string& projectOpName, const std::string& statEqJoinOpName,
        const std::string& windowOpName, const std::string& sinkOpName, uint64_t inputRate,
        const std::string& jobIdSuffix, enjima::runtime::StreamingTask::ProcessingMode processingMode,
        uint64_t reservoirCapacity)
{
    JobT streamingJob;
    OpIDT srcOpId = pExecEngine->GetNextOperatorId();
    auto uPtrSrcOp = std::make_unique<FixedRateLatencyTrackingUIDBasedYSBSource<Duration>>(srcOpId,
            GetSuffixedOperatorName(srcOpName, jobIdSuffix), 50, inputRate, reservoirCapacity);
    uPtrSrcOp->PopulateEventCache(1048576, 100, 10);
    auto adIdCampaignIdMap = uPtrSrcOp->GetAdIdToCampaignIdMap();

    enjima::runtime::DataStream<FixedRateLatencyTrackingUIDBasedYSBSource<Duration>> srcDataStream(std::move(uPtrSrcOp),
            streamingJob);
    OpIDT projectOpId = pExecEngine->GetNextOperatorId();
    using MapOpT = MapOperator<UIDBasedYSBAdEvent, UIDBasedYSBProjectedEvent, UIDBasedYSBProjectFunction>;
    auto uPtrProjectOp = std::make_unique<MapOpT>(projectOpId, GetSuffixedOperatorName(projectOpName, jobIdSuffix),
            UIDBasedYSBProjectFunction{});

    OpIDT statJoinOpId = pExecEngine->GetNextOperatorId();
    using JoinOpT = StaticEquiJoinOperator<UIDBasedYSBProjectedEvent, uint64_t, UIDBasedYSBCampaignAdEvent, uint64_t,
            UIDBasedYSBAdIdExtractFunction, UIDBasedYSBEqJoinFunction>;
    auto uPtrStatEqJoinOp =
            std::make_unique<JoinOpT>(statJoinOpId, GetSuffixedOperatorName(statEqJoinOpName, jobIdSuffix),
                    UIDBasedYSBAdIdExtractFunction{}, UIDBasedYSBEqJoinFunction{}, adIdCampaignIdMap);

    OpIDT windowOpId = pExecEngine->GetNextOperatorId();
    auto windowAggFn =
            enjima::api::MergeableKeyedAggregateFunction<UIDBasedYSBCampaignAdEvent, UIDBasedYSBWinEmitEvent>{};
    using WindowOpT =
            SlidingEventTimeWindowOperator<UIDBasedYSBCampaignAdEvent, UIDBasedYSBWinEmitEvent, decltype(windowAggFn)>;
    auto uPtrWindowOp = std::make_unique<WindowOpT>(windowOpId, GetSuffixedOperatorName(windowOpName, jobIdSuffix),
            windowAggFn, std::chrono::seconds(10), std::chrono::seconds(2));

    OpIDT sinkOpId = pExecEngine->GetNextOperatorId();
    auto sinkFn = UIDBasedNoOpSinkFunction<UIDBasedYSBWinEmitEvent>{};
    using SinkOpT =
            GenericSinkOperator<UIDBasedYSBWinEmitEvent, UIDBasedNoOpSinkFunction<UIDBasedYSBWinEmitEvent>, Duration>;
    auto uPtrSinkOp = std::make_unique<SinkOpT>(sinkOpId, GetSuffixedOperatorName(sinkOpName, jobIdSuffix), sinkFn);

    srcDataStream.AddOperator(std::move(uPtrProjectOp))
            .AddOperator(std::move(uPtrStatEqJoinOp))
            .AddOperator(std::move(uPtrWindowOp))
            .template AddSinkOperator<Duration>(std::move(uPtrSinkOp));
    streamingJob.SetProcessingMode(processingMode);
    return streamingJob;
}