//
// Created by m34ferna on 12/10/24.
//

#include "TestSetupHelperFunctions.h"

JobT SetUpWindowlessPipelineForMultiQueryRun(enjima::runtime::ExecutionEngine* pExecEngine,
        const std::string& srcOpName, const std::string& projectOpName, const std::string& statEqJoinOpName,
        const std::string& sinkOpName, const std::string& jobIdSuffix,
        enjima::runtime::StreamingTask::ProcessingMode processingMode)
{
    JobT streamingJob;
    OpIDT srcOpId = pExecEngine->GetNextOperatorId();
    auto uPtrSrcOp =
            std::make_unique<InMemoryYSBSourceOperator>(srcOpId, GetSuffixedOperatorName(srcOpName, jobIdSuffix));
    uPtrSrcOp->PopulateEventCache(1048576, 100, 10);
    auto adIdCampaignIdMap = uPtrSrcOp->GetAdIdToCampaignIdMap();

    enjima::runtime::DataStream<InMemoryYSBSourceOperator> srcDataStream(std::move(uPtrSrcOp), streamingJob);
    OpIDT projectOpId = pExecEngine->GetNextOperatorId();
    using MapOpT = MapOperator<YSBAdT, YSBProjT, YSBProjectFunction>;
    auto uPtrProjectOp = std::make_unique<MapOpT>(projectOpId, GetSuffixedOperatorName(projectOpName, jobIdSuffix),
            YSBProjectFunction{});

    OpIDT statJoinOpId = pExecEngine->GetNextOperatorId();
    using JoinOpT =
            StaticEquiJoinOperator<YSBProjT, uint64_t, YSBCampT, uint64_t, YSBKeyExtractFunction, YSBEqJoinFunction>;
    auto uPtrStatEquiJoinOp =
            std::make_unique<JoinOpT>(statJoinOpId, GetSuffixedOperatorName(statEqJoinOpName, jobIdSuffix),
                    YSBKeyExtractFunction{}, YSBEqJoinFunction{}, adIdCampaignIdMap);

    OpIDT sinkOpId = pExecEngine->GetNextOperatorId();
    auto sinkFn = NoOpYSBSinkFunction<YSBCampT>{};
    using SinkOpT = GenericSinkOperator<YSBCampT, NoOpYSBSinkFunction<YSBCampT>>;
    auto uPtrSinkOp = std::make_unique<SinkOpT>(sinkOpId, GetSuffixedOperatorName(sinkOpName, jobIdSuffix), sinkFn);

    srcDataStream.AddOperator(std::move(uPtrProjectOp))
            .AddOperator(std::move(uPtrStatEquiJoinOp))
            .AddSinkOperator(std::move(uPtrSinkOp));
    streamingJob.SetProcessingMode(processingMode);
    return streamingJob;
}

void ValidatePipelineResultsForMultiQueryRun(bool hasWindow, bool isWindowJoin, enjima::metrics::Profiler* pProf,
        enjima::runtime::ExecutionEngine* pExecEngine, const std::string& srcOpName, const std::string& projectOpName,
        const std::string& statEqJoinOpName, const std::string& windowOpName, const std::string& windowJoinOpName,
        const std::string& rightWindowOpName, const std::string& sinkOpName, int numQueries)
{
    uint64_t totSrcOut = 0;
    double avgLastLatency = 0.0;
    for (int i = 1; i <= numQueries; i++) {
        std::string jobIdSuffix = std::to_string(i);
        enjima::metrics::Counter<uint64_t>* timeWinOutCounter = nullptr;
        enjima::metrics::Counter<uint64_t>* rightTimeWinOutCounter = nullptr;
        enjima::metrics::Counter<uint64_t>* timeWinInCounter = nullptr;
        enjima::metrics::Counter<uint64_t>* winJoinInCounter = nullptr;
        enjima::metrics::Counter<uint64_t>* winJoinLeftInCounter = nullptr;
        enjima::metrics::Counter<uint64_t>* winJoinRightInCounter = nullptr;
        enjima::metrics::Counter<uint64_t>* winJoinOutCounter = nullptr;
        std::string suffixedWindowOpName;
        std::string suffixedWindowJoinOpName;
        std::string suffixedRightWindowOpName;
        auto suffixedSinkOpName = GetSuffixedOperatorName(sinkOpName, jobIdSuffix);
        auto sinkInCounter = pProf->GetOrCreateCounter(suffixedSinkOpName + enjima::metrics::kInCounterSuffix);
        auto sinkLatencyHistogram = pProf->GetLatencyHistogram(suffixedSinkOpName);

        if (hasWindow) {
            suffixedWindowOpName = GetSuffixedOperatorName(windowOpName, jobIdSuffix);
            timeWinOutCounter = pProf->GetOrCreateCounter(suffixedWindowOpName + enjima::metrics::kOutCounterSuffix);
            timeWinInCounter = pProf->GetOrCreateCounter(suffixedWindowOpName + enjima::metrics::kInCounterSuffix);
            if (isWindowJoin) {
                suffixedRightWindowOpName = GetSuffixedOperatorName(rightWindowOpName, jobIdSuffix);
                suffixedWindowJoinOpName = GetSuffixedOperatorName(windowJoinOpName, jobIdSuffix);
                rightTimeWinOutCounter =
                        pProf->GetOrCreateCounter(suffixedRightWindowOpName + enjima::metrics::kOutCounterSuffix);
                winJoinInCounter =
                        pProf->GetOrCreateCounter(suffixedWindowJoinOpName + enjima::metrics::kInCounterSuffix);
                winJoinLeftInCounter =
                        pProf->GetOrCreateCounter(suffixedWindowJoinOpName + enjima::metrics::kLeftInCounterSuffix);
                winJoinRightInCounter =
                        pProf->GetOrCreateCounter(suffixedWindowJoinOpName + enjima::metrics::kRightInCounterSuffix);
                winJoinOutCounter =
                        pProf->GetOrCreateCounter(suffixedWindowJoinOpName + enjima::metrics::kOutCounterSuffix);
            }
        }

        auto suffixedEqJoinOpName = GetSuffixedOperatorName(statEqJoinOpName, jobIdSuffix);
        auto eqJoinOutCounter = pProf->GetOrCreateCounter(suffixedEqJoinOpName + enjima::metrics::kOutCounterSuffix);
        auto eqJoinInCounter = pProf->GetOrCreateCounter(suffixedEqJoinOpName + enjima::metrics::kInCounterSuffix);

        auto suffixedPrjOpName = GetSuffixedOperatorName(projectOpName, jobIdSuffix);
        auto prjOutCounter = pProf->GetOrCreateCounter(suffixedPrjOpName + enjima::metrics::kOutCounterSuffix);
        auto prjInCounter = pProf->GetOrCreateCounter(suffixedPrjOpName + enjima::metrics::kInCounterSuffix);

        auto suffixedSrcOpName = GetSuffixedOperatorName(srcOpName, jobIdSuffix);
        auto srcOutCounter = pProf->GetOrCreateCounter(suffixedSrcOpName + enjima::metrics::kOutCounterSuffix);

        EXPECT_EQ(pExecEngine->GetNumActiveJobs(), 0);

        EXPECT_GT(srcOutCounter->GetCount(), 0);
        EXPECT_EQ(prjInCounter->GetCount(), srcOutCounter->GetCount());
        EXPECT_EQ(prjOutCounter->GetCount(), prjInCounter->GetCount());
        EXPECT_EQ(eqJoinInCounter->GetCount(), prjOutCounter->GetCount());
        EXPECT_EQ(eqJoinOutCounter->GetCount(), eqJoinInCounter->GetCount());
        if (hasWindow) {
            EXPECT_EQ(timeWinInCounter->GetCount(), eqJoinOutCounter->GetCount());
            EXPECT_LE(timeWinOutCounter->GetCount(), timeWinInCounter->GetCount());
            if (isWindowJoin) {
                EXPECT_EQ(winJoinLeftInCounter->GetCount(), timeWinOutCounter->GetCount());
                EXPECT_EQ(winJoinRightInCounter->GetCount(), rightTimeWinOutCounter->GetCount());
                EXPECT_EQ(winJoinInCounter->GetCount(),
                        winJoinLeftInCounter->GetCount() + winJoinRightInCounter->GetCount());
                EXPECT_LE(winJoinOutCounter->GetCount(), winJoinInCounter->GetCount());
                EXPECT_EQ(sinkInCounter->GetCount(), winJoinOutCounter->GetCount());
            }
            else {
                EXPECT_EQ(sinkInCounter->GetCount(), timeWinOutCounter->GetCount());
            }
        }
        else {
            EXPECT_EQ(sinkInCounter->GetCount(), eqJoinOutCounter->GetCount());
        }

        std::cout << "Count for " << suffixedSrcOpName << "(out) : " << srcOutCounter->GetCount() << std::endl;
        std::cout << "Count for " << suffixedEqJoinOpName << "(in) : " << eqJoinInCounter->GetCount() << std::endl;
        std::cout << "Count for " << suffixedEqJoinOpName << "(out) : " << eqJoinOutCounter->GetCount() << std::endl;
        std::cout << "Count for " << suffixedSinkOpName << "(in) : " << sinkInCounter->GetCount() << std::endl;
        std::cout << "Last latency average for " << sinkOpName << " : " << sinkLatencyHistogram->GetAverage()
                  << std::endl;
        std::cout << std::endl;
        totSrcOut += srcOutCounter->GetCount();
        avgLastLatency += sinkLatencyHistogram->GetAverage();
    }
    avgLastLatency /= static_cast<double>(numQueries);
    std::cout << "Count for " << srcOpName << "(out) : " << totSrcOut << std::endl;
    std::cout << "Avg. last latency average for " << sinkOpName << " : " << avgLastLatency << std::endl;
    std::cout << std::endl;
}

// Single query related functions
JobT SetUpBasicWindowlessPipelineNoAsserts(enjima::runtime::ExecutionEngine* pExecEngine, const std::string& srcOpName,
        const std::string& projectOpName, const std::string& statEqJoinOpName, const std::string& sinkOpName,
        enjima::runtime::StreamingTask::ProcessingMode processingMode)
{
    JobT streamingJob;
    OpIDT srcOpId = pExecEngine->GetNextOperatorId();
    auto uPtrSrcOp = std::make_unique<InMemoryYSBSourceOperator>(srcOpId, srcOpName);
    uPtrSrcOp->PopulateEventCache(1048576, 100, 10);
    auto adIdCampaignIdMap = uPtrSrcOp->GetAdIdToCampaignIdMap();

    enjima::runtime::DataStream<InMemoryYSBSourceOperator> srcDataStream(std::move(uPtrSrcOp), streamingJob);
    OpIDT projectOpId = pExecEngine->GetNextOperatorId();
    using MapOpT = MapOperator<YSBAdT, YSBProjT, YSBProjectFunction>;
    auto uPtrProjectOp = std::make_unique<MapOpT>(projectOpId, projectOpName, YSBProjectFunction{});

    OpIDT statJoinOpId = pExecEngine->GetNextOperatorId();
    using JoinOpT =
            StaticEquiJoinOperator<YSBProjT, uint64_t, YSBCampT, uint64_t, YSBKeyExtractFunction, YSBEqJoinFunction>;
    auto uPtrStatEquiJoinOp = std::make_unique<JoinOpT>(statJoinOpId, statEqJoinOpName, YSBKeyExtractFunction{},
            YSBEqJoinFunction{}, adIdCampaignIdMap);

    OpIDT sinkOpId = pExecEngine->GetNextOperatorId();
    auto sinkFn = NoOpYSBSinkFunction<YSBCampT>{};
    using SinkOpT = GenericSinkOperator<YSBCampT, NoOpYSBSinkFunction<YSBCampT>, std::chrono::milliseconds>;
    auto uPtrSinkOp = std::make_unique<SinkOpT>(sinkOpId, sinkOpName, sinkFn);

    srcDataStream.AddOperator(std::move(uPtrProjectOp))
            .AddOperator(std::move(uPtrStatEquiJoinOp))
            .AddSinkOperator<std::chrono::milliseconds>(std::move(uPtrSinkOp));
    streamingJob.SetProcessingMode(processingMode);
    return streamingJob;
}

void ValidatePipelineResults(bool hasWindow, enjima::metrics::Profiler* pProf,
        enjima::runtime::ExecutionEngine* pExecEngine, const std::string& srcOpName, const std::string& projectOpName,
        const std::string& statEqJoinOpName, const std::string& windowOpName, const std::string& sinkOpName)
{
    enjima::metrics::Counter<uint64_t>* winOutCounter = nullptr;
    enjima::metrics::Counter<uint64_t>* winInCounter = nullptr;
    auto sinkInCounter = pProf->GetOrCreateCounter(sinkOpName + enjima::metrics::kInCounterSuffix);
    auto sinkLatencyHistogram = pProf->GetLatencyHistogram(sinkOpName);
    if (hasWindow) {
        winOutCounter = pProf->GetOrCreateCounter(windowOpName + enjima::metrics::kOutCounterSuffix);
        winInCounter = pProf->GetOrCreateCounter(windowOpName + enjima::metrics::kInCounterSuffix);
    }
    auto eqJoinOutCounter = pProf->GetOrCreateCounter(statEqJoinOpName + enjima::metrics::kOutCounterSuffix);
    auto eqJoinInCounter = pProf->GetOrCreateCounter(statEqJoinOpName + enjima::metrics::kInCounterSuffix);
    auto prjOutCounter = pProf->GetOrCreateCounter(projectOpName + enjima::metrics::kOutCounterSuffix);
    auto prjInCounter = pProf->GetOrCreateCounter(projectOpName + enjima::metrics::kInCounterSuffix);
    auto srcOutCounter = pProf->GetOrCreateCounter(srcOpName + enjima::metrics::kOutCounterSuffix);

    EXPECT_EQ(pExecEngine->GetNumActiveJobs(), 0);

    EXPECT_GT(srcOutCounter->GetCount(), 0);
    EXPECT_EQ(prjInCounter->GetCount(), srcOutCounter->GetCount());
    EXPECT_EQ(prjOutCounter->GetCount(), prjInCounter->GetCount());
    EXPECT_EQ(eqJoinInCounter->GetCount(), prjOutCounter->GetCount());
    EXPECT_EQ(eqJoinOutCounter->GetCount(), eqJoinInCounter->GetCount());
    if (hasWindow) {
        EXPECT_EQ(winInCounter->GetCount(), eqJoinOutCounter->GetCount());
        EXPECT_LE(winOutCounter->GetCount(), winInCounter->GetCount());
        EXPECT_EQ(sinkInCounter->GetCount(), winOutCounter->GetCount());
    }
    else {
        EXPECT_EQ(sinkInCounter->GetCount(), eqJoinOutCounter->GetCount());
    }

    std::cout << "Count for " << srcOpName << "(out) : " << srcOutCounter->GetCount() << std::endl;
    std::cout << "Count for " << statEqJoinOpName << "(in) : " << eqJoinInCounter->GetCount() << std::endl;
    std::cout << "Count for " << statEqJoinOpName << "(out) : " << eqJoinOutCounter->GetCount() << std::endl;
    std::cout << "Count for " << sinkOpName << "(in) : " << sinkInCounter->GetCount() << std::endl;
    std::cout << "Last latency average for " << sinkOpName << " : " << sinkLatencyHistogram->GetAverage() << std::endl;
    std::cout << std::endl;
}

std::vector<std::string> GetLastLineAsTokenizedVectorFromFile(const std::string& filename,
        enjima::metrics::Profiler* pProf)
{
    pProf->FlushMetricsLogger();
    std::ifstream inputFileStream(filename);
    EXPECT_TRUE(inputFileStream.is_open());
    std::string line;
    std::string prevLine;
    bool endOfFile = false;
    while (!endOfFile) {
        prevLine = line;
        if (!std::getline(inputFileStream, line)) {
            endOfFile = true;
        }
    }
    std::vector<std::string> tokenizedStrVec;
    tokenizedStrVec.reserve(10);
    std::string delim = ",";
    size_t pos;
    std::string token;
    while ((pos = prevLine.find(delim)) != std::string::npos) {
        token = prevLine.substr(0, pos);
        tokenizedStrVec.push_back(token);
        prevLine.erase(0, pos + delim.length());
    }
    tokenizedStrVec.push_back(prevLine);
    return tokenizedStrVec;
}

std::vector<std::string> GetTargetValuesAsTokenizedVectorFromFile(const std::string& filename,
        enjima::metrics::Profiler* pProf, const std::string& targetVal, size_t targetIdx, size_t metricNameIdx)
{
    pProf->FlushMetricsLogger();
    auto metricsLoggerThreadId = pProf->GetMetricsLoggerThreadId();
    auto threadIdStr = std::to_string(metricsLoggerThreadId);
    std::ifstream inputFileStream(filename);
    EXPECT_TRUE(inputFileStream.is_open());
    std::vector<std::string> matchedLines;
    std::string line;
    while (std::getline(inputFileStream, line)) {
        if (line.find(threadIdStr) != std::string::npos && line.find(targetVal) != std::string::npos) {
            matchedLines.push_back(line);
        }
    }
    std::vector<std::string> targetValueVec;
    std::vector<std::string> tokenizedStrVec;
    tokenizedStrVec.reserve(10);
    std::string delim = ",";
    size_t pos;
    std::string token;
    for (auto& matchedLine: matchedLines) {
        tokenizedStrVec.clear();
        while ((pos = matchedLine.find(delim)) != std::string::npos) {
            token = matchedLine.substr(0, pos);
            tokenizedStrVec.push_back(token);
            matchedLine.erase(0, pos + delim.length());
        }
        tokenizedStrVec.push_back(matchedLine);
        if (tokenizedStrVec.at(metricNameIdx) == targetVal) {
            targetValueVec.push_back(tokenizedStrVec.at(targetIdx));
        }
    }
    return targetValueVec;
}

void PrintAveragedLatencyResultsForMultiQueryRun(enjima::metrics::Profiler* pProf, const std::string& sinkOpName,
        int numQueries)
{
    double overallAvgLatencySum = 0;
    for (int q = 1; q <= numQueries; q++) {
        auto targetMetricName = GetSuffixedOperatorName(sinkOpName, std::to_string(q)).append("_latency_histogram");
        auto avgLatencyValues =
                GetTargetValuesAsTokenizedVectorFromFile("metrics/latency.csv", pProf, targetMetricName, 4, 3);
        if (!avgLatencyValues.empty()) {
            uint64_t avgLatencySumForQ = 0;
            uint64_t avgLatencyCountForQ = 0;
            for (size_t i = 1; i < (avgLatencyValues.size() - 1); i++) {
                avgLatencySumForQ += std::stoul(avgLatencyValues.at(i));
                avgLatencyCountForQ++;
            }
            double overallAvgLatencyForQ =
                    static_cast<double>(avgLatencySumForQ) / static_cast<double>(avgLatencyCountForQ);
            overallAvgLatencySum += overallAvgLatencyForQ;
            std::cout << "Avg. latency average for " << targetMetricName << " : " << overallAvgLatencyForQ << std::endl;
        }
    }
    std::cout << "Avg. latency average for " << numQueries
              << " queries : " << static_cast<double>(overallAvgLatencySum) / static_cast<double>(numQueries)
              << std::endl;
    std::cout << std::endl;
}

void PrintAveragedThroughputResultsForMultiQueryRun(enjima::metrics::Profiler* pProf, const std::string& srcOpName,
        int numQueries)
{
    std::cout << std::fixed << std::setprecision(2) << "Setting format to 'fixed' and precision to '2'" << std::endl;
    double overallTpSum = 0;
    for (int q = 1; q <= numQueries; q++) {
        auto targetMetricName = GetSuffixedOperatorName(srcOpName, std::to_string(q)).append("_outThroughput_gauge");
        auto tpValues =
                GetTargetValuesAsTokenizedVectorFromFile("metrics/throughput.csv", pProf, targetMetricName, 4, 3);
        if (!tpValues.empty()) {
            double tpSumForQ = 0;
            uint64_t tpCountForQ = 0;
            // We discard the first and the last value
            for (size_t i = 1; i < (tpValues.size() - 1); i++) {
                tpSumForQ += std::stod(tpValues.at(i));
                tpCountForQ++;
            }
            double avgTpForQ = tpSumForQ / static_cast<double>(tpCountForQ);
            overallTpSum += avgTpForQ;
            std::cout << "Avg. throughput for " << targetMetricName << " : " << avgTpForQ << std::endl;
        }
    }
    std::cout << "Sum throughput for " << numQueries << " queries : " << overallTpSum << std::endl;
    std::cout << std::endl;
}
