package joining.parallel.parallelization.test;

import config.JoinConfig;
import config.LoggingConfig;
import config.ParallelConfig;
import expressions.compilation.KnaryBoolEval;
import joining.parallel.indexing.OffsetIndex;
import joining.parallel.join.ModJoin;
import joining.parallel.parallelization.Parallelization;
import joining.parallel.parallelization.hybrid.JoinPlan;
import joining.parallel.parallelization.search.SearchResult;
import joining.parallel.plan.LeftDeepPartitionPlan;
import joining.parallel.progress.ParallelProgressTracker;
import joining.parallel.threads.ThreadPool;
import joining.result.ResultTuple;
import joining.result.UniqueJoinResult;
import logs.LogUtils;
import net.sf.jsqlparser.expression.Expression;
import predicate.NonEquiNode;
import preprocessing.Context;
import query.QueryInfo;
import statistics.JoinStats;
import statistics.QueryStats;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class Sychronization extends Parallelization {
    /**
     * Join operators for sampling threads.
     */
    public final OldJoinTest oldJoin;
    /**
     * Join operators for sampling threads.
     */
    public final OldJoinTest traceOldJoin;
    /**
     * initialization of parallelization
     *
     * @param nrThreads the number of threads
     * @param query     select query with join predicates
     * @param context   query execution context
     */
    public Sychronization(int nrThreads, int budget, QueryInfo query, Context context) throws Exception {
        super(nrThreads, budget, query, context);
        // Compile predicates
        Map<Expression, NonEquiNode> predToEval = new HashMap<>();
        int nrJoined = query.nrJoined;
        for (int i = 0; i < query.nonEquiJoinNodes.size(); i++) {
            // Compile predicate and store in lookup table
            Expression pred = query.nonEquiJoinPreds.get(i).finalExpression;
            NonEquiNode node = query.nonEquiJoinNodes.get(i);
            predToEval.put(pred, node);
        }
        OffsetIndex[][] threadOffsets = new OffsetIndex[1][nrJoined];
        for (int tableCtr = 0; tableCtr < nrJoined; tableCtr++) {
            threadOffsets[0][tableCtr] = new OffsetIndex();
        }
        // Initialize multi-way join operator
        oldJoin = new OldJoinTest(query, context, budget,
                1, 0, predToEval, threadOffsets);
        // Initialize multi-way join operator
        traceOldJoin = new OldJoinTest(query, context, budget,
                1, 0, predToEval, threadOffsets);
    }

    @Override
    public void execute(Set<ResultTuple> resultList) throws Exception {
        List<String>[] logs = new List[nrThreads];
        List<Callable<SearchResult>> tasks = new ArrayList<>();
        Map<Expression, KnaryBoolEval> predToComp = new HashMap<>();
        Map<Integer, LeftDeepPartitionPlan> planCache = new ConcurrentHashMap<>();
        int nrDPThreads = ParallelConfig.EXE_THREADS - 1;
        int nrSPThreads = 1;
        int nrJoined = query.nrJoined;
        List<int[]> randomOrders = new NSPNodeTest(0, query,
                JoinConfig.AVOID_CARTESIAN, 0, 0, 1).getRandomOrders(4);
        // First run: record the trace for each thread
        // Mutex shared by multiple threads.
        AtomicBoolean isFinished = new AtomicBoolean(false);
        // Initialize search and data parallelization task.
        AtomicReference<JoinPlan> nextJoinOrder = new AtomicReference<>();
        logs[0] = new ArrayList<>();
//        roundTraces.add(new RoundTrace[10000]);
        SampleTest sampleTask = new SampleTest(query, context, oldJoin,
                0, nrSPThreads, nrDPThreads, isFinished, nextJoinOrder,
                planCache, randomOrders, true);
        tasks.add(sampleTask);

        int nrSplits = query.equiJoinPreds.size() + nrJoined;
        ModJoinTest[] joins = new ModJoinTest[nrDPThreads];
        ParallelProgressTracker tracker = new ParallelProgressTracker(nrJoined, nrDPThreads, nrSplits);
        for (int dataCtr = 0; dataCtr < nrDPThreads; dataCtr++) {
            logs[nrSPThreads + dataCtr] = new ArrayList<>();
            ModJoinTest modJoin = new ModJoinTest(query, context, oldJoin.budget,
                    nrDPThreads, dataCtr, oldJoin.predToEval, predToComp, planCache);
            joins[dataCtr] = modJoin;
            modJoin.tracker = tracker;
//            roundTraces.add(new RoundTrace[10000]);
            HDataTest dataTask = new HDataTest(query, context, modJoin,
                    dataCtr, nrDPThreads, isFinished, nextJoinOrder, randomOrders, false);
            tasks.add(dataTask);
        }

        // Initialize a thread pool.
        ExecutorService executorService = ThreadPool.executorService;
        long executionStart = System.currentTimeMillis();
        List<Future<SearchResult>> futures = executorService.invokeAll(tasks);
        long executionEnd = System.currentTimeMillis();
        JoinStats.exeTime = executionEnd - executionStart;

        int maxSize = 0;
        context.resultTuplesList = nrDPThreads == 0 ? null :
                new ArrayList<>(nrDPThreads+1);
        long avgNrEpisode = 0;
        for (int futureCtr = 0; futureCtr < nrThreads; futureCtr++) {
            try {
                SearchResult result = futures.get(futureCtr).get();
                if (!result.isSearch) {
                    maxSize += result.result.size();
                    context.resultTuplesList.add(result.result);
                    UniqueJoinResult uniqueJoinResult = joins[result.id].uniqueJoinResult;
                    long threadCtr = joins[result.id].roundCtr;
                    avgNrEpisode += threadCtr;
                    if (uniqueJoinResult != null) {
                        if (context.uniqueJoinResult == null) {
                            context.uniqueJoinResult = uniqueJoinResult;
                        }
                        else {
                            context.uniqueJoinResult.merge(uniqueJoinResult);
                        }
                    }
                }
                else {
                    if (context.resultTuplesList == null) {
                        resultList.addAll(oldJoin.concurrentList);
                    }
                    else {
                        Set<ResultTuple> resultTuples = new HashSet<>(oldJoin.concurrentList);
                        maxSize += resultTuples.size();
                        context.resultTuplesList.add(resultTuples);
                    }
                }

            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
        long totalTime = 0;
        long totalTries = 0;
        for (Callable<SearchResult> dataTest: tasks) {
            if (dataTest instanceof HDataTest) {
                totalTries++;
                totalTime += ((HDataTest)dataTest).exeTime;
            }
            else {
                totalTries++;
                totalTime += ((SampleTest)dataTest).exeTime;
            }
        }
        JoinStats.exeTime = totalTime / totalTries;
        context.maxSize = maxSize;
        long mergeEnd = System.currentTimeMillis();
        JoinStats.mergeTime = mergeEnd - executionEnd;
        JoinStats.nrSamples = nrDPThreads == 0 ? oldJoin.roundCtr : avgNrEpisode / nrDPThreads;

        // Write log to the local file.
        if (LoggingConfig.PARALLEL_JOIN_VERBOSE) {
            LogUtils.writeLogs(logs, "verbose/lockFree/" + QueryStats.queryName);
        }

//        long size = resultList.size();
//        // memory consumption
//        if (StartupConfig.Memory) {
//            JoinStats.treeSize = root.getSize();
//            if (ParallelConfig.PARALLEL_SPEC == 0 && nrThreads == 1) {
//                JoinStats.stateSize = dpJoins.get(0).oldTracker.getSize();
//            }
//            else {
//                JoinStats.stateSize = dpJoins.get(0).tracker.getSize();
//            }
//            JoinStats.joinSize = size * nrTables * 4;
//        }
    }
}
