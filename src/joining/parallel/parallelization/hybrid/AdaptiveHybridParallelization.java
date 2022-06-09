package joining.parallel.parallelization.hybrid;

import config.LoggingConfig;
import config.ParallelConfig;
import expressions.compilation.KnaryBoolEval;
import joining.parallel.indexing.OffsetIndex;
import joining.parallel.join.HybridJoin;
import joining.parallel.parallelization.Parallelization;
import joining.parallel.parallelization.search.SearchResult;
import joining.parallel.plan.LeftDeepPartitionPlan;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class AdaptiveHybridParallelization extends Parallelization {
    /**
     * Multiple join operators for threads
     */
    private List<HybridJoin> hybridJoins = new ArrayList<>();
    /**
     * initialization of parallelization
     *
     * @param nrThreads the number of threads
     * @param budget
     * @param query     select query with join predicates
     * @param context   query execution context
     */
    public AdaptiveHybridParallelization(int nrThreads, int budget,
                                         QueryInfo query, Context context) throws Exception {
        super(nrThreads, budget, query, context);
        // Compile predicates
        Map<Expression, NonEquiNode> predToEval = new HashMap<>();
        int nrJoined = query.nrJoined;
        OffsetIndex[][] threadOffsets = new OffsetIndex[nrThreads][nrJoined];
        for (int i = 0; i < query.nonEquiJoinNodes.size(); i++) {
            // Compile predicate and store in lookup table
            Expression pred = query.nonEquiJoinPreds.get(i).finalExpression;
            NonEquiNode node = query.nonEquiJoinNodes.get(i);
            predToEval.put(pred, node);
        }
        Map<Expression, KnaryBoolEval> predToComp = new HashMap<>();
        Map<Integer, LeftDeepPartitionPlan> planCache = new ConcurrentHashMap<>();
        // Initialize multi-way join operator
        for (int threadCtr = 0; threadCtr < nrThreads; threadCtr++) {
            for (int tableCtr = 0; tableCtr < nrJoined; tableCtr++) {
                threadOffsets[threadCtr][tableCtr] = new OffsetIndex();
            }
            HybridJoin hybridJoin = new HybridJoin(query, context, budget,
                    nrThreads, threadCtr, predToEval, predToComp, planCache, threadOffsets);
            hybridJoins.add(hybridJoin);
        }
    }

    @Override
    public void execute(Set<ResultTuple> resultList) throws Exception {
        List<String>[] logs = new List[nrThreads];
        List<NewHybridTask> tasks = new ArrayList<>();
        Map<Expression, KnaryBoolEval> predToComp = new HashMap<>();
        Map<Integer, LeftDeepPartitionPlan> planCache = new ConcurrentHashMap<>();
        int nrThreads = ParallelConfig.EXE_THREADS;
        // Mutex shared by multiple threads.
        AtomicBoolean isFinished = new AtomicBoolean(false);
        // Mutex shared by multiple threads.
        AtomicInteger coverTid = new AtomicInteger(-1);
        // Set of data threads
        Set<Integer> dataThreads = ConcurrentHashMap.newKeySet();
//        dataThreads.add(4);dataThreads.add(5);dataThreads.add(6);
//        dataThreads.add(2);dataThreads.add(7);dataThreads.add(1);
        // Number of forget times
        AtomicInteger nrForgets = new AtomicInteger(0);
        // Initialize search and data parallelization task.
        AtomicReference<HybridJoinPlan> nextJoinOrder = new AtomicReference<>();
        // Initialize search and data parallelization task.
        AtomicReference<HybridJoinPlan>[] nextSPOrders = new AtomicReference[nrThreads];
        for (int hybridCtr = 0; hybridCtr < nrThreads; hybridCtr++) {
            logs[hybridCtr] = new ArrayList<>();
            HybridJoin hybridJoin = hybridJoins.get(hybridCtr);
//            System.out.println(Arrays.toString(hybridJoin.cardinalities));
            nextSPOrders[hybridCtr] = new AtomicReference<>(null);
            NewHybridTask hybridTask = new NewHybridTask(query, context, hybridJoin,
                    hybridCtr, nrThreads, isFinished, dataThreads, coverTid,
                    nextJoinOrder, nrForgets, nextSPOrders, planCache);
            if (!hybridTask.runnable) {
                dataThreads.add(hybridCtr);
            }
            tasks.add(hybridTask);
        }

        // Initialize a thread pool.
        ExecutorService executorService = ThreadPool.executorService;
        long executionStart = System.currentTimeMillis();
        List<Future<SearchResult>> futures = executorService.invokeAll(tasks);
        long executionEnd = System.currentTimeMillis();
        JoinStats.exeTime = executionEnd - executionStart;
        // Check whether search parallel finishes before data
        int finalDPThreads = tasks.get(0).dataThreads.size();
        HybridJoin finishedJoin = null;
        for (int threadCtr = 0; threadCtr < nrThreads; threadCtr++) {
            HybridJoin hybridJoin = hybridJoins.get(threadCtr);
            SearchResult result = futures.get(threadCtr).get();
            if (result.isSearch && hybridJoin.lastState != null && hybridJoin.lastState.isFinished()) {
                finishedJoin = hybridJoin;
                System.out.println("Finish ID: " + threadCtr);
                break;
            }
        }
        long finishedCount = 0;
        if (finishedJoin != null) {
            resultList.addAll(finishedJoin.result.tuples);
            finishedCount = finishedJoin.roundCtr;
        }
        else {
            context.resultTuplesList = new ArrayList<>(finalDPThreads);
        }

        int maxSize = 0;
        long avgNrEpisode = 0;
        int avgCtr = 0;
        for (int futureCtr = 0; futureCtr < tasks.size(); futureCtr++) {
            try {
                SearchResult result = futures.get(futureCtr).get();
                if (LoggingConfig.PARALLEL_JOIN_VERBOSE) {
                    logs[result.id] = result.logs;
                }
                if (!result.isSearch) {
                    if (result.isData) {
                        avgNrEpisode += hybridJoins.get(result.id).roundCtr;
                        avgCtr += 1;
                    }
                    if (finishedJoin == null) {
                        maxSize += result.result.size();
                        context.resultTuplesList.add(result.result);
                        UniqueJoinResult uniqueJoinResult = hybridJoins.get(result.id).uniqueJoinResult;
                        if (uniqueJoinResult != null) {
                            if (context.uniqueJoinResult == null) {
                                context.uniqueJoinResult = uniqueJoinResult;
                            } else {
                                context.uniqueJoinResult.merge(uniqueJoinResult);
                            }
                        }
                    }
                }

            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
        context.maxSize = maxSize;
        long mergeEnd = System.currentTimeMillis();
        JoinStats.mergeTime = mergeEnd - executionEnd;
        JoinStats.nrSamples = avgCtr == 0 ? 1 : avgNrEpisode / avgCtr;
        System.out.println("Samples: " + JoinStats.nrSamples);
        // Write log to the local file.
        if (LoggingConfig.PARALLEL_JOIN_VERBOSE) {
            LogUtils.writeLogs(logs, "verbose/hybrid-adaptive/" + QueryStats.queryName);
        }
        System.out.println("Result Set: " + resultList.size());
    }
}