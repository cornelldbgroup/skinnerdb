package joining.parallel.parallelization.hybrid;

import config.JoinConfig;
import config.LoggingConfig;
import config.ParallelConfig;
import expressions.compilation.KnaryBoolEval;
import joining.parallel.indexing.OffsetIndex;
import joining.parallel.join.DPJoin;
import joining.parallel.join.ModJoin;
import joining.parallel.join.OldJoin;
import joining.parallel.parallelization.Parallelization;
import joining.parallel.parallelization.search.SPTask;
import joining.parallel.parallelization.search.SearchResult;
import joining.parallel.plan.LeftDeepPartitionPlan;
import joining.parallel.progress.ParallelProgressTracker;
import joining.parallel.threads.ThreadPool;
import joining.parallel.uct.SyncNode;
import joining.result.ResultTuple;
import joining.result.UniqueJoinResult;
import joining.uct.SelectionPolicy;
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

public class HybridParallelization extends Parallelization {
    /**
     * Multiple join operators for threads
     */
    private List<OldJoin> oldJoins = new ArrayList<>();
    /**
     * initialization of parallelization
     *
     * @param nrThreads the number of threads
     * @param budget
     * @param query     select query with join predicates
     * @param context   query execution context
     */
    public HybridParallelization(int nrThreads, int budget,
                                 QueryInfo query, Context context) throws Exception {
        super(nrThreads, budget, query, context);
        // Compile predicates
        Map<Expression, NonEquiNode> predToEval = new HashMap<>();
        int nrJoined = query.nrJoined;
        int nrSPThreads = ParallelConfig.SEARCH_THREADS;
        OffsetIndex[][] threadOffsets = new OffsetIndex[nrSPThreads][nrJoined];
        for (int i = 0; i < query.nonEquiJoinNodes.size(); i++) {
            // Compile predicate and store in lookup table
            Expression pred = query.nonEquiJoinPreds.get(i).finalExpression;
            NonEquiNode node = query.nonEquiJoinNodes.get(i);
            predToEval.put(pred, node);
        }
        // Initialize multi-way join operator
        for (int threadCtr = 0; threadCtr < ParallelConfig.SEARCH_THREADS; threadCtr++) {
            for (int tableCtr = 0; tableCtr < nrJoined; tableCtr++) {
                threadOffsets[threadCtr][tableCtr] = new OffsetIndex();
            }
            OldJoin oldJoin = new OldJoin(query, context, budget,
                    nrSPThreads, threadCtr, predToEval, threadOffsets, null);
            oldJoins.add(oldJoin);
        }
    }

    @Override
    public void execute(Set<ResultTuple> resultList) throws Exception {
        List<String>[] logs = new List[nrThreads];
        List<Callable<SearchResult>> tasks = new ArrayList<>();
        Map<Expression, KnaryBoolEval> predToComp = new HashMap<>();
        Map<Integer, LeftDeepPartitionPlan> planCache = new ConcurrentHashMap<>();
        int nrDPThreads = ParallelConfig.EXE_THREADS - ParallelConfig.SEARCH_THREADS;
        int nrSPThreads = ParallelConfig.SEARCH_THREADS;
        int nrJoined = query.nrJoined;
        // Mutex shared by multiple threads.
        AtomicBoolean isFinished = new AtomicBoolean(false);
        // Initialize search and data parallelization task.
        AtomicReference<JoinPlan> nextJoinOrder = new AtomicReference<>();
        for (int searchCtr = 0; searchCtr < nrSPThreads; searchCtr++) {
            logs[searchCtr] = new ArrayList<>();
            OldJoin oldJoin = oldJoins.get(searchCtr);
            HSearchTask searchTask = new HSearchTask(query, context, oldJoin,
                    searchCtr, nrSPThreads, nrDPThreads, isFinished, nextJoinOrder, planCache);
            if (searchTask.runnable) {
                tasks.add(searchTask);
            }
        }
        nrSPThreads = tasks.size();
//        nrDPThreads = ParallelConfig.EXE_THREADS - nrSPThreads;
        ModJoin[] joins = new ModJoin[nrDPThreads];
        for (int dataCtr = 0; dataCtr < nrDPThreads; dataCtr++) {
            logs[nrSPThreads + dataCtr] = new ArrayList<>();
            ModJoin modJoin = new ModJoin(query, context, oldJoins.get(0).budget,
                    nrDPThreads, dataCtr, oldJoins.get(0).predToEval, predToComp, planCache);
            joins[dataCtr] = modJoin;
            HDataTask dataTask = new HDataTask(query, context, modJoin,
                    dataCtr, nrDPThreads, isFinished, nextJoinOrder);
            tasks.add(dataTask);
        }
        // Initialize a thread pool.
        ExecutorService executorService = ThreadPool.executorService;
        long executionStart = System.currentTimeMillis();
        List<Future<SearchResult>> futures = executorService.invokeAll(tasks);
        long executionEnd = System.currentTimeMillis();
        JoinStats.exeTime = executionEnd - executionStart;
        // Check whether search parallel finishes before data
        OldJoin finishedJoin = oldJoins.stream().filter(join ->
                join.lastState != null && join.lastState.isFinished()).findFirst().orElse(null);
        long finishedCount = 0;
        if (finishedJoin != null) {
            resultList.addAll(finishedJoin.concurrentList);
            finishedCount = finishedJoin.roundCtr;
        }
        else {
            context.resultTuplesList = new ArrayList<>(nrSPThreads);
        }

        int maxSize = 0;
        long avgNrEpisode = 0;
        for (int futureCtr = 0; futureCtr < tasks.size(); futureCtr++) {
            try {
                SearchResult result = futures.get(futureCtr).get();
                if (LoggingConfig.PARALLEL_JOIN_VERBOSE) {
                    int id = result.isSearch ? result.id : nrSPThreads + result.id;
                    logs[id] = result.logs;
                }
                if (!result.isSearch) {
                    if (finishedJoin == null) {
                        maxSize += result.result.size();
                        context.resultTuplesList.add(result.result);
                        UniqueJoinResult uniqueJoinResult = joins[result.id].uniqueJoinResult;
                        avgNrEpisode += joins[result.id].roundCtr;
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
        JoinStats.nrSamples = nrDPThreads == 0 ? finishedCount : avgNrEpisode / nrDPThreads;
        // Write log to the local file.
        if (LoggingConfig.PARALLEL_JOIN_VERBOSE) {
            LogUtils.writeLogs(logs, "verbose/hybrid/" + QueryStats.queryName);
        }
        System.out.println("Result Set: " + resultList.size());
    }
}