package joining.parallel.parallelization.lockfree;

import config.JoinConfig;
import config.LoggingConfig;
import config.ParallelConfig;
import config.StartupConfig;
import expressions.compilation.KnaryBoolEval;
import joining.parallel.plan.LeftDeepPartitionPlan;
import joining.progress.ProgressTracker;
import joining.result.ResultTuple;
import joining.parallel.join.DPJoin;
import joining.parallel.join.ModJoin;
import joining.parallel.parallelization.EndPlan;
import joining.parallel.parallelization.Parallelization;
import joining.parallel.progress.ParallelProgressTracker;
import joining.parallel.threads.ThreadPool;
import joining.parallel.uct.DPNode;
import joining.result.UniqueJoinResult;
import logs.LogUtils;
import net.sf.jsqlparser.expression.Expression;
import predicate.NonEquiNode;
import preprocessing.Context;
import query.QueryInfo;
import statistics.JoinStats;
import statistics.QueryStats;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public class LockFreeParallelization extends Parallelization {
    /**
     * Multiple join operators for threads
     */
    private final List<DPJoin> dpJoins = new ArrayList<>();
    private final List<DPJoin> optimalJoins = new ArrayList<>();
    /**
     * initialization of parallelization
     *
     * @param nrThreads the number of threads
     * @param query     select query with join predicates
     * @param context   query execution context
     */
    public LockFreeParallelization(int nrThreads, int budget, QueryInfo query, Context context) throws Exception {
        super(nrThreads, budget, query, context);
        // Compile predicates
        Map<Expression, NonEquiNode> predToEval = new HashMap<>();
        Map<Expression, KnaryBoolEval> predToComp = new HashMap<>();
        for (int i = 0; i < query.nonEquiJoinNodes.size(); i++) {
            // Compile predicate and store in lookup table
            Expression pred = query.nonEquiJoinPreds.get(i).finalExpression;
            NonEquiNode node = query.nonEquiJoinNodes.get(i);
            predToEval.put(pred, node);
        }
        // Initialize multi-way join operator
        int nrTables = query.nrJoined;
        int nrSplits = query.equiJoinPreds.size() + nrTables;
        Map<Integer, LeftDeepPartitionPlan> planCache = new ConcurrentHashMap<>();
        if (JoinConfig.NEWTRACKER && nrThreads > 1 && ParallelConfig.PARALLEL_SPEC == 0) {
            ParallelProgressTracker tracker = new ParallelProgressTracker(nrTables, nrThreads, nrSplits);
            for (int i = 0; i < nrThreads; i++) {
                ModJoin modJoin = new ModJoin(query, context, budget, nrThreads, i, predToEval, predToComp, planCache);
                modJoin.tracker = tracker;
                dpJoins.add(modJoin);
            }
        }
        else if (ParallelConfig.PARALLEL_SPEC == 13) {
            for (int i = 0; i < nrThreads; i++) {
                ModJoin modJoin = new ModJoin(query, context, budget, nrThreads, i, predToEval, predToComp, planCache);
                modJoin.trackers = new ProgressTracker[nrTables];
                for (int table = 0; table < nrTables; table++) {
                    modJoin.trackers[table] = new ProgressTracker(nrTables, modJoin.cardinalities);
                }
                dpJoins.add(modJoin);
            }
        }
        else {
            for (int i = 0; i < nrThreads; i++) {
                ModJoin modJoin = new ModJoin(query, context, budget, nrThreads, i, predToEval, predToComp, planCache);
                modJoin.oldTracker = new ProgressTracker(nrTables, modJoin.cardinalities);
                dpJoins.add(modJoin);
            }
        }

        if (JoinConfig.NEWTRACKER && nrThreads > 1 && ParallelConfig.PARALLEL_SPEC == 0) {
            ParallelProgressTracker tracker = new ParallelProgressTracker(nrTables, nrThreads, nrSplits);
            for (int i = 0; i < nrThreads; i++) {
                ModJoin modJoin = new ModJoin(query, context, budget, nrThreads, i, predToEval, predToComp, planCache);
                modJoin.tracker = tracker;
                optimalJoins.add(modJoin);
            }
        }
        else if (ParallelConfig.PARALLEL_SPEC == 13) {
            for (int i = 0; i < nrThreads; i++) {
                ModJoin modJoin = new ModJoin(query, context, budget, nrThreads, i, predToEval, predToComp, planCache);
                modJoin.trackers = new ProgressTracker[nrTables];
                for (int table = 0; table < nrTables; table++) {
                    modJoin.trackers[table] = new ProgressTracker(nrTables, modJoin.cardinalities);
                }
                optimalJoins.add(modJoin);
            }
        }
        else {
            for (int i = 0; i < nrThreads; i++) {
                ModJoin modJoin = new ModJoin(query, context, budget, nrThreads, i, predToEval, predToComp, planCache);
                modJoin.oldTracker = new ProgressTracker(nrTables, modJoin.cardinalities);
                optimalJoins.add(modJoin);
            }
        }
    }

    @Override
    public void execute(Set<ResultTuple> resultList) throws Exception {
        // Initialize UCT join order search tree.
        DPNode root = new DPNode(0, query, JoinConfig.AVOID_CARTESIAN, nrThreads);
        // Initialize a thread pool.
        ExecutorService executorService = ThreadPool.executorService;
        // Initialize variables for broadcasting.
        int nrTables = query.nrJoined;
        // Initialize an end plan.
        EndPlan endPlan = new EndPlan(nrThreads, nrTables, root);
        List<LockFreeTask> tasks = new ArrayList<>();
        // Mutex shared by multiple threads.
        ReentrantLock lock = new ReentrantLock();
        AtomicBoolean end = new AtomicBoolean(false);
        AtomicBoolean finish = new AtomicBoolean(false);
        // logs list
        List<String>[] logs = new List[nrThreads];
        for (int i = 0; i < nrThreads; i++) {
            logs[i] = new ArrayList<>();
        }
        if (nrThreads == 1) {
            JoinConfig.PARALLEL_WEIGHT = JoinConfig.EXPLORATION_WEIGHT;
        }
        for (int i = 0; i < nrThreads; i++) {
            LockFreeTask lockFreeTask = new LockFreeTask(query, context, root, endPlan,
                    end, finish, lock, dpJoins.get(i));
            tasks.add(lockFreeTask);
        }
        long executionStart1 = System.currentTimeMillis();
        List<Future<LockFreeResult>> futures = executorService.invokeAll(tasks);
        long executionEnd1 = System.currentTimeMillis();
        JoinStats.exeTime = executionEnd1 - executionStart1;
        // Optimal run
        // Initialize an end plan.
//        EndPlan endPlan2 = new EndPlan(nrThreads, nrTables, root);
//        List<LockFreeTask> tasks2 = new ArrayList<>();
//        // Mutex shared by multiple threads.
//        ReentrantLock lock2 = new ReentrantLock();
//        AtomicBoolean end2 = new AtomicBoolean(false);
//        AtomicBoolean finish2 = new AtomicBoolean(false);
//        int[] optimal = endPlan.getJoinOrder();
//        for (int thread = 0; thread < nrThreads; thread++) {
//            LockFreeTask lockFreeTask = new LockFreeTask(query, context, root, endPlan2, end2,
//                    finish2, lock2, optimalJoins.get(thread));
//            System.arraycopy(optimal, 0, lockFreeTask.optimal, 0 , nrTables);
//            tasks2.add(lockFreeTask);
//        }
//
//        long executionStart = System.currentTimeMillis();
//        List<Future<LockFreeResult>> futures = executorService.invokeAll(tasks2);
//        long executionEnd = System.currentTimeMillis();
//        JoinStats.exeTime = executionEnd - executionStart;

        int maxSize = 0;
        context.resultTuplesList = new ArrayList<>(nrThreads);
        for (int futureCtr = 0; futureCtr < nrThreads; futureCtr++) {
            try {
                LockFreeResult result = futures.get(futureCtr).get();
                maxSize += result.result.size();
                // collect results
                context.resultTuplesList.add(result.result);
                if (LoggingConfig.PARALLEL_JOIN_VERBOSE) {
                    logs[result.id] = result.logs;
                }
                UniqueJoinResult uniqueJoinResult = dpJoins.get(futureCtr).uniqueJoinResult;
                if (uniqueJoinResult != null) {
                    if (context.uniqueJoinResult == null) {
                        context.uniqueJoinResult = uniqueJoinResult;
                    }
                    else {
                        context.uniqueJoinResult.merge(uniqueJoinResult);
                    }
                }
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
        context.maxSize = maxSize;
//        List<int[]> resultArrayList = new ArrayList<>(maxSize);
//        // Generate group keys
//        futures.forEach(futureResult -> {
//            try {
//                LockFreeResult result = futureResult.get();
//                for (ResultTuple resultTuple: result.result) {
//                    if (resultList.add(resultTuple)) {
//                        resultArrayList.add(resultTuple.baseIndices);
//                    }
//                }
//                if (LoggingConfig.PARALLEL_JOIN_VERBOSE) {
//                    logs[result.id] = result.logs;
//                }
//
//            } catch (InterruptedException | ExecutionException e) {
//                e.printStackTrace();
//            }
//        });
//        context.resultList = resultArrayList;
        long nrSamples = 0;
        for (DPJoin joinOp: dpJoins) {
            nrSamples = Math.max(joinOp.roundCtr, nrSamples);
            JoinStats.nrTuples = Math.max(joinOp.statsInstance.nrTuples, JoinStats.nrTuples);
        }
        JoinStats.nrSamples = nrSamples;
        // Write log to the local file.
        if (LoggingConfig.PARALLEL_JOIN_VERBOSE) {
            LogUtils.writeLogs(logs, "verbose/lockFree/" + QueryStats.queryName);
        }

        long size = resultList.size();
        // memory consumption
        if (StartupConfig.Memory) {
            JoinStats.treeSize = root.getSize();
            if (ParallelConfig.PARALLEL_SPEC == 0 && nrThreads == 1) {
                JoinStats.stateSize = ((ModJoin)dpJoins.get(0)).oldTracker.getSize();
            }
            else {
                JoinStats.stateSize = ((ModJoin)dpJoins.get(0)).tracker.getSize();
            }
            JoinStats.joinSize = size * nrTables * 4;
        }
    }
}
