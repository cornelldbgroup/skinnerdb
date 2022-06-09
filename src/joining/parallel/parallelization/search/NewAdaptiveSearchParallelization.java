package joining.parallel.parallelization.search;

import config.LoggingConfig;
import joining.parallel.indexing.OffsetIndex;
import joining.parallel.join.OldJoin;
import joining.parallel.parallelization.Parallelization;
import joining.parallel.parallelization.hybrid.JoinPlan;
import joining.parallel.threads.ThreadPool;
import joining.parallel.uct.ASPNode;
import joining.parallel.uct.NASPNode;
import joining.result.ResultTuple;
import logs.LogUtils;
import net.sf.jsqlparser.expression.Expression;
import predicate.NonEquiNode;
import preprocessing.Context;
import query.QueryInfo;
import statistics.JoinStats;
import statistics.QueryStats;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class NewAdaptiveSearchParallelization extends Parallelization {
    /**
     * Multiple join operators for threads
     */
    private List<OldJoin> oldJoins = new ArrayList<>();
    /**
     * initialization of parallelization
     *
     * @param nrThreads the number of threads
     * @param budget    the budget per episode
     * @param query     select query with join predicates
     * @param context   query execution context
     */
    public NewAdaptiveSearchParallelization(int nrThreads, int budget,
                                            QueryInfo query, Context context,
                                            Set<ResultTuple> resultSet) throws Exception {
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
        // Initialize multi-way join operator
        for (int threadCtr = 0; threadCtr < nrThreads; threadCtr++) {
            for (int tableCtr = 0; tableCtr < nrJoined; tableCtr++) {
                threadOffsets[threadCtr][tableCtr] = new OffsetIndex();
            }
            OldJoin oldJoin = new OldJoin(query, context, budget,
                    nrThreads, threadCtr, predToEval, threadOffsets, resultSet);
            oldJoins.add(oldJoin);
        }
    }
    @Override
    public void execute(Set<ResultTuple> resultList) throws Exception {
        List<String>[] logs = new List[nrThreads];
        List<ASPTask> tasks = new ArrayList<>();
        int nrSPThreads = nrThreads;
        // Mutex shared by multiple threads.
        AtomicBoolean isFinished = new AtomicBoolean(false);
        // Initialize UCT join order search tree.
        List<Integer> unusedThreads = new ArrayList<>();
        NASPNode firstRoot = new NASPNode(0, query, true, nrThreads, unusedThreads);
        // Space assignment
        AtomicReference<List<Set<Integer>>>[] threadAssignment = new AtomicReference[nrSPThreads];
        // Initialize UCT join order search tree.
        for (int threadCtr = 0; threadCtr < nrSPThreads; threadCtr++) {
            logs[threadCtr] = new ArrayList<>();
            threadAssignment[threadCtr] = new AtomicReference<>(null);
            ASPTask searchTask = new ASPTask(
                    query, context, oldJoins.get(threadCtr),
                    threadCtr, nrSPThreads, threadAssignment, firstRoot, isFinished);
            searchTask.isSmall = unusedThreads.size() > 0;
            if (!unusedThreads.contains(threadCtr)) {
                tasks.add(searchTask);
            }
        }
//        logs[nrSPThreads] = new ArrayList<>();
//        OldJoin joinOp = oldJoins.get(nrSPThreads);
//        SPWorkTask dataTask = new SPWorkTask(query, context, joinOp,
//                0, nrDPThreads, isFinished, nextJoinOrder);
//        tasks.add(dataTask);

        // Initialize a thread pool.
        ExecutorService executorService = ThreadPool.executorService;
        long executionStart = System.currentTimeMillis();
        List<Future<SearchResult>> futures = executorService.invokeAll(tasks);
        long executionEnd = System.currentTimeMillis();
        JoinStats.exeTime = executionEnd - executionStart;
        futures.forEach(futureResult -> {
            try {
                SearchResult result = futureResult.get();
                if (LoggingConfig.PARALLEL_JOIN_VERBOSE) {
                    int id = result.isSearch ? result.id : nrSPThreads + result.id;
                    logs[id] = result.logs;
                }
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        });
        for (ASPTask spTask: tasks) {
            OldJoin join = spTask.joinOp;
            if (join.lastState != null && join.lastState.isFinished()) {
                JoinStats.nrSamples = join.roundCtr;
                resultList.addAll(join.concurrentList);
                break;
            }
        }
        long mergeEnd = System.currentTimeMillis();
        JoinStats.mergeTime = mergeEnd - executionEnd;

//        JoinStats.nrSamples = oldJoins.get(nrSPThreads).roundCtr;
        // Write log to the local file.
        if (LoggingConfig.PARALLEL_JOIN_VERBOSE) {
            LogUtils.writeLogs(logs, "verbose/search/" + QueryStats.queryName);
        }
        System.out.println("Result Set: " + resultList.size());
    }
}
