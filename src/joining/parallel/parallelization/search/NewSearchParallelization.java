package joining.parallel.parallelization.search;

import config.JoinConfig;
import config.LoggingConfig;
import config.ParallelConfig;
import joining.parallel.indexing.OffsetIndex;
import joining.parallel.join.ModJoin;
import joining.parallel.join.OldJoin;
import joining.parallel.join.SPJoin;
import joining.parallel.join.SubJoin;
import joining.parallel.parallelization.Parallelization;
import joining.parallel.parallelization.hybrid.HDataTask;
import joining.parallel.parallelization.hybrid.JoinPlan;
import joining.parallel.progress.ParallelProgressTracker;
import joining.parallel.threads.ThreadPool;
import joining.parallel.uct.NSPNode;
import joining.parallel.uct.SPNode;
import joining.result.ResultTuple;
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

public class NewSearchParallelization extends Parallelization {
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
    public NewSearchParallelization(int nrThreads, int budget,
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
        List<SPTask> tasks = new ArrayList<>();
        int nrDPThreads = 1;
        int nrSPThreads = nrThreads;
        // Mutex shared by multiple threads.
        AtomicBoolean isFinished = new AtomicBoolean(false);
        // Initialize search and data parallelization task.
        AtomicReference<JoinPlan> nextJoinOrder = new AtomicReference<>();
        // Initialize UCT join order search tree.
        for (int threadCtr = 0; threadCtr < nrSPThreads; threadCtr++) {
            logs[threadCtr] = new ArrayList<>();
            SPTask searchTask = new SPTask(
                    query, context, oldJoins.get(threadCtr),
                    threadCtr, nrSPThreads, isFinished, nextJoinOrder);
            if (searchTask.runnable) {
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
        for (SPTask spTask: tasks) {
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
