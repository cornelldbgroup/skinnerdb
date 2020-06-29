package joining;

import config.*;
import joining.join.DPJoin;
import joining.result.ResultTuple;
import joining.joinThreadTask.JoinPartitionsTask;
import joining.joinThreadTask.SplitTableCoordinator;
import joining.uct.UctNode;
import preprocessing.Context;
import query.QueryInfo;
import statistics.JoinStats;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static threads.ThreadPool.executorService;

/**
 * Controls the join phase in parallel:
 * 1) initialize join statistics
 * 2) initialize UCT nodes and join operators for each thread
 * 3) for each thread, it dynamically splits a table and
 *    run UCT algorithms within the partition.
 * 4) aggregate join results
 *
 * @author Ziyun Wei
 *
 */
public class ParallelJoinProcessor extends JoinProcessor {
    /**
     * The number of join-related log entries
     * generated for the current query.
     */
    static int nrLogEntries = 0;
    /**
     * Executes the join phase and stores result in relation.
     * Also updates mapping from query column references to
     * database columns.
     *
     * @param query		query to process
     * @param context	query execution context
     */
    public static void process(QueryInfo query,
                               Context context) throws Exception {
        // Initialize statistics
        long startMillis = System.currentTimeMillis();
        JoinStats.initializeJoinStats();
        // Initialize logging for new query
        nrLogEntries = 0;
        // Can we skip the join phase?
        if (query.nrJoined == 1 && PreConfig.PRE_FILTER) {
            String alias = query.aliases[0];
            String table = context.aliasToFiltered.get(alias);
            context.joinedTable = table;
            return;
        }
        // The number of threads
        int nrThreads = ParallelConfig.JOIN_THREADS;
        // Initialize multi-way join operator for each thread
        DPJoin[] joinOps = new DPJoin[nrThreads];
        // Initialize UCT join order search tree for each thread
        UctNode[] roots = new UctNode[nrThreads];
        // Initialize split table coordinator
        SplitTableCoordinator coordinator = new SplitTableCoordinator(nrThreads, query.nrJoined);
        for (int tid = 0; tid < nrThreads; tid++) {
            joinOps[tid] = new DPJoin(query, context,
                    JoinConfig.BUDGET_PER_EPISODE, tid);
            roots[tid] = new UctNode(0, query,
                    JoinConfig.AVOID_CARTESIANS, joinOps[tid]);
        }
        // Initialize thread tasks
        List<JoinPartitionsTask> tasks = new ArrayList<>(nrThreads);
        // Finished flag shared by multiple threads
        AtomicBoolean joinFinished = new AtomicBoolean(false);
        for (int tid = 0; tid < nrThreads; tid++) {
            tasks.add(new JoinPartitionsTask(query, roots[tid], joinOps[tid], joinFinished, coordinator));
        }
        // Submit tasks to the thread pool
        long executionStart = System.currentTimeMillis();
        List<Future<Set<ResultTuple>>> joinThreadResults = executorService.invokeAll(tasks);
        long executionEnd = System.currentTimeMillis();

        // Merge results for all threads
        Set<ResultTuple> tuples = new LinkedHashSet<>();
        joinThreadResults.forEach(futureResult -> {
            try {
                Set<ResultTuple> result = futureResult.get();
                tuples.addAll(result);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        });
        // Measure pure join processing time (without materialization)
        JoinStats.pureJoinMillis = executionEnd - executionStart;

//        // TODO: Update statistics
//        JoinStats.nrSamples = roundCtr;
//        JoinStats.avgReward = accReward/roundCtr;
//        JoinStats.maxReward = maxReward;
//        JoinStats.totalWork = 0;
//        for (int tableCtr = 0; tableCtr < query.nrJoined; ++tableCtr) {
//            if (tableCtr == joinOrder[0]) {
//                JoinStats.totalWork += 1;
//            } else {
//                JoinStats.totalWork += Math.max(
//                        joinOp.tracker.tableOffset[tableCtr],0)/
//                        (double)joinOp.cardinalities[tableCtr];
//            }
//        }

        // Materialize result table
        materialize(query, context, tuples);
        // Measure execution time for join phase
        JoinStats.joinMillis = System.currentTimeMillis() - startMillis;
    }
}
