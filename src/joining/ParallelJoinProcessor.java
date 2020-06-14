package joining;

import catalog.CatalogManager;
import config.*;
import joining.join.DPJoin;
import joining.join.OldJoin;
import joining.result.ResultTuple;
import joining.tasks.DPTask;
import joining.uct.SelectionPolicy;
import joining.uct.UctNode;
import operators.Materialize;
import preprocessing.Context;
import query.ColumnRef;
import query.QueryInfo;
import statistics.JoinStats;
import visualization.TreePlotter;

import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import static threads.ThreadPool.executorService;

/**
 * Controls the join phase in parallel.
 *
 * @author Ziyun Wei
 *
 */
public class ParallelJoinProcessor {
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
        JoinStats.nrTuples = 0;
        JoinStats.nrFastBacktracks = 0;
        JoinStats.nrIndexLookups = 0;
        JoinStats.nrIndexEntries = 0;
        JoinStats.nrUniqueIndexLookups = 0;
        JoinStats.nrIterations = 0;
        JoinStats.nrUctNodes = 0;
        JoinStats.nrPlansTried = 0;
        JoinStats.nrSamples = 0;
        // Initialize logging for new query
        nrLogEntries = 0;
        // Can we skip the join phase?
        if (query.nrJoined == 1 && PreConfig.PRE_FILTER) {
            String alias = query.aliases[0];
            String table = context.aliasToFiltered.get(alias);
            context.joinedTable = table;
            return;
        }
        // the number of threads
        int nrThreads = ParallelConfig.JOIN_THREADS;
        // initialize multi-way join operator for each thread
        DPJoin[] joinOps = new DPJoin[nrThreads];
        // initialize UCT join order search tree for each thread
        UctNode[] roots = new UctNode[nrThreads];
        for (int tid = 0; tid < nrThreads; tid++) {
            joinOps[tid] = new DPJoin(query, context,
                    JoinConfig.BUDGET_PER_EPISODE, tid);
            roots[tid] = new UctNode(0, query,
                    JoinConfig.AVOID_CARTESIANS, joinOps[tid]);
        }
        // initialize thread tasks
        List<DPTask> tasks = new ArrayList<>(nrThreads);
        // finish flag shared by multiple threads.
        AtomicBoolean finish = new AtomicBoolean(false);
        for (int tid = 0; tid < nrThreads; tid++) {
            tasks.add(new DPTask(query, roots[tid], joinOps[tid], finish));
        }
        // submit tasks to the thread pool
        long executionStart = System.currentTimeMillis();
        List<Future<Set<ResultTuple>>> joinThreadResults = executorService.invokeAll(tasks);
        long executionEnd = System.currentTimeMillis();

        // merge results for all threads
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
        int nrTuples = tuples.size();
        log("Materializing join result with " + nrTuples + " tuples ...");
        String targetRelName = NamingConfig.DEFAULT_JOINED_NAME;
        Materialize.execute(tuples, query.aliasToIndex,
                query.colsForPostProcessing,
                context.columnMapping, targetRelName);
        // Update processing context
        context.joinedTable = NamingConfig.DEFAULT_JOINED_NAME;
        context.columnMapping.clear();
        for (ColumnRef postCol : query.colsForPostProcessing) {
            String newColName = postCol.aliasName + "." + postCol.columnName;
            ColumnRef newRef = new ColumnRef(targetRelName, newColName);
            context.columnMapping.put(postCol, newRef);
        }
        // Store number of join result tuples
        JoinStats.skinnerJoinCard = CatalogManager.
                getCardinality(NamingConfig.DEFAULT_JOINED_NAME);
        // Measure execution time for join phase
        JoinStats.joinMillis = System.currentTimeMillis() - startMillis;
    }
    /**
     * Print out log entry if the maximal number of log
     * entries has not been reached yet.
     *
     * @param logEntry	log entry to print
     */
    static void log(String logEntry) {
        if (nrLogEntries < LoggingConfig.MAX_JOIN_LOGS) {
            ++nrLogEntries;
            System.out.println(logEntry);
        }
    }
}
