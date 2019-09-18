package joining;

import config.JoinConfig;
import config.LoggingConfig;
import config.NamingConfig;
import config.ParallelConfig;
import joining.join.DPJoin;
import joining.join.ModJoin;
import joining.join.OldJoin;
import joining.parallelization.Parallelization;
import joining.parallelization.lockfree.LockFreeParallelization;
import joining.result.ResultTuple;
import joining.uct.DPNode;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This variant of the join processor parallelize
 * data processing in parallel via multiple
 * threads.
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
        Set<ResultTuple> resultTuples = new HashSet<>();
        Parallelization parallelization = new LockFreeParallelization(ParallelConfig.EXE_THREADS,
                JoinConfig.BUDGET_PER_EPISODE, query, context);

        parallelization.execute(resultTuples);

        // Materialize result table
        int nrTuples = resultTuples.size();
        log("Materializing join result with " + nrTuples + " tuples ...");
        String targetRelName = NamingConfig.JOINED_NAME;
        Materialize.execute(resultTuples, query.aliasToIndex,
                query.colsForPostProcessing,
                context.columnMapping, targetRelName);
        // Update processing context
        context.columnMapping.clear();
        for (ColumnRef postCol : query.colsForPostProcessing) {
            String newColName = postCol.aliasName + "." + postCol.columnName;
            ColumnRef newRef = new ColumnRef(targetRelName, newColName);
            context.columnMapping.put(postCol, newRef);
        }

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
