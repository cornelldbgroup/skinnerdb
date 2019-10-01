package joining;

import config.JoinConfig;
import config.LoggingConfig;
import config.NamingConfig;
import config.ParallelConfig;
import joining.parallelization.Parallelization;
import joining.parallelization.lockfree.LockFreeParallelization;
import joining.result.ResultTuple;
import operators.Materialize;
import preprocessing.Context;
import query.ColumnRef;
import query.QueryInfo;
import java.util.*;

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
//        Materialize.executeContext(resultTuples, query.aliasToIndex,
//                query.colsForPostProcessing,
//                context.columnMapping, targetRelName, context);
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
