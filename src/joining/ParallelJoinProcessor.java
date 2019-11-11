package joining;

import catalog.CatalogManager;
import config.JoinConfig;
import config.LoggingConfig;
import config.NamingConfig;
import config.ParallelConfig;
import joining.result.ResultTuple;
import operators.Materialize;
import joining.parallel.parallelization.Parallelization;
import joining.parallel.parallelization.lockfree.LockFreeParallelization;
import preprocessing.Context;
import query.ColumnRef;
import query.QueryInfo;
import statistics.JoinStats;

import java.util.HashSet;
import java.util.Set;

/**
 * This variant of the join processor parallelize
 * data processing in joining.parallel via multiple
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
        long startMillis = System.currentTimeMillis();
        if (query.equiJoinPreds.size() == 0) {
            String targetRelName = NamingConfig.JOINED_NAME;
            Materialize.executeFromExistingTable(query.colsForPostProcessing,
                    context.columnMapping, targetRelName);
            // Measure execution time for join phase
            JoinStats.exeTime = 0;
            JoinStats.subExeTime.add(JoinStats.exeTime);
            // Update processing context
            context.columnMapping.clear();
            for (ColumnRef postCol : query.colsForPostProcessing) {
                String newColName = postCol.aliasName + "." + postCol.columnName;
                ColumnRef newRef = new ColumnRef(targetRelName, newColName);
                context.columnMapping.put(postCol, newRef);
            }
        }
        else {
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
        // Store number of join result tuples
        JoinStats.skinnerJoinCard = CatalogManager.
                getCardinality(NamingConfig.JOINED_NAME);
        // Measure execution time for join phase
        JoinStats.joinMillis = System.currentTimeMillis() - startMillis;
        System.out.println("Join card: " + JoinStats.skinnerJoinCard + "\tJoin time:" + JoinStats.joinMillis);

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
