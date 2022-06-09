package joining;

import catalog.CatalogManager;
import config.*;
import joining.parallel.parallelization.dpdsync.DPDSync;
import joining.parallel.parallelization.hybrid.AdaptiveHybridParallelization;
import joining.parallel.parallelization.hybrid.HybridParallelization;
import joining.parallel.parallelization.join.JoinParallelization;
import joining.parallel.parallelization.leaf.LeafParallelization;
import joining.parallel.parallelization.lockfree.DataParallelization;
import joining.parallel.parallelization.root.RootParallelization;
import joining.parallel.parallelization.search.*;
import joining.parallel.parallelization.task.StandardParallelization;
import joining.parallel.parallelization.task.TaskParallelization;
import joining.parallel.parallelization.tree.TreeParallelization;
import joining.result.ResultTuple;
import operators.Materialize;
import joining.parallel.parallelization.Parallelization;
import joining.parallel.parallelization.lockfree.LockFreeParallelization;
import preprocessing.Context;
import query.ColumnRef;
import query.QueryInfo;
import statistics.JoinStats;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This variant of the join processor parallelize
 * data processing in joining.parallel via multiple
 * threads.
 *
 * @author Anonymous
 *
 */
public class ParallelJoinProcessor {
    /**
     * The number of join-related log entries
     * generated for the current query.
     */
    static int nrLogEntries = 0;
    /**
     * The list of final result tuples.
     */
    public static ResultTuple[] results;
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
        // Initialize join statistics
        JoinStats.nrTuples = 0;
        JoinStats.nrSamples = 0;
        JoinStats.nrIndexLookups = 0;
        JoinStats.nrIndexEntries = 0;
        JoinStats.nrUniqueIndexLookups = 0;
        JoinStats.nrUctNodes = 0;
        JoinStats.nrPlansTried = 0;
        JoinStats.lastJoinCard = 0;
        JoinStats.avgReward = 0;
        JoinStats.maxReward = 0;
        JoinStats.totalWork = 0;
        // There is no predicate to evaluate in join phase.
        if (query.equiJoinPreds.size() == 0 && query.nonEquiJoinPreds.size() == 0 && PreConfig.FILTER) {
            String targetRelName = NamingConfig.JOINED_NAME;
            Materialize.executeFromExistingTable(query.colsForPostProcessing,
                    context.columnMapping, targetRelName);
            // Measure execution time for join phase
            JoinStats.exeTime = 0;
            JoinStats.joinMillis = 0;
            JoinStats.matMillis = 0;
            // Update processing context
            context.columnMapping.clear();
            for (ColumnRef postCol : query.colsForPostProcessing) {
                String newColName = postCol.aliasName + "." + postCol.columnName;
                ColumnRef newRef = new ColumnRef(targetRelName, newColName);
                context.columnMapping.put(postCol, newRef);
            }
            // Store number of join result tuples
            int skinnerJoinCard = CatalogManager.getCardinality(targetRelName);
            JoinStats.lastJoinCard = skinnerJoinCard;
            System.out.println("Join card: " + skinnerJoinCard);
            JoinStats.lastJoinCard = skinnerJoinCard;
        }
        else {
            long startMillis = System.currentTimeMillis();
            Set<ResultTuple> resultTuples = new HashSet<>(1000000);
            // DPD async
            if (ParallelConfig.PARALLEL_SPEC == 0) {
//                Parallelization parallelization = new LockFreeParallelization(ParallelConfig.EXE_THREADS,
//                        JoinConfig.BUDGET_PER_EPISODE, query, context);
                DataParallelization parallelization = new DataParallelization(ParallelConfig.EXE_THREADS,
                        JoinConfig.BUDGET_PER_EPISODE, query, context);
                System.out.println("Alias: " + Arrays.toString(query.aliases));
                System.out.println("Cards: " + Arrays.toString(parallelization.oldJoin.cardinalities));
                parallelization.execute(resultTuples);
            }
            // DPD sync
            else if (ParallelConfig.PARALLEL_SPEC == 1) {
                Parallelization parallelization = new DPDSync(ParallelConfig.EXE_THREADS,
                        JoinConfig.BUDGET_PER_EPISODE, query, context);
                parallelization.execute(resultTuples);
            }
            // PPS
            else if (ParallelConfig.PARALLEL_SPEC == 2) {
                Parallelization parallelization = new SearchParallelization(ParallelConfig.EXE_THREADS,
                        JoinConfig.BUDGET_PER_EPISODE, query, context);
                parallelization.execute(resultTuples);
            }
            // PPA
            else if (ParallelConfig.PARALLEL_SPEC == 3) {
                Parallelization parallelization = new SearchParallelization(ParallelConfig.EXE_THREADS,
                        JoinConfig.BUDGET_PER_EPISODE, query, context);
                parallelization.execute(resultTuples);
            }
            // Root parallelization
            else if (ParallelConfig.PARALLEL_SPEC == 4) {
                Parallelization parallelization = new RootParallelization(ParallelConfig.EXE_THREADS,
                        JoinConfig.BUDGET_PER_EPISODE, query, context);
                parallelization.execute(resultTuples);
            }
            // Leaf parallelization
            else if (ParallelConfig.PARALLEL_SPEC == 5) {
                Parallelization parallelization = new LeafParallelization(ParallelConfig.EXE_THREADS,
                        JoinConfig.BUDGET_PER_EPISODE, query, context);
                parallelization.execute(resultTuples);
            }
            // Tree parallelization
            else if (ParallelConfig.PARALLEL_SPEC == 6) {
                Parallelization parallelization = new TreeParallelization(ParallelConfig.EXE_THREADS,
                        JoinConfig.BUDGET_PER_EPISODE, query, context);
                parallelization.execute(resultTuples);
            }
            // Search parallelization fixing one join order
            else if (ParallelConfig.PARALLEL_SPEC == 7) {
                Parallelization parallelization = new SearchParallelization(ParallelConfig.EXE_THREADS,
                        JoinConfig.BUDGET_PER_EPISODE, query, context);
                parallelization.execute(resultTuples);
            }
            // Search parallelization fixing one join order
            else if (ParallelConfig.PARALLEL_SPEC == 8) {
                Parallelization parallelization = new AdaptiveSearchParallelization(ParallelConfig.EXE_THREADS,
                        JoinConfig.BUDGET_PER_EPISODE, query, context);
                parallelization.execute(resultTuples);
            }
            // one search thread and multiple executor threads
            else if (ParallelConfig.PARALLEL_SPEC == 9) {
                Parallelization parallelization = new TaskParallelization(ParallelConfig.EXE_THREADS,
                        JoinConfig.BUDGET_PER_EPISODE, query, context);
                parallelization.execute(resultTuples);
            }
            // Search parallelization fixing one join order
            else if (ParallelConfig.PARALLEL_SPEC == 10) {
                Parallelization parallelization = new HeuristicParallelization(ParallelConfig.EXE_THREADS,
                        JoinConfig.BUDGET_PER_EPISODE, query, context);
                parallelization.execute(resultTuples);
            }
            // DPL
            else if (ParallelConfig.PARALLEL_SPEC == 11) {
                Parallelization parallelization = new DataParallelization(ParallelConfig.EXE_THREADS,
                        JoinConfig.BUDGET_PER_EPISODE, query, context);
                parallelization.execute(resultTuples);
            }
            // DPM
            else if (ParallelConfig.PARALLEL_SPEC == 12) {
                Parallelization parallelization = new DataParallelization(ParallelConfig.EXE_THREADS,
                        JoinConfig.BUDGET_PER_EPISODE, query, context);
                parallelization.execute(resultTuples);
            }
            // DPM
            else if (ParallelConfig.PARALLEL_SPEC == 13) {
                Parallelization parallelization = new LockFreeParallelization(ParallelConfig.EXE_THREADS,
                        JoinConfig.BUDGET_PER_EPISODE, query, context);
                parallelization.execute(resultTuples);
            }
            // CAPS
            else if (ParallelConfig.PARALLEL_SPEC == 14) {
                Parallelization parallelization = new AdaptiveSearchParallelization(ParallelConfig.EXE_THREADS,
                        JoinConfig.BUDGET_PER_EPISODE, query, context);
                parallelization.execute(resultTuples);
            }
            // PJ
            else if (ParallelConfig.PARALLEL_SPEC == 15) {
                Parallelization parallelization = new JoinParallelization(ParallelConfig.EXE_THREADS,
                        JoinConfig.BUDGET_PER_EPISODE, query, context);
                parallelization.execute(resultTuples);
            }
            // DBTP
            else if (ParallelConfig.PARALLEL_SPEC == 16) {
                Parallelization parallelization = new StandardParallelization(ParallelConfig.EXE_THREADS,
                        JoinConfig.BUDGET_PER_EPISODE, query, context);
                parallelization.execute(resultTuples);
            }
            // New SP
            else if (ParallelConfig.PARALLEL_SPEC == 17) {
                Parallelization parallelization = new NewSearchParallelization(ParallelConfig.EXE_THREADS,
                        JoinConfig.BUDGET_PER_EPISODE, query, context, resultTuples);
                parallelization.execute(resultTuples);
            }
            // Hybrid Parallelization
            else if (ParallelConfig.PARALLEL_SPEC == 18) {
                Parallelization parallelization = new HybridParallelization(ParallelConfig.EXE_THREADS,
                        JoinConfig.BUDGET_PER_EPISODE, query, context);
                parallelization.execute(resultTuples);
            }
            // New SP adaptive
            else if (ParallelConfig.PARALLEL_SPEC == 19) {
                Parallelization parallelization = new NewAdaptiveSearchParallelization(ParallelConfig.EXE_THREADS,
                        JoinConfig.BUDGET_PER_EPISODE, query, context, resultTuples);
                parallelization.execute(resultTuples);
            }
            // New SP adaptive
            else if (ParallelConfig.PARALLEL_SPEC == 20) {
                Parallelization parallelization = new AdaptiveHybridParallelization(ParallelConfig.EXE_THREADS,
                        JoinConfig.BUDGET_PER_EPISODE, query, context);
                parallelization.execute(resultTuples);
            }

            System.out.println("Finish Parallel Join!");

            int skinnerJoinCard;
            String targetRelName = NamingConfig.JOINED_NAME;
            if (context.uniqueJoinResult != null) {
                String resultRel = query.plainSelect.getIntoTables().get(0).getName();
                Materialize.materializeRow(context.uniqueJoinResult, query, context, resultRel);
                skinnerJoinCard = 1;
            }
            else if (context.resultTuplesList == null) {
                Materialize.execute(resultTuples, query.aliasToIndex,
                        query.colsForPostProcessing,
                        context.columnMapping, targetRelName);
                skinnerJoinCard = CatalogManager.getCardinality(targetRelName);
            }
            else {
                Materialize.execute(resultTuples, query.aliasToIndex,
                        query.colsForPostProcessing,
                        context, targetRelName);
                skinnerJoinCard = CatalogManager.getCardinality(targetRelName);
            }
            // Materialize result table
//            String resultRel = query.plainSelect.getIntoTables().get(0).getName();
//            log("Materializing join result with " + nrTuples + " tuples ...");

//            // Update processing context
            context.columnMapping.clear();
            for (ColumnRef postCol : query.colsForPostProcessing) {
                String newColName = postCol.aliasName + "." + postCol.columnName;
                ColumnRef newRef = new ColumnRef(targetRelName, newColName);
                context.columnMapping.put(postCol, newRef);
            }
            long materializeEnd = System.currentTimeMillis();
            JoinStats.matMillis = materializeEnd - startMillis - JoinStats.exeTime;
            JoinStats.lastJoinCard = skinnerJoinCard;
            JoinStats.joinMillis = JoinStats.exeTime;
            // Store number of join result tuples
            System.out.println("Join card: " + skinnerJoinCard + "\tJoin time:" + JoinStats.exeTime);
            JoinStats.lastJoinCard = skinnerJoinCard;
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
