package joining;

import catalog.CatalogManager;
import config.*;
import joining.parallel.parallelization.dpdsync.DPDSync;
import joining.parallel.parallelization.join.JoinParallelization;
import joining.parallel.parallelization.leaf.LeafParallelization;
import joining.parallel.parallelization.root.RootParallelization;
import joining.parallel.parallelization.search.AdaptiveSearchParallelization;
import joining.parallel.parallelization.search.HeuristicParallelization;
import joining.parallel.parallelization.search.SearchParallelization;
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
        // there is no predicate to evaluate in join phase.
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
            Set<ResultTuple> resultTuples = new HashSet<>();
            // DPD async
            if (ParallelConfig.PARALLEL_SPEC == 0) {
                Parallelization parallelization = new LockFreeParallelization(ParallelConfig.EXE_THREADS,
                        JoinConfig.BUDGET_PER_EPISODE, query, context);
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
                Parallelization parallelization = new LockFreeParallelization(ParallelConfig.EXE_THREADS,
                        JoinConfig.BUDGET_PER_EPISODE, query, context);
                parallelization.execute(resultTuples);
            }
            // DPM
            else if (ParallelConfig.PARALLEL_SPEC == 12) {
                Parallelization parallelization = new LockFreeParallelization(ParallelConfig.EXE_THREADS,
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
            // CAPS
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
