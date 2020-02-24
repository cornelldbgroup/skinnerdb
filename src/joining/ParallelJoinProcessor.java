package joining;

import catalog.CatalogManager;
import config.*;
import joining.parallel.parallelization.dpdsync.DPDSync;
import joining.parallel.parallelization.leaf.LeafParallelization;
import joining.parallel.parallelization.root.RootParallelization;
import joining.parallel.parallelization.search.AdaptiveSearchParallelization;
import joining.parallel.parallelization.search.SearchParallelization;
import joining.parallel.parallelization.task.TaskParallelization;
import joining.parallel.parallelization.tree.TreeParallelization;
import joining.result.ResultTuple;
import operators.Materialize;
import joining.parallel.parallelization.Parallelization;
import joining.parallel.parallelization.lockfree.LockFreeParallelization;
import postprocessing.ParallelPostProcessor;
import postprocessing.PostProcessor;
import preprocessing.Context;
import preprocessing.Preprocessor;
import query.ColumnRef;
import query.QueryInfo;
import statistics.JoinStats;
import statistics.PostStats;
import statistics.PreStats;

import java.util.*;

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
        long startMillis = System.currentTimeMillis();
        // there is no predicate to evaluate in join phase.
        if (query.equiJoinPreds.size() == 0 && query.nonEquiJoinPreds.size() == 0 && PreConfig.FILTER) {
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
            // Store number of join result tuples
            int skinnerJoinCard = CatalogManager.getCardinality(targetRelName);
            JoinStats.skinnerJoinCards.add(skinnerJoinCard);
            System.out.println("Join card: " + skinnerJoinCard + "\tJoin time:" + Arrays.toString(JoinStats.subExeTime.toArray()));
            JoinStats.lastJoinCard = skinnerJoinCard;
        }
        else {
            Set<ResultTuple> resultTuples = new HashSet<>();
            List<Long> subExes = new ArrayList<>();
            List<Long> subSamples = new ArrayList<>();
            List<Long> subTuples = new ArrayList<>();
//            int nrCases = GeneralConfig.ISTESTCASE ? GeneralConfig.TEST_CASE : 1;
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
            subExes.add(JoinStats.subExeTime.remove(JoinStats.subExeTime.size() - 1));
            subSamples.add(JoinStats.nrSamples);
            subTuples.add(JoinStats.nrTuples);

            System.out.println("Finish Parallel Join!");
            int median = 0;
            long[] subExe = subExes.stream().mapToLong(exe->exe).toArray();
            Arrays.sort(subExe);
            JoinStats.subExeTime.add(subExe[median]);
            JoinStats.subAllExeTime.add(Arrays.toString(subExe));
            JoinStats.subAllSamples.add(Arrays.toString(subSamples.toArray()));
            JoinStats.subAllTuples.add(Arrays.toString(subTuples.toArray()));


            long materializeStart = System.currentTimeMillis();
            // Materialize result table
            int nrTuples = resultTuples.size();
//            String resultRel = query.plainSelect.getIntoTables().get(0).getName();
//            log("Materializing join result with " + nrTuples + " tuples ...");
            String targetRelName = NamingConfig.JOINED_NAME;
            Materialize.execute(resultTuples, query.aliasToIndex,
                    query.colsForPostProcessing,
                    context.columnMapping, targetRelName);
//            // Update processing context
            context.columnMapping.clear();
            for (ColumnRef postCol : query.colsForPostProcessing) {
                String newColName = postCol.aliasName + "." + postCol.columnName;
                ColumnRef newRef = new ColumnRef(targetRelName, newColName);
                context.columnMapping.put(postCol, newRef);
            }
            long materializeEnd = System.currentTimeMillis();
            JoinStats.subMateriazed.add(materializeEnd - materializeStart);
            // Store number of join result tuples
            int skinnerJoinCard = resultTuples.size();
            JoinStats.skinnerJoinCards.add(skinnerJoinCard);
            System.out.println("Join card: " + skinnerJoinCard + "\tJoin time:" + Arrays.toString(JoinStats.subExeTime.toArray()));
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
