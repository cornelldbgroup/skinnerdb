package joining;

import catalog.CatalogManager;
import config.*;
import joining.join.DataParallelJoin;
import joining.result.ResultTuple;
import joining.joinThreadTask.JoinPartitionsTask;
import joining.joinThreadTask.DPJoinCoordinator;
import joining.uct.UctNode;
import preprocessing.Context;
import query.QueryInfo;
import statistics.JoinStats;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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
        DataParallelJoin[] joinOps = new DataParallelJoin[nrThreads];
        // Initialize UCT join order search tree for each thread
        UctNode[] roots = new UctNode[nrThreads];
        // Initialize split table coordinator
        DPJoinCoordinator coordinator = new DPJoinCoordinator(nrThreads, query.nrJoined);
        for (int tid = 0; tid < nrThreads; tid++) {
            joinOps[tid] = new DataParallelJoin(query, context,
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
        System.out.println("Join Time: " + JoinStats.pureJoinMillis);
        if (LoggingConfig.WRITE_DPJOIN_LOGS) {
            // logs list
            List<String>[] logs = new List[nrThreads];
            for (int tid = 0; tid < nrThreads; tid++) {
                logs[tid] = joinOps[tid].logs;
            }
            writeLogs(logs, "test");
        }
//        // TODO: Update statistics
        JoinStats.nrSamples = 0;
        for (int tid = 0; tid < nrThreads; tid++) {
            JoinStats.nrSamples = Math.max(JoinStats.nrSamples, joinOps[tid].roundCtr);
        }
        System.out.println("Round Ctr: " + JoinStats.nrSamples);
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
        // Store number of join result tuples
        JoinStats.skinnerJoinCard = CatalogManager.
                getCardinality(NamingConfig.DEFAULT_JOINED_NAME);
    }
    /**
     * Write logs into local files.
     *
     * @param logs      A list of logs for multiple threads.
     * @param path      log directory path.
     */
    public static void writeLogs(List<String>[] logs, String path) {
        List<Thread> threads = new ArrayList<>();
        int nrThreads = logs.length;
        String PATH = "./logs/" + path;
        File directory = new File(PATH);
        if (!directory.exists()){
            directory.mkdirs();
        }
        for(int i = 0; i < nrThreads; i++) {
            List<String> threadLogs = logs[i];
            int tid = i;
            Thread T1 = new Thread(() -> {
                try {
                    FileWriter writer = new FileWriter("./logs/" + path + "/" + tid +".txt");
                    threadLogs.forEach(log -> {
                        try {
                            writer.write(log + "\n");
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            T1.start();
            threads.add(T1);
        }
        for (int i = 0; i < nrThreads; i++) {
            try {
                threads.get(i).join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
