package joining.parallel.parallelization.task;

import catalog.CatalogManager;
import config.JoinConfig;
import config.LoggingConfig;
import config.NamingConfig;
import config.ParallelConfig;
import joining.join.OldJoin;
import joining.parallel.join.FixJoin;
import joining.parallel.join.SPJoin;
import joining.parallel.parallelization.Parallelization;
import joining.parallel.progress.ParallelProgressTracker;
import joining.parallel.threads.ThreadPool;
import joining.result.ResultTuple;
import joining.uct.SelectionPolicy;
import joining.uct.UctNode;
import logs.LogUtils;
import net.sf.jsqlparser.expression.Expression;
import operators.Materialize;
import predicate.NonEquiNode;
import preprocessing.Context;
import query.ColumnRef;
import query.QueryInfo;
import statistics.JoinStats;
import statistics.QueryStats;
import visualization.TreePlotter;

import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

public class StandardParallelization extends Parallelization {
    /**
     * Multiple join operators for threads
     */
    private List<FixJoin> spJoins = new ArrayList<>();
    /**
     * initialization of parallelization
     *
     * @param nrThreads the number of threads
     * @param budget
     * @param query     select query with join predicates
     * @param context   query execution context
     */
    public StandardParallelization(int nrThreads, int budget, QueryInfo query, Context context) throws Exception {
        super(nrThreads, budget, query, context);
        // Compile predicates
        Map<Expression, NonEquiNode> predToEval = new HashMap<>();
        for (int i = 0; i < query.nonEquiJoinNodes.size(); i++) {
            // Compile predicate and store in lookup table
            Expression pred = query.nonEquiJoinPreds.get(i).finalExpression;
            NonEquiNode node = query.nonEquiJoinNodes.get(i);
            predToEval.put(pred, node);
        }
        // Initialize multi-way join operator
        int nrTables = query.nrJoined;
        ParallelProgressTracker tracker = new ParallelProgressTracker(nrTables, nrThreads, 1);
        for (int i = 0; i < nrThreads; i++) {
            FixJoin modJoin = new FixJoin(query, context, budget, nrThreads, i, predToEval);
            modJoin.tracker = tracker;
            spJoins.add(modJoin);
        }
    }

    @Override
    public void execute(Set<ResultTuple> resultList) throws Exception {
        // Initialize statistics
        long startMillis = System.currentTimeMillis();
        // there is no predicate to evaluate in join phase.
        if (query.equiJoinPreds.size() == 0 && query.nonEquiJoinPreds.size() == 0) {
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
            return;
        }
        // Initialize a thread pool.
        ExecutorService executorService = ThreadPool.executorService;
        // Mutex shared by multiple threads.
        AtomicBoolean end = new AtomicBoolean(false);

        OldJoin joinOp = new OldJoin(query, context,
                JoinConfig.BUDGET_PER_EPISODE);
        // Initialize UCT join order search tree
        UctNode root = new UctNode(0, query, true, joinOp);
        // Initialize counters and variables
        int[] joinOrder = new int[query.nrJoined];
        long roundCtr = 0;
        // Get default action selection policy
        SelectionPolicy policy = JoinConfig.DEFAULT_SELECTION;
        // Initialize counter until memory loss
        long nextForget = 1;
        double maxReward = Double.NEGATIVE_INFINITY;
        // master parameters
        int nrExecutors = ParallelConfig.EXE_THREADS;
        int nextThread = 0;
        int lastCount = 0;
        int nextPeriod = 1;
        double nextNum = 1;
        double base  = Math.pow(ParallelConfig.C, 1.0 / nrExecutors);
        while (!joinOp.isFinished()) {
            ++roundCtr;
            double reward = root.sample(roundCtr, joinOrder, policy);
            // Count reward except for final sample
            if (!joinOp.isFinished()) {
                maxReward = Math.max(reward, maxReward);
            }

            // Consider memory loss
            if (JoinConfig.FORGET && roundCtr==nextForget) {
                root = new UctNode(roundCtr, query, true, joinOp);
                nextForget *= 10;
            }

            // assign the best join order to next thread.
            if (roundCtr == lastCount + nextPeriod && nrExecutors > 1) {
                System.out.println("Assign " + Arrays.toString(joinOrder) +
                        " to Executor " + nextThread + " at round " + roundCtr);

                nextThread = (nextThread + 1) % nrExecutors;
                lastCount = (int) roundCtr;
                nextNum = nextNum * base;
                nextPeriod = (int) Math.round(nextNum);
            }

        }
        // Write log to the local file.
        if (LoggingConfig.PARALLEL_JOIN_VERBOSE) {
            List<String>[] logs = new List[1];
            logs[0] = joinOp.logs;
            LogUtils.writeLogs(logs, "verbose/task/" + QueryStats.queryName);
        }

        // Measure execution time for join phase
        JoinStats.exeTime = System.currentTimeMillis() - startMillis;
        JoinStats.subExeTime.add(JoinStats.exeTime);
        // Materialize result table
        Collection<ResultTuple> tuples = joinOp.result.getTuples();
        int nrTuples = tuples.size();
        String targetRelName = NamingConfig.JOINED_NAME;
        Materialize.execute(tuples, query.aliasToIndex,
                query.colsForPostProcessing,
                context.columnMapping, targetRelName);
        // Update processing context
        context.columnMapping.clear();
        for (ColumnRef postCol : query.colsForPostProcessing) {
            String newColName = postCol.aliasName + "." + postCol.columnName;
            ColumnRef newRef = new ColumnRef(targetRelName, newColName);
            context.columnMapping.put(postCol, newRef);
        }
        // Store number of join result tuples
        int skinnerJoinCard = CatalogManager.
                getCardinality(NamingConfig.JOINED_NAME);
        JoinStats.skinnerJoinCards.add(skinnerJoinCard);

        // Measure execution time for join phase
        JoinStats.joinMillis = System.currentTimeMillis() - startMillis;
        JoinStats.subMateriazed.add(JoinStats.joinMillis - JoinStats.exeTime);
        JoinStats.subJoinTime.add(JoinStats.exeTime);
        System.out.println("Round count: " + roundCtr);
        System.out.println("Join Order: " + Arrays.toString(joinOrder));
        System.out.println("Join Card: " + skinnerJoinCard + "\tJoin time:" + JoinStats.joinMillis);
        JoinStats.lastJoinCard = nrTuples;
    }
}
