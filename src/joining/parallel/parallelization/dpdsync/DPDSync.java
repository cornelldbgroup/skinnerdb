package joining.parallel.parallelization.dpdsync;

import config.JoinConfig;
import config.LoggingConfig;
import expressions.compilation.KnaryBoolEval;
import joining.parallel.join.DPJoin;
import joining.parallel.join.ModJoin;
import joining.parallel.parallelization.Parallelization;
import joining.parallel.plan.LeftDeepPartitionPlan;
import joining.parallel.progress.ParallelProgressTracker;
import joining.parallel.uct.SyncNode;
import joining.result.ResultTuple;
import joining.result.UniqueJoinResult;
import joining.uct.SelectionPolicy;
import logs.LogUtils;
import net.sf.jsqlparser.expression.Expression;
import predicate.NonEquiNode;
import preprocessing.Context;
import query.QueryInfo;
import statistics.JoinStats;
import statistics.QueryStats;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class DPDSync extends Parallelization {
    /**
     * Multiple join operators for threads
     */
    private List<DPJoin> dpJoins = new ArrayList<>();
    /**
     * initialization of parallelization
     *
     * @param nrThreads the number of threads
     * @param budget
     * @param query     select query with join predicates
     * @param context   query execution context
     */
    public DPDSync(int nrThreads, int budget, QueryInfo query, Context context) throws Exception {
        super(nrThreads, budget, query, context);
        // Compile predicates
        Map<Expression, NonEquiNode> predToEval = new HashMap<>();
        Map<Expression, KnaryBoolEval> predToComp = new HashMap<>();
        for (int i = 0; i < query.nonEquiJoinNodes.size(); i++) {
            // Compile predicate and store in lookup table
            Expression pred = query.nonEquiJoinPreds.get(i).finalExpression;
            NonEquiNode node = query.nonEquiJoinNodes.get(i);
            predToEval.put(pred, node);
        }
        // Initialize multi-way join operator
        int nrTables = query.nrJoined;
        int nrSplits = query.equiJoinPreds.size() + nrTables;
        Map<Integer, LeftDeepPartitionPlan> planCache = new ConcurrentHashMap<>();
        ParallelProgressTracker tracker = new ParallelProgressTracker(nrTables, 1, 1);
        for (int i = 0; i < nrThreads; i++) {
            ModJoin modJoin = new ModJoin(query, context, budget, nrThreads, i, predToEval, predToComp, planCache);
            modJoin.tracker = tracker;
            dpJoins.add(modJoin);
        }
    }

    @Override
    public void execute(Set<ResultTuple> resultList) throws Exception {
        long executionStart = System.currentTimeMillis();
        // Initialize UCT join order search tree.
        SyncNode root = new SyncNode(0, query, JoinConfig.AVOID_CARTESIAN, nrThreads);
        // Initialize counters and variables
        int[] joinOrder = new int[query.nrJoined];
        // Initialize counter until memory loss
        long nextForget = 10;
        // Initialize variables for broadcasting.
        int nrTables = query.nrJoined;
        long roundCtr = 0;
        SelectionPolicy policy = SelectionPolicy.UCB1;
        // initialize logs
        List<String>[] logs = new List[nrThreads];
        if (LoggingConfig.PARALLEL_JOIN_VERBOSE) {
            for (int i = 0; i < nrThreads; i++) {
                logs[i] = new ArrayList<>();
            }
        }
        boolean isFinish = false;
        while (!isFinish) {
            isFinish = true;
            ++roundCtr;
            double reward = root.sampleDP(roundCtr, joinOrder, policy, dpJoins);
            for (DPJoin dpJoin: dpJoins) {
                if (!dpJoin.isFinished()) {
                    isFinish = false;
                }
            }
            // Consider memory loss
            if (JoinConfig.FORGET && roundCtr==nextForget) {
                root = new SyncNode(0, query, JoinConfig.AVOID_CARTESIAN, nrThreads);
                nextForget *= 10;
            }
        }
        System.out.println("Round: " + roundCtr + "\tJoin order: " + Arrays.toString(joinOrder));
        long executionEnd = System.currentTimeMillis();
        for (int threadCtr = 0; threadCtr < nrThreads; threadCtr++) {
            DPJoin joinOp = dpJoins.get(threadCtr);
            resultList.addAll(joinOp.result.tuples);
            UniqueJoinResult uniqueJoinResult = joinOp.uniqueJoinResult;
            if (uniqueJoinResult != null) {
                if (context.uniqueJoinResult == null) {
                    context.uniqueJoinResult = uniqueJoinResult;
                }
                else {
                    context.uniqueJoinResult.merge(uniqueJoinResult);
                }
            }
        }
        if (LoggingConfig.PARALLEL_JOIN_VERBOSE) {
            for (int i = 0; i < nrThreads; i++) {
                logs[i] = dpJoins.get(i).logs;
            }
            LogUtils.writeLogs(logs, "verbose/dpdsync/" + QueryStats.queryName);
        }
        JoinStats.exeTime = executionEnd - executionStart;
        JoinStats.nrSamples = roundCtr;
    }
}
