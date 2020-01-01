package joining.parallel.parallelization.dpdsync;

import config.JoinConfig;
import config.LoggingConfig;
import expressions.compilation.KnaryBoolEval;
import joining.parallel.join.DPJoin;
import joining.parallel.join.ModJoin;
import joining.parallel.join.SPJoin;
import joining.parallel.parallelization.Parallelization;
import joining.parallel.progress.ParallelProgressTracker;
import joining.parallel.threads.ThreadPool;
import joining.parallel.uct.DPNode;
import joining.parallel.uct.SyncNode;
import joining.result.ResultTuple;
import joining.uct.SelectionPolicy;
import joining.uct.UctNode;
import logs.LogUtils;
import net.sf.jsqlparser.expression.Expression;
import predicate.NonEquiNode;
import preprocessing.Context;
import query.QueryInfo;
import statistics.JoinStats;
import statistics.QueryStats;
import visualization.TreePlotter;

import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;

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
        int nrSplits = query.equiJoinPreds.size();
        ParallelProgressTracker tracker = new ParallelProgressTracker(nrTables, nrThreads, nrSplits);
        for (int i = 0; i < nrThreads; i++) {
            ModJoin modJoin = new ModJoin(query, context, budget, nrThreads, i, predToEval, predToComp);
            modJoin.tracker = tracker;
            dpJoins.add(modJoin);
        }
    }

    @Override
    public void execute(Set<ResultTuple> resultList) throws Exception {
        long executionStart = System.currentTimeMillis();
        // Initialize UCT join order search tree.
        SyncNode root = new SyncNode(0, query, true, nrThreads);
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
                root = new SyncNode(0, query, true, nrThreads);
                nextForget *= 10;
            }
        }
        long executionEnd = System.currentTimeMillis();
        for (int i = 0; i < nrThreads; i++) {
            DPJoin joinOp = dpJoins.get(i);
            resultList.addAll(joinOp.result.tuples);
        }
        if (LoggingConfig.PARALLEL_JOIN_VERBOSE) {
            for (int i = 0; i < nrThreads; i++) {
                logs[i] = dpJoins.get(i).logs;
            }
            LogUtils.writeLogs(logs, "verbose/dpdsync/" + QueryStats.queryName);
        }
        JoinStats.exeTime = executionEnd - executionStart;
        JoinStats.subExeTime.add(JoinStats.exeTime);
        JoinStats.nrSamples = roundCtr;
        System.out.println("Result Set: " + resultList.size() + " " + JoinStats.exeTime + " " + roundCtr);
    }
}
