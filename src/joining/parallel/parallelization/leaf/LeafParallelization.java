package joining.parallel.parallelization.leaf;

import config.JoinConfig;
import config.LoggingConfig;
import joining.parallel.join.SPJoin;
import joining.parallel.join.SubJoin;
import joining.parallel.parallelization.Parallelization;
import joining.parallel.progress.ParallelProgressTracker;
import joining.parallel.threads.ThreadPool;
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

import java.util.*;

/**
 * Parallelization specification.
 * Using root parallelization:
 * Different threads synchronize after each episode
 * on a single UCT tree.
 *
 * @author Ziyun Wei
 */
public class LeafParallelization extends Parallelization {
    /**
     * Multiple join operators for threads
     */
    private List<SPJoin> joinOps = new ArrayList<>(nrThreads);

    /**
     * initialization of parallelization
     *
     * @param nrThreads the number of threads
     * @param budget
     * @param query     select query with join predicates
     * @param context   query execution context
     */
    public LeafParallelization(int nrThreads, int budget, QueryInfo query, Context context) throws Exception {
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
            SubJoin spJoin = new SubJoin(query, context, budget, nrThreads, i, predToEval);
            spJoin.tracker = tracker;
            joinOps.add(spJoin);
        }
    }

    @Override
    public void execute(Set<ResultTuple> resultList) throws Exception {
        long executionStart = System.currentTimeMillis();
        long nextForget = 10;
        // Initialize UCT join order search tree
        SyncNode root = new SyncNode(0, query, true, nrThreads);
        // Initialize counters and variables
        int[] joinOrder = new int[query.nrJoined];
        Arrays.fill(joinOrder, -1);
        long roundCtr = 0;
        // Get default action selection policy
        SelectionPolicy policy = SelectionPolicy.UCB1;
        // initialize logs
        List<String>[] logs = new List[nrThreads];
        if (LoggingConfig.PARALLEL_JOIN_VERBOSE) {
            for (int i = 0; i < nrThreads; i++) {
                logs[i] = new ArrayList<>();
            }
        }
        boolean finish = false;

        while (!finish) {
            ++roundCtr;
            double reward = root.sample(roundCtr, joinOrder, policy, joinOps);
            // Count reward except for final sample
            for (int i = 0; i < nrThreads; i++) {
                SPJoin joinOp = joinOps.get(i);
                if (joinOp.isFinished() && joinOp.result != null) {
                    finish = true;
                }
            }
//            if (roundCtr == 1000000) {
//                for (int i = 0; i < nrThreads; i++) {
//                    logs[i] = joinOps.get(i).logs;
//                }
//                LogUtils.writeLogs(logs, "verbose/leaf/" + QueryStats.queryName);
//                System.out.println("Write to logs!");
//            }
            // Consider memory loss
            if (JoinConfig.FORGET && roundCtr == nextForget) {
                root = new SyncNode(0, query, true, nrThreads);
                nextForget *= 10;
            }
//            joinOp.writeLog("Episode Time: " + (end - start) + "\tReward: " + reward);
        }
        long executionEnd = System.currentTimeMillis();
        for (int i = 0; i < nrThreads; i++) {
            SPJoin joinOp = joinOps.get(i);
            resultList.addAll(joinOp.result.tuples);
        }
        if (LoggingConfig.PARALLEL_JOIN_VERBOSE) {
            for (int i = 0; i < nrThreads; i++) {
                logs[i] = joinOps.get(i).logs;
            }
            LogUtils.writeLogs(logs, "verbose/leaf/" + QueryStats.queryName);
        }
        JoinStats.exeTime = executionEnd - executionStart;
        JoinStats.subExeTime.add(JoinStats.exeTime);
        JoinStats.nrSamples = roundCtr;
        System.out.println("Result Set: " + resultList.size() + " " + JoinStats.exeTime + " " + roundCtr);
    }
}
