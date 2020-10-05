package joining.parallel.parallelization.join;

import config.JoinConfig;
import config.LoggingConfig;
import joining.parallel.join.ParaJoin;
import joining.parallel.join.SPJoin;
import joining.parallel.join.SubJoin;
import joining.parallel.parallelization.Parallelization;
import joining.parallel.progress.ParallelProgressTracker;
import joining.parallel.progress.TaskTracker;
import joining.parallel.uct.SyncNode;
import joining.result.ResultTuple;
import joining.uct.SelectionPolicy;
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
 * @author Anonymous
 */
public class JoinParallelization extends Parallelization {
    /**
     * Multiple join operators for threads
     */
    private final ParaJoin joinOp;

    /**
     * initialization of parallelization
     *
     * @param nrThreads the number of threads
     * @param budget
     * @param query     select query with join predicates
     * @param context   query execution context
     */
    public JoinParallelization(int nrThreads, int budget, QueryInfo query, Context context) throws Exception {
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
        ParaJoin paraJoin = new ParaJoin(query, context, budget, nrThreads, predToEval);
        paraJoin.tracker = new TaskTracker(nrTables, paraJoin.cardinalities);
        joinOp = paraJoin;
    }

    @Override
    public void execute(Set<ResultTuple> resultList) throws Exception {
        // Create all tasks before hand
        ParaJoin paraJoin = new ParaJoin(query, joinOp.preSummary, joinOp.budget, nrThreads, joinOp.predToEval);
        paraJoin.simulateTasks();
        long taskStart = System.currentTimeMillis();
        boolean isFinished = false;
        int round = 0;
        while (!isFinished) {
            // Count reward except for final sample
            paraJoin.runSimulatedTasks();
            // Finished flag
            isFinished = paraJoin.isFinished();
            round++;
        }
        long taskEnd = System.currentTimeMillis();
        System.out.println("Task: " + (taskEnd - taskStart) + " " + round);
        paraJoin.threadPool.shutdown();

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
        List<String>[] logs = new List[1];
        if (LoggingConfig.PARALLEL_JOIN_VERBOSE) {
            logs[0] = new ArrayList<>();
        }
        boolean finish = false;

        while (!finish) {
            ++roundCtr;
            // Count reward except for final sample
            double reward = root.sample(roundCtr, joinOrder, policy, joinOp);
            // Finished flag
            finish = joinOp.isFinished();
            // Consider memory loss
            if (JoinConfig.FORGET && roundCtr == nextForget) {
                root = new SyncNode(0, query, true, nrThreads);
                nextForget *= 10;
            }
            if (System.currentTimeMillis() - executionStart > 10000) {
                System.out.println("Too large");
                logs[0] = joinOp.logs;
                break;
            }
        }
        // Write log to the local file.
        if (LoggingConfig.PARALLEL_JOIN_VERBOSE) {
            logs[0] = joinOp.logs;
            LogUtils.writeLogs(logs, "verbose/join/" + QueryStats.queryName);
        }
        long executionEnd = System.currentTimeMillis();
        joinOp.threadPool.shutdown();
        JoinStats.exeTime = executionEnd - executionStart;
        JoinStats.nrSamples = roundCtr;
        resultList.addAll(joinOp.tuples);
        for (int tid = 0; tid < nrThreads; tid++) {
            resultList.addAll(joinOp.threads.get(tid).tuples);
        }
        System.out.println("Final Join Order: " + Arrays.toString(joinOrder));
        System.out.println("Result Set: " + resultList.size() + " " + JoinStats.exeTime + " " + roundCtr);
    }
}
