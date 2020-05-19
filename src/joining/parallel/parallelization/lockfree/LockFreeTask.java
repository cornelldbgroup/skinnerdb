package joining.parallel.parallelization.lockfree;

import config.JoinConfig;
import config.ParallelConfig;
import joining.parallel.uct.SimpleUctNode;
import joining.progress.State;
import joining.result.ResultTuple;
import joining.uct.SelectionPolicy;
import joining.parallel.join.DPJoin;
import joining.parallel.parallelization.EndPlan;
import joining.parallel.uct.DPNode;
import joining.uct.UctNode;
import logs.LogUtils;
import preprocessing.Context;
import query.QueryInfo;
import statistics.JoinStats;
import statistics.QueryStats;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public class LockFreeTask implements Callable<LockFreeResult>{
    /**
     * Query to process.
     */
    private final QueryInfo query;
    /**
     * context after pre-processing.
     */
    private final Context context;
    /**
     * The root of parallel UCT tree.
     */
    private volatile DPNode root;
    /**
     * The finished plan.
     */
    private final EndPlan endPlan;
    /**
     * Finish flag
     */
    private final AtomicBoolean finish;
    /**
     * Finish flag
     */
    private final AtomicBoolean terminated;
    /**
     * Mutex lock.
     */
    private final ReentrantLock lock;
    /**
     * Join executor.
     */
    private final DPJoin joinOp;


    public LockFreeTask(QueryInfo query, Context context, DPNode root, EndPlan endPlan,
                        AtomicBoolean finish, AtomicBoolean terminated, ReentrantLock lock, DPJoin dpJoin) {
        this.query = query;
        this.context = context;
        this.root = root;
        this.endPlan = endPlan;
        this.finish = finish;
        this.terminated = terminated;
        this.lock = lock;
        this.joinOp = dpJoin;
    }

    @Override
    public LockFreeResult call() throws Exception {
        long timer1 = System.currentTimeMillis();
        // Initialize counters and variables
        int tid = joinOp.tid;
        int nrJoined = query.nrJoined;
        int[] joinOrder = new int[nrJoined];
        long roundCtr = 0;
        // Get default action selection policy
        SelectionPolicy policy = SelectionPolicy.UCB1;
        // Initialize counter until memory loss
        long nextForget = 1;
        // Initialize plot counter
        int plotCtr = 0;
        // Iterate until join result was generated
        double accReward = 0;
        Set<Integer> finishedTables = new HashSet<>();
        while (!terminated.get()) {
            DPNode root = endPlan.root;
            ++roundCtr;
            double reward = 0;
            int finalTable = endPlan.getSplitTable();
            if (finalTable != -1) {
                joinOrder = endPlan.getJoinOrder();
                joinOp.isShared = true;

                if (ParallelConfig.HEURISTIC_SHARING && ParallelConfig.PARALLEL_SPEC == 0) {
                    int lastTable;
                    if (finishedTables.contains(finalTable)) {
                        int table = root.getSplitTableByCard(joinOrder, joinOp.cardinalities, finishedTables);
                        if (table == -1) {
                            break;
                        }
                        State slowState = endPlan.slowestState.get();
                        reward = joinOp.execute(joinOrder, table, (int) roundCtr, endPlan.finishFlags, slowState);
                        endPlan.threadSlowStates[tid][table] = joinOp.lastState;
                    }
                    else {
                        State slowState = endPlan.slowestState.get();
                        reward = joinOp.execute(joinOrder, finalTable, (int) roundCtr, endPlan.finishFlags, slowState);
                        int largeTable = joinOp.largeTable;
                        lastTable = joinOp.lastTable;

                        boolean isSlow = endPlan.isSlow(joinOp.lastState, tid, lastTable);
                        if (isSlow && largeTable != lastTable) {
                            State curSlow = endPlan.setSplitTable(largeTable, joinOp.lastState);
//                            joinOp.writeLog("Set split Table to: " + largeTable + "\tSlow: " + curSlow.toString());
                        }
//                        else {
//                            State slow = endPlan.getSlowState(lastTable);
//                            joinOp.writeLog("Slow State: " + slow);
//                        }
                    }
                }
                else if (ParallelConfig.HEURISTIC_STOP || ParallelConfig.PARALLEL_SPEC == 11
                        || ParallelConfig.PARALLEL_SPEC == 12) {
                    reward = joinOp.execute(joinOrder, finalTable, (int) roundCtr);
                    if (joinOp.isFinished()) {
                        break;
                    }
                }
                else {
                    reward = root.sampleFinal((int) roundCtr, joinOrder, joinOp, finishedTables, endPlan.finishFlags);
                    if (reward < 0) {
                        break;
                    }
                }
            }
            else {
                reward = root.sample(roundCtr, joinOrder, joinOp, policy);
            }
            // Count reward except for final sample
            if (!joinOp.isFinished()) {
                accReward += reward;
            }
            // broadcasting the finished plan.
            else {
                int splitTable = joinOp.lastTable;
                if (finish.compareAndSet(false, true)) {
//                    joinOrder = new int[]{8, 2, 5, 7, 3, 9, 1, 6, 4, 0};
//                    splitTable = 2;
                    System.out.println(tid + " shared: " + Arrays.toString(joinOrder) + " splitting " + splitTable);
                    endPlan.setJoinOrder(joinOrder);
                    endPlan.setSplitTable(splitTable);
                }
                endPlan.threadSlowStates[tid][splitTable] = joinOp.lastState;

                boolean isFinished = endPlan.setFinished(tid, joinOp.lastTable);
                finishedTables.add(splitTable);
                if (isFinished) {
                    terminated.set(true);
                    break;
                }
                if (ParallelConfig.HEURISTIC_STOP || ParallelConfig.PARALLEL_SPEC == 11 ||
                        ParallelConfig.PARALLEL_SPEC == 12) {
                    if (splitTable == endPlan.getSplitTable()) {
                        break;
                    }
                }
            }
//            if (roundCtr == 10000) {
//                List<String>[] logs = new List[1];
//                for (int i = 0; i < 1; i++) {
//                    logs[i] = joinOp.logs;
//                }
//                LogUtils.writeLogs(logs, "verbose/lockFree/" + QueryStats.queryName);
//                System.out.println("Write to logs!");
//                System.exit(0);
//            }
            if (JoinConfig.FORGET && roundCtr == nextForget) {
                endPlan.root = new DPNode(roundCtr, query, true, ParallelConfig.EXE_THREADS);
                nextForget *= 10;
            }
        }
        // Materialize result table
        long timer2 = System.currentTimeMillis();
        joinOp.roundCtr = roundCtr;
        System.out.println("Thread " + tid + ": " + (timer2 - timer1) + "\t Round: " + roundCtr);
        Collection<ResultTuple> tuples = joinOp.result.getTuples();
        return new LockFreeResult(tuples, joinOp.logs, tid);
    }

    /**
     * Print out log entry if the maximal number of log
     * entries has not been reached yet.
     *
     * @param logEntry	log entry to print
     */
    static void log(String logEntry) {
        System.out.println(logEntry);
    }
}
