package joining.parallel.parallelization.lockfree;

import config.JoinConfig;
import config.LoggingConfig;
import config.ParallelConfig;
import joining.parallel.join.DPJoin;
import joining.parallel.parallelization.EndPlan;
import joining.parallel.parallelization.NewEndPlan;
import joining.parallel.uct.DPNode;
import joining.progress.State;
import joining.result.ResultTuple;
import joining.uct.SelectionPolicy;
import preprocessing.Context;
import query.QueryInfo;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public class DPTask implements Callable<LockFreeResult>{
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
    private final NewEndPlan endPlan;
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
    /**
    **/
    public final int[] optimal;

    public DPTask(QueryInfo query, Context context, DPNode root, NewEndPlan endPlan,
                  AtomicBoolean finish, AtomicBoolean terminated, ReentrantLock lock, DPJoin dpJoin) {
        this.query = query;
        this.context = context;
        this.root = root;
        this.endPlan = endPlan;
        this.finish = finish;
        this.terminated = terminated;
        this.lock = lock;
        this.joinOp = dpJoin;
        this.optimal = new int[query.nrJoined];
        optimal[0] = -1;
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
//            long optimizeStart = System.currentTimeMillis();
//            System.out.println( "Optimize Start: " + optimizeStart);
            DPNode root = endPlan.root;
            ++roundCtr;
            double reward = 0;
            int splitTable = endPlan.getSplitTable();
            if (splitTable != -1) {
                joinOrder = endPlan.joinOrder;
                joinOp.isShared = true;

                int prevTable;
                State slowState = endPlan.slowestStates[splitTable].get();
                if (finishedTables.contains(splitTable)) {
                    int table = root.getSplitTableByCard(joinOrder, joinOp.cardinalities, finishedTables);
                    if (table == -1) {
                        break;
                    }
                    reward = joinOp.execute(joinOrder, table, (int) roundCtr, endPlan.finishFlags, slowState);
                }
                else {
                    reward = joinOp.execute(joinOrder, splitTable, (int) roundCtr, endPlan.finishFlags, slowState);
                    int largeTable = joinOp.largeTable;
                    prevTable = joinOp.lastTable;
                    State prevState = joinOp.lastState;
                    // Maintain the slowest state
//                        boolean isSlow = endPlan.isSlow(prevState, tid, prevTable, joinOp);
                    State slow = endPlan.getSlowState(prevTable);
                    if (LoggingConfig.PARALLEL_JOIN_VERBOSE) {
                        joinOp.writeLog("Current Slow State: " + slow);
                    }
                    boolean isSlow = slow.tid == tid;
                    if (isSlow && !prevState.isFinished()) {
                        State curSlow = endPlan.setSplitTable(largeTable, slow, splitTable);
//                            joinOp.writeLog("Set split Table to: " + largeTable + "\tSlow: " +
//                                    curSlow.toString() + " " + System.currentTimeMillis());
                    }
//                        else {
//                            State slow = endPlan.getSlowState(prevTable);
//                            joinOp.writeLog("Current Slow State: " + slow);
//                        }
                }
            }
            else {
                if (optimal[0] >= 0) {
                    int split = root.getSplitTableByCard(optimal, joinOp.cardinalities);
                    System.arraycopy(optimal, 0, joinOrder, 0, nrJoined);
                    reward = joinOp.execute(optimal, split, (int) roundCtr);
                }
                else {
                    reward = root.sample(roundCtr, joinOrder, joinOp, policy);
                }
            }
//            long optimizeEnd = System.currentTimeMillis();

            // Count reward except for final sample
            if (!joinOp.isFinished()) {
                accReward += reward;
            }
            // broadcasting the finished plan.
            else {
                int prevSplitTable = joinOp.lastTable;
                if (finish.compareAndSet(false, true)) {
                    System.out.println(tid + " shared: " + Arrays.toString(joinOrder) + " splitting " + prevSplitTable);
                    endPlan.setJoinOrder(joinOrder);
                    endPlan.setSplitTable(prevSplitTable);
                }
                // TODO
                boolean isFinished = endPlan.setFinished(tid, prevSplitTable);
                finishedTables.add(prevSplitTable);
                if (isFinished) {
                    terminated.set(true);
                    break;
                }
            }
//            System.out.println(roundCtr + ": " + (optimizeEnd - optimizeStart));
            if (roundCtr == 100000) {
                long timer2 = System.currentTimeMillis();
                joinOp.roundCtr = roundCtr;
                System.out.println("Thread " + tid + ": " + (timer2 - timer1) + "\t Round: " + roundCtr);
                Collection<ResultTuple> tuples = joinOp.result.getTuples();
                return new LockFreeResult(tuples, joinOp.logs, tid);
            }
            if (JoinConfig.FORGET && roundCtr == nextForget && tid == 0) {
                endPlan.root = new DPNode(roundCtr, query, JoinConfig.AVOID_CARTESIAN, ParallelConfig.EXE_THREADS);
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
