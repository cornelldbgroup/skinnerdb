package joining.parallel.parallelization.lockfree;

import config.ParallelConfig;
import joining.parallel.uct.SimpleUctNode;
import joining.result.ResultTuple;
import joining.uct.SelectionPolicy;
import joining.parallel.join.DPJoin;
import joining.parallel.parallelization.EndPlan;
import joining.parallel.uct.DPNode;
import logs.LogUtils;
import preprocessing.Context;
import query.QueryInfo;
import statistics.QueryStats;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public class LockFreeTask implements Callable<LockFreeResult>{
    /**
     * Query to process.
     */
    private final QueryInfo query;
    private final Context context;
    /**
     * The root of parallel UCT tree.
     */
    private DPNode root;
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
        int[] joinOrder = new int[query.nrJoined];
        long roundCtr = 0;
        // Get default action selection policy
        SelectionPolicy policy = SelectionPolicy.UCB1;
        // Initialize counter until scale down
        long nextScaleDown = 1;
        // Initialize counter until memory loss
        long nextForget = 1;
        // Initialize plot counter
        int plotCtr = 0;
        // Iterate until join result was generated
        double accReward = 0;
        joinOp.slowThreads = new int[query.nrJoined];
        Arrays.fill(joinOp.slowThreads, -1);
        while (!terminated.get()) {
            ++roundCtr;
            double reward;
            int finalTable = endPlan.getSplitTable();
            if (finalTable != -1) {
                joinOrder = endPlan.getJoinOrder();
                joinOp.isShared = true;

                if (ParallelConfig.HEURISTIC_SHARING) {
                    int largeTable = joinOp.largeTable;
                    if (joinOp.slowest && largeTable != finalTable && joinOp.noProgressOnSplit) {
                        endPlan.setSplitTable(largeTable);
                    }
                    boolean isFinished  = false;
                    if (joinOp.isFinished()) {
                        isFinished = endPlan.setFinished(tid, joinOp.lastTable);
                    }
                    if (isFinished) {
                        terminated.set(true);
                        break;
                    }
                    else {
                        continue;
                    }
                }
                else if (ParallelConfig.HEURISTIC_STOP) {
                    reward = joinOp.execute(joinOrder, finalTable, (int) roundCtr);
                    if (joinOp.isFinished()) {
                        break;
                    }
                }
                else {
                    System.arraycopy(endPlan.slowThreads, 0, joinOp.slowThreads, 0, query.nrJoined);
                    if (query.nrJoined < 5 || ParallelConfig.EXE_THREADS <= 10) {
                        reward = joinOp.execute(joinOrder, finalTable, (int) roundCtr);
                        if (joinOp.isFinished()) {
                            break;
                        }
                        else {
                            continue;
                        }
                    }
                    else {
                        int uctTable = endPlan.tableRoot.getMaxOrderedUCTTable();
                        reward = joinOp.execute(joinOrder, uctTable, (int) roundCtr);
                        boolean isFinished  = false;

                        if (joinOp.isFinished()) {
                            isFinished = endPlan.setFinished(tid, joinOp.lastTable);
                        }
                        else {
                            endPlan.tableRoot.updateStatistics(uctTable, reward);
                        }
                        if (isFinished) {
                            terminated.set(true);
                            break;
                        }
                        else {
                            continue;
                        }
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
                if (!finish.get()) {
                    lock.lock();
                    if (!finish.get()) {
                        System.out.println(tid + " shared: " + Arrays.toString(joinOrder) + " splitting " + splitTable);
                        endPlan.tableRoot = new SimpleUctNode(joinOrder, joinOp.cardinalities);
                        endPlan.setJoinOrder(joinOrder);
                        endPlan.setSplitTable(splitTable);
                        finish.set(true);
                    }
                    lock.unlock();
                }
                if (query.nrJoined < 5 || ParallelConfig.EXE_THREADS <= 10) {
                    break;
                }
                boolean isFinished = endPlan.setFinished(tid, joinOp.lastTable);
                if (isFinished) {
                    terminated.set(true);
                    break;
                }
                if (ParallelConfig.HEURISTIC_STOP) {
                    if (splitTable == endPlan.getSplitTable()) {
                        break;
                    } else {
                        System.out.println(tid + ": bad restart");
                    }
                }
            }
//            joinOp.writeLog("Episode Time: " + (end - start) + "\tReward: " + reward);

        }

        // Update statistics
//        JoinStats.nrSamples = roundCtr;
//        JoinStats.avgReward = accReward/roundCtr;
//        JoinStats.maxReward = maxReward;
//        JoinStats.totalWork = 0;
//        for (int tableCtr=0; tableCtr<query.nrJoined; ++tableCtr) {
//            if (tableCtr == joinOrder[0]) {
//                JoinStats.totalWork += 1;
//            } else {
//                JoinStats.totalWork += Math.max(
//                        joinOp.tracker.tableOffset[tableCtr],0)/
//                        (double)joinOp.cardinalities[tableCtr];
//            }
//        }
//        // Output final stats if join logging enabled
//        if (LoggingConfig.MAX_JOIN_LOGS > 0) {
//            System.out.println("Exploration weight:\t" +
//                    JoinConfig.EXPLORATION_WEIGHT);
//            System.out.println("Nr. rounds:\t" + roundCtr);
//            System.out.println("Table offsets:\t" +
//                    Arrays.toString(joinOp.tracker.tableOffset));
//            System.out.println("Table cards.:\t" +
//                    Arrays.toString(joinOp.cardinalities));
//        }
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
