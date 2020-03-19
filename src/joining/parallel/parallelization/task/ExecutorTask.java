package joining.parallel.parallelization.task;

import config.JoinConfig;
import config.ParallelConfig;
import joining.parallel.join.SPJoin;
import joining.parallel.parallelization.tree.TreeResult;
import joining.parallel.uct.ASPNode;
import joining.parallel.uct.SPNode;
import joining.result.ResultTuple;
import joining.uct.SelectionPolicy;
import joining.uct.UctNode;
import logs.LogUtils;
import query.QueryInfo;
import statistics.QueryStats;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

public class ExecutorTask implements Callable<TaskResult> {
    /**
     * The query to process.
     */
    private final QueryInfo query;
    /**
     * Multi-way join operator.
     */
    private final SPJoin spJoin;
    /**
     * Shared atomic flags among all threads.
     * It indicates whether the join finishes.
     */
    private final AtomicBoolean finish;
    /**
     * The best join order assigned to
     * each executor threads by the searching thread.
     */
    private int[][] bestJoinOrder;

    public ExecutorTask(QueryInfo query, SPJoin spJoin, AtomicBoolean finish, int[][] bestJoinOrder) {
        this.query = query;
        this.spJoin = spJoin;
        this.finish = finish;
        this.bestJoinOrder = bestJoinOrder;
    }

    @Override
    public TaskResult call() throws Exception {
        long timer1 = System.currentTimeMillis();
        int tid = spJoin.tid;
        int nrTables = query.nrJoined;
        long roundCtr = 0;
        // Get default action selection policy
        SelectionPolicy policy = SelectionPolicy.UCB1;
        // Initialize counter until memory loss
        long nextForget = 10;
        // Iterate until join result was generated
        double accReward = 0;

        if (tid == 0) {
            int[] joinOrder = new int[nrTables];
            int nrThreads = ParallelConfig.EXE_THREADS;
            int nextThread = 1;
            int lastCount = 0;
            int nextPeriod = 1;
            double nextNum = 1;
            double base  = Math.pow(ParallelConfig.C, 1.0 / nrThreads);
            SPNode root = new SPNode(0, query, true, 1);
            while (!finish.get()) {
                ++roundCtr;
                double reward;
                reward = root.sample(roundCtr, joinOrder, spJoin, policy, true);
                // Count reward except for final sample
                if (!spJoin.isFinished()) {
                    accReward += reward;
                }
                else {
                    if (finish.compareAndSet(false, true)) {
                        System.out.println("Finish id: " + tid + "\t" + Arrays.toString(joinOrder) + "\t" + roundCtr);
                        spJoin.roundCtr = roundCtr;
                    }
                    break;
                }
                // assign the best join order to next thread.
                if (roundCtr == lastCount + nextPeriod && nrThreads > 1) {
                    int[] best = new int[nrTables];
                    root.maxJoinOrder(best, 0);
                    System.arraycopy(best, 0, bestJoinOrder[nextThread], 0, nrTables);
                    bestJoinOrder[nextThread][nrTables] = 1;
                    System.out.println("Assign " + Arrays.toString(best)
                            + " to Thread " + nextThread + " at round " + roundCtr);
                    nextThread = (nextThread + 1) % nrThreads;
                    if (nextThread == 0) {
                        nextThread++;
                    }
                    lastCount = (int) roundCtr;
                    nextNum = nextNum * base;
                    nextPeriod = (int) Math.round(nextNum);

                }

                // Consider memory loss
                if (JoinConfig.FORGET && roundCtr==nextForget) {
                    root = new SPNode(0, query, true, 1);
                    nextForget *= 10;
                }

//                if (roundCtr == 100000) {
//                    List<String>[] logs = new List[1];
//                    for (int i = 0; i < 1; i++) {
//                        logs[i] = spJoin.logs;
//                    }
//                    LogUtils.writeLogs(logs, "verbose/task/" + QueryStats.queryName);
//                    System.out.println("Write to logs!");
//                    System.exit(0);
//                }
            }
        }
        else {
            int[] order = bestJoinOrder[tid];
            int[] joinOrder = new int[nrTables];
            joinOrder[0] = -1;
            while (!finish.get()) {
                if (joinOrder[0] >= 0) {
                    ++roundCtr;
                    double reward = spJoin.execute(joinOrder, (int) roundCtr);
                    // Count reward except for final sample
                    if (!spJoin.isFinished()) {
                        accReward += reward;
                    }
                    else {
                        if (finish.compareAndSet(false, true)) {
                            System.out.println("Finish id: " + tid + "\t" +
                                    Arrays.toString(joinOrder) + "\t" + roundCtr);
                            spJoin.roundCtr = roundCtr;
                        }
                        break;
                    }
                }
                if (order[nrTables] == 1) {
                    System.arraycopy(order, 0, joinOrder, 0, nrTables);
                    order[nrTables] = 0;
                }
            }
        }

        // Materialize result table
        long timer2 = System.currentTimeMillis();
        spJoin.roundCtr = roundCtr;
        System.out.println("Thread " + tid + " " + (timer2 - timer1) + "\t Round: " + roundCtr);
        Set<ResultTuple> tuples = spJoin.result.tuples;
        return new TaskResult(tuples, spJoin.logs, tid);
    }
}
