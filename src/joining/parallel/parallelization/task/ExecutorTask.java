package joining.parallel.parallelization.task;

import config.JoinConfig;
import config.ParallelConfig;
import config.StartupConfig;
import joining.parallel.join.FixJoin;
import joining.parallel.join.SubJoin;
import joining.parallel.uct.SPNode;
import joining.result.ResultTuple;
import joining.uct.SelectionPolicy;
import query.QueryInfo;
import statistics.JoinStats;
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
    private final FixJoin spJoin;
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
    /**
     * The probability for each prefix in the best join order assigned to
     * each executor threads by the searching thread.
     */
    private double[][] bestProbs;
    /**
     * Multiple join operators for threads
     */
    private final List<FixJoin> fixJoins;

    public ExecutorTask(QueryInfo query, FixJoin spJoin, AtomicBoolean finish,
                        int[][] bestJoinOrder, double[][] bestProbs,
                        List<FixJoin> fixJoins) {
        this.query = query;
        this.spJoin = spJoin;
        this.finish = finish;
        this.bestJoinOrder = bestJoinOrder;
        this.bestProbs = bestProbs;
        this.fixJoins = fixJoins;
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
            int nextPeriod = ParallelConfig.C;
            double nextNum = 1;
            int nrExecutors = Math.min(ParallelConfig.NR_EXECUTORS, nrThreads - 1);
//            double base = Math.pow(ParallelConfig.C, 1.0 / (nrThreads-1));
            double base = ParallelConfig.C;
            SPNode root = new SPNode(0, query, true, 1);
            SubJoin subJoin = new SubJoin(query, spJoin.preSummary, spJoin.budget, nrThreads, 0, spJoin.predToEval);
//            FixJoin subJoin = new FixJoin(query, spJoin.preSummary, spJoin.budget, nrThreads, 0, spJoin.predToEval, 1);
            subJoin.tracker = spJoin.tracker;
            while (!finish.get()) {
                ++roundCtr;
                double reward;
                reward = root.sample(roundCtr, joinOrder, subJoin, policy, true);
                // Count reward except for final sample
                if (!subJoin.isFinished()) {
                    accReward += reward;
                }
                else {
                    if (finish.compareAndSet(false, true)) {
                        System.out.println("Finish id: " + tid + "\t" + Arrays.toString(joinOrder) + "\t" + roundCtr);
                        subJoin.roundCtr = roundCtr;
                        for (FixJoin fixJoin: fixJoins) {
                            fixJoin.terminate.set(true);
                        }
                    }
                    break;
                }
                // assign the best join order to next thread.
                if (roundCtr == lastCount + nextPeriod && nrExecutors >= 1) {
                    int[] best = new int[nrTables];
                    double[] probs = new double[nrTables];
                    root.maxJoinOrder(best, 0, probs);
                    for (int i = 1; i < nrTables; i++) {
                        probs[i] = probs[i-1] * probs[i];
                    }
                    for (int i = 0; i < nrTables - 1; i++) {
                        probs[i] = probs[i] - probs[i+1];
                    }
                    boolean equal = true;
                    for (int i = 0; i < nrTables; i++) {
                        if (best[i] != bestJoinOrder[nextThread][i]) {
                            equal = false;
                            break;
                        }
                    }
                    if (!equal) {
//                        best = new int[]{0, 2, 3, 4, 1, 5, 6, 7};
                        System.arraycopy(best, 0, bestJoinOrder[nextThread], 0, nrTables);
                        System.arraycopy(probs, 0, bestProbs[nextThread], 0, nrTables);
                        bestJoinOrder[nextThread][nrTables] = 2;
                        fixJoins.get(nextThread).terminate.set(true);
                        System.out.println("Assign " + Arrays.toString(best)
                                + " to Executor " + nextThread + " at round " + roundCtr + " " + System.currentTimeMillis());
                    }

                    nextThread = (nextThread + 1) % (nrExecutors + 1);
                    if (nextThread == 0) {
                        nextThread = (nextThread + 1) % (nrExecutors + 1);
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
                subJoin.roundCtr = roundCtr;
                spJoin.roundCtr = roundCtr;
                spJoin.statsInstance.nrTuples = subJoin.statsInstance.nrTuples;
            }
            // memory consumption
            if (StartupConfig.Memory) {
                JoinStats.treeSize = root.getSize(true);
                JoinStats.stateSize = subJoin.tracker.getSize();
            }
            // Materialize result table
            long timer2 = System.currentTimeMillis();
            System.out.println("Thread " + tid + " " + (timer2 - timer1)
                    + "\tRound: " + roundCtr + "\tOrder: " + Arrays.toString(joinOrder));
            Set<ResultTuple> tuples = subJoin.result.tuples;
//            spJoin.threadResultsList = subJoin.threadResultsList;
            return new TaskResult(tuples, subJoin.logs, tid);
        }
        else {
            int[] order = bestJoinOrder[tid];
            double[] probs = bestProbs[tid];
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
                            for (FixJoin fixJoin: fixJoins) {
                                fixJoin.terminate.set(true);
                            }
                        }
                        break;
                    }
                }
                System.arraycopy(order, 0, joinOrder, 0, nrTables);
                System.arraycopy(probs, 0, spJoin.joinProbs, 0, nrTables);
            }
            // Materialize result table
            long timer2 = System.currentTimeMillis();
            spJoin.roundCtr = roundCtr;
            System.out.println("Thread " + tid + " " + (timer2 - timer1)
                    + "\tRound: " + roundCtr + "\tOrder: " + Arrays.toString(joinOrder));
            Set<ResultTuple> tuples = spJoin.result.tuples;
            if (StartupConfig.Memory) {
                long size = 0;
                for (long batchSize: spJoin.maxSizes) {
                    size += batchSize;
                }
                JoinStats.joinSize = Math.max(size, tuples.size()) * nrTables * 4 + 2000000;
            }
            return new TaskResult(tuples, spJoin.logs, tid);
        }
    }
}
