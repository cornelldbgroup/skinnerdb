package joining.parallel.parallelization.search;

import config.ParallelConfig;
import joining.parallel.join.SPJoin;
import joining.parallel.uct.SPNode;
import joining.result.ResultTuple;
import joining.uct.SelectionPolicy;
import preprocessing.Context;
import query.QueryInfo;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

/**
 * The thread task for search parallelization.
 * In the task, a thread will run learning samples
 * and collect results that satisfy with predicates.
 *
 * @author Anonymous
 */
public class SearchTask implements Callable<SearchResult> {
    /**
     * The query to process.
     */
    private final QueryInfo query;
    /**
     * Query processing context.
     */
    private final Context context;
    /**
     * Root of uct tree.
     */
    private final SPNode root;
    /**
     * Search parallel operator.
     */
    private final SPJoin spJoin;
    /**
     * Atomic boolean flag to represent
     * the end of query.
     */
    private final AtomicBoolean finish;
    /**
     * Controls search partitions for each thread.
     */
    private final SearchScheduler scheduler;
    private boolean isSample = true;

    public SearchTask(QueryInfo query, Context context,
                      SPNode root, SPJoin spJoin,
                      SearchScheduler scheduler, AtomicBoolean finish) {
        this.query = query;
        this.context = context;
        this.root = root;
        this.spJoin = spJoin;
        this.scheduler = scheduler;
        this.finish = finish;
    }
    @Override
    public SearchResult call() throws Exception {
        long timer1 = System.currentTimeMillis();
        int tid = spJoin.tid;
        int nrThreads = ParallelConfig.EXE_THREADS;
        int[] joinOrder = new int[query.nrJoined];
        long roundCtr = 0;
        // Get default action selection policy
        SelectionPolicy policy = SelectionPolicy.UCB1;
        // Initialize counter until scale down
        long nextScaleDown = 1;
        // Initialize counter until memory loss
        long nextForget = 10;
        // Initialize plot counter
        int plotCtr = 0;
        // Iterate until join result was generated
        double accReward = 0;
        while (!finish.get()) {
            long roundStart = System.currentTimeMillis();
            ++roundCtr;
            double reward;
            int adaptive = scheduler.adaptiveCtr;
            if (ParallelConfig.PARALLEL_SPEC == 3) {
                if (tid == nrThreads - 1 &&
                        roundCtr == Math.pow(100, adaptive)) {
                    scheduler.redistribution();
                }
            }
            else if (ParallelConfig.PARALLEL_SPEC == 7) {
                if (tid == nrThreads - 1 &&
                        roundCtr == 10 * Math.pow(10, adaptive)) {
                    scheduler.fixOptimalJoinOrder();
                }
            }
            if (scheduler.isChanged[tid]) {
                if (ParallelConfig.PARALLEL_SPEC == 7) {
                    if (tid == scheduler.optimalTid) {
                        joinOrder = scheduler.optimalJoinOrder;
                        spJoin.sid = scheduler.optimalSid;
                        System.out.println(adaptive + ": " + Arrays.toString(joinOrder) + " " + tid + " " + spJoin.sid);
                        isSample = false;
                    }
                    else {
                        isSample = true;
                    }

                }
                else {
                    spJoin.initSearchRoot(scheduler.preNodes, scheduler.nodePerThread.get(tid), scheduler.treeLevel);
                }
                scheduler.isChanged[tid] = false;
            }
            if (ParallelConfig.PARALLEL_SPEC == 7 && !isSample) {
                reward = spJoin.execute(joinOrder, (int) roundCtr);
            }
            else {
                spJoin.sid = -1;
                reward = root.sample(roundCtr, joinOrder, spJoin, policy, true);
            }
            scheduler.updateStatistics(spJoin.sid, reward);
            // Count reward except for final sample
            if (!spJoin.isFinished()) {
                accReward += reward;
            }
            // broadcasting the finished plan.
            else {
                if (!finish.get()) {
                    finish.set(true);
                    spJoin.roundCtr = roundCtr;
                    System.out.println("Finish id: " + tid + "\t" + Arrays.toString(joinOrder) + "\t" + roundCtr);
                }
                break;
            }
            long roundEnd = System.currentTimeMillis();
            spJoin.writeLog("Round Time: " + (roundEnd - roundStart) + " ms");
        }
        long timer2 = System.currentTimeMillis();
        System.out.println("Thread " + tid + " " + (timer2 - timer1)
                + "\tRound: " + roundCtr + "\tOrder: " + Arrays.toString(joinOrder));
        Collection<ResultTuple> tuples = spJoin.result.getTuples();
        return new SearchResult(tuples, spJoin.logs, tid);
    }
}
