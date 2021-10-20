package joining.parallel.parallelization.search;

import config.JoinConfig;
import config.ParallelConfig;
import joining.parallel.join.OldJoin;
import joining.parallel.join.SPJoin;
import joining.parallel.uct.NSPNode;
import joining.parallel.uct.SPNode;
import joining.result.ResultTuple;
import joining.uct.SelectionPolicy;
import preprocessing.Context;
import query.QueryInfo;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The thread task for search parallelization.
 * In the task, a thread will run learning samples
 * and collect results that satisfy with predicates.
 *
 * @author Anonymous
 */
public class SPTask implements Callable<SearchResult> {
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
    private NSPNode root;
    /**
     * Search parallel operator.
     */
    private final OldJoin joinOp;
    /**
     * Atomic boolean flag to represent
     * the end of query.
     */
    private final AtomicBoolean isFinished;
    /**
     * Thread identification
     */
    private final int tid;
    /**
     *
     *
     * @param query
     * @param context
     * @param joinOp
     * @param tid
     * @param nrThreads
     * @param isFinished
     */
    public SPTask(QueryInfo query, Context context,
                  OldJoin joinOp,
                  int tid, int nrThreads,
                  AtomicBoolean isFinished) {
        this.query = query;
        this.context = context;
        this.root = new NSPNode(0, query,
                JoinConfig.AVOID_CARTESIAN, tid, 0, nrThreads);
        this.joinOp = joinOp;
        this.isFinished = isFinished;
        this.tid = tid;
    }
    @Override
    public SearchResult call() throws Exception {
        long timer1 = System.currentTimeMillis();
        int nrThreads = ParallelConfig.EXE_THREADS;
        int nrJoined = query.nrJoined;
        int[] joinOrder = new int[nrJoined];
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
        double maxReward = Double.NEGATIVE_INFINITY;
        while (!isFinished.get()) {
            ++roundCtr;
            double reward = root.sample(roundCtr, joinOrder, this.joinOp, policy);
            // Count reward except for final sample
            if (!this.joinOp.isFinished()) {
                accReward += reward;
                maxReward = Math.max(reward, maxReward);
            }
            else {
                isFinished.set(true);
            }
            // Consider memory loss
            if (JoinConfig.FORGET && roundCtr==nextForget) {
                root = new NSPNode(roundCtr, query, root.useHeuristic,
                        tid, 0, nrThreads);
                nextForget *= 10;
            }
        }
        long timer2 = System.currentTimeMillis();
        System.out.println("Thread " + tid + " " + (timer2 - timer1)
                + "\tRound: " + roundCtr + "\tOrder: " + Arrays.toString(joinOrder));
        Collection<ResultTuple> tuples = joinOp.result.getTuples();
        return new SearchResult(tuples, joinOp.logs, tid);
    }
}
