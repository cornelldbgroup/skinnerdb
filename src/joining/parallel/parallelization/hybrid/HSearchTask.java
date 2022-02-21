package joining.parallel.parallelization.hybrid;

import config.JoinConfig;
import joining.parallel.join.OldJoin;
import joining.parallel.parallelization.search.SearchResult;
import joining.parallel.uct.NSPNode;
import joining.result.ResultTuple;
import joining.uct.SelectionPolicy;
import org.apache.commons.lang3.ArrayUtils;
import preprocessing.Context;
import query.QueryInfo;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class HSearchTask implements Callable<SearchResult> {
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
     * Thread identification.
     */
    private final int tid;
    /**
     * Number of threads for data parallelization.
     */
    private final int nrDPThreadsPerSpace;
    /**
     * Number of threads.
     */
    public final int nrThreads;
    /**
     * Concurrent queue to store the next join order.
     */
    public final AtomicReference<JoinPlan> nextJoinOrder;
    /**
     * @param query
     * @param context
     * @param joinOp
     * @param tid
     * @param nrThreads
     * @param isFinished
     */
    public HSearchTask(QueryInfo query, Context context,
                       OldJoin joinOp,
                       int tid, int nrThreads, int nrDPThreadsPerSpace,
                       AtomicBoolean isFinished,
                       AtomicReference<JoinPlan> nextJoinOrder) {
        this.query = query;
        this.context = context;
        this.root = new NSPNode(0, query,
                JoinConfig.AVOID_CARTESIAN, tid, 0, nrThreads);
        this.joinOp = joinOp;
        this.isFinished = isFinished;
        this.tid = tid;
        this.nrThreads = nrThreads;
        this.nextJoinOrder = nextJoinOrder;
        this.nrDPThreadsPerSpace = nrDPThreadsPerSpace;
    }

    @Override
    public SearchResult call() throws Exception {
        long timer1 = System.currentTimeMillis();
        int nrJoined = query.nrJoined;
        long roundCtr = 0;
        // Get default action selection policy
        SelectionPolicy policy = SelectionPolicy.UCB1;
        // Initialize counter until memory loss
        long nextForget = 10;
        // Iterate until join result was generated
        double accReward = 0;
        double maxReward = Double.NEGATIVE_INFINITY;
        int[] joinOrder = new int[nrJoined];
        while (!isFinished.get()) {
            ++roundCtr;
            double reward = root.sample(roundCtr, joinOrder, this.joinOp, policy);
            // Optimal join order
            int[] optimalJoinOrder = root.optimalJoinOrder();
            int leftTable = optimalJoinOrder[0];
            int firstAction = ArrayUtils.indexOf(root.nextTable, leftTable);
            double firstReward = root.getReward(firstAction);
            nextJoinOrder.updateAndGet(prevPlan -> prevPlan == null || firstReward > prevPlan.reward
                    || tid == prevPlan.tid ?
                    new JoinPlan(optimalJoinOrder, nrDPThreadsPerSpace,
                            nrJoined, firstReward, tid, null) : prevPlan);
            // Count reward except for final sample
            if (!this.joinOp.isFinished()) {
                accReward += reward;
                maxReward = Math.max(reward, maxReward);
            } else {
                isFinished.set(true);
            }
            // Consider memory loss
            if (JoinConfig.FORGET && roundCtr == nextForget) {
                root = new NSPNode(roundCtr, query, root.useHeuristic,
                        tid, 0, nrThreads);
                nextForget *= 10;
            }
        }
        long timer2 = System.currentTimeMillis();
        System.out.println("Search thread " + tid + " " + (timer2 - timer1)
                + "\tRound: " + roundCtr + "\tOrder: " + Arrays.toString(joinOrder));
        Collection<ResultTuple> tuples = joinOp.result.getTuples();
        return new SearchResult(tuples, joinOp.logs, tid);
    }
}
