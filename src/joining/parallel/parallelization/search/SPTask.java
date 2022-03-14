package joining.parallel.parallelization.search;

import config.JoinConfig;
import config.ParallelConfig;
import joining.parallel.join.OldJoin;
import joining.parallel.parallelization.hybrid.JoinPlan;
import joining.parallel.plan.LeftDeepPartitionPlan;
import joining.parallel.uct.NSPNode;
import joining.plan.JoinOrder;
import joining.progress.State;
import joining.result.ResultTuple;
import joining.uct.SelectionPolicy;
import org.apache.commons.lang3.ArrayUtils;
import preprocessing.Context;
import query.QueryInfo;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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
    public final OldJoin joinOp;
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
     * Concurrent queue to store the next join order.
     */
    public final AtomicReference<JoinPlan> nextJoinOrder;
    /**
     * Map to store the next join plan.
     */
    public final Map<Integer, JoinPlan> taskCache;
    /**
     * Concurrent map to store the left deep plan.
     */
    public final Map<Integer, LeftDeepPartitionPlan> planCache;
    /**
     * Whether to make the task runnable?
     */
    public boolean runnable = true;
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
                  AtomicBoolean isFinished,
                  AtomicReference<JoinPlan> nextJoinOrder) {
        this.query = query;
        this.context = context;
        this.root = new NSPNode(0, query,
                JoinConfig.AVOID_CARTESIAN, tid, 0, nrThreads);
        NSPNode node = root.spaceNode();
        if (node.nrActions == 0 && tid != node.startThreads) {
            runnable = false;
        }
        this.joinOp = joinOp;
        this.isFinished = isFinished;
        this.tid = tid;
        this.nextJoinOrder = nextJoinOrder;
        this.taskCache = new HashMap<>();
        this.planCache = new HashMap<>();
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
        boolean isNotified = false;
        while (!isFinished.get()) {
            ++roundCtr;
            double reward = root.sample(roundCtr, joinOrder, this.joinOp, policy);
            if (isNotified) {
                // Optimal join order
                int[] optimalJoinOrder = root.optimalJoinOrder();
                JoinPlan prevPlan = nextJoinOrder.get();
                JoinOrder order = new JoinOrder(optimalJoinOrder);
                int firstAction = ArrayUtils.indexOf(root.nextTable, optimalJoinOrder[0]);
                double firstReward = root.getReward(firstAction);
                State optimalProgressState = joinOp.tracker.continueFromSP(order);
                double optimalProgress = reward(optimalJoinOrder, joinOp.cardinalities,
                        optimalProgressState.tupleIndices);
                // Maintain the progress and split table for the slowest thread
                if (prevPlan != null && prevPlan.tid == tid) {
                    int[] prevOrder = prevPlan.joinOrder;
                    int firstPrevAction = ArrayUtils.indexOf(root.nextTable, prevOrder[0]);
                    double firstPrevReward = root.getReward(firstPrevAction);
                    State prevProgressState = joinOp.tracker.continueFromSP(new JoinOrder(prevOrder));
                    // Update reward
                    prevPlan.reward = joinOp.lastState.isFinished() ? Double.MAX_VALUE :
                            reward(prevOrder, joinOp.cardinalities,
                                    prevProgressState.tupleIndices);
                    prevPlan.firstReward = firstPrevReward;
                }
                // The optimal join order is different from previous optimal join order
                if (roundCtr % 10 == 0 &&
                        (prevPlan == null || (tid == prevPlan.tid &&
                                !Arrays.equals(optimalJoinOrder, prevPlan.joinOrder)) ||
                                (firstReward > prevPlan.firstReward ||
                                        (optimalJoinOrder[0] == prevPlan.joinOrder[0]
                                                && optimalProgress > prevPlan.reward)))) {
                    int joinHash = order.splitHashCode(-1);
                    LeftDeepPartitionPlan plan = planCache.get(joinHash);
                    if (plan == null) {
                        plan = new LeftDeepPartitionPlan(query, joinOp.predToEval, order);
                        planCache.put(joinHash, plan);
                    }
                    // Whether the plan has been generated?
                    JoinPlan joinPlan = taskCache.get(joinHash);
                    if (joinPlan == null) {
                        joinPlan = new JoinPlan(optimalJoinOrder, 1,
                                nrJoined, optimalProgress, tid, plan);
                        taskCache.put(joinHash, joinPlan);
                    }
                    nextJoinOrder.set(joinPlan);
                }
            }
            // Count reward except for final sample
            if (!this.joinOp.isFinished()) {
                accReward += reward;
                maxReward = Math.max(reward, maxReward);
            }
            else {
                System.out.println(Arrays.toString(joinOrder));
                isFinished.set(true);
                break;
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
                + "\tRound: " + roundCtr + "\tOrder: " + Arrays.toString(joinOrder) + "\t"
                + System.currentTimeMillis());
        Collection<ResultTuple> tuples = joinOp.result.getTuples();
        return new SearchResult(tuples, joinOp.logs, tid);
    }
    /**
     * Calculates reward for progress during one invocation.
     *
     * @param joinOrder			join order followed
     * @return					reward between 0 and 1, proportional to progress
     */
    double reward(int[] joinOrder, int[] cardinalities, int[] tupleIndices) {
        double progress = 0;
        double weight = 1;
        for (int curTable : joinOrder) {
            // Scale down weight by cardinality of current table
            int curCard = cardinalities[curTable];
            // Fully processed tuples from this table
            progress += weight * (tupleIndices[curTable] + 0.0) / curCard;
            weight *= 1.0 / curCard;
        }
        return progress;
    }
}
