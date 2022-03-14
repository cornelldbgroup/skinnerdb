package joining.parallel.parallelization.hybrid;

import config.JoinConfig;
import config.ParallelConfig;
import joining.parallel.join.OldJoin;
import joining.parallel.parallelization.search.SearchResult;
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
     * Concurrent map to store the left deep plan.
     */
    public final Map<Integer, LeftDeepPartitionPlan> planCache;
    /**
     * Map to store the next join plan.
     */
    public final Map<Integer, JoinPlan> taskCache;
    /**
     * Whether to make the task runnable?
     */
    public boolean runnable = true;

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
                       AtomicReference<JoinPlan> nextJoinOrder,
                       Map<Integer, LeftDeepPartitionPlan> planCache) {
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
        this.nrThreads = nrThreads;
        this.nextJoinOrder = nextJoinOrder;
        this.nrDPThreadsPerSpace = nrDPThreadsPerSpace;
        this.planCache = planCache;
        this.taskCache = new HashMap<>();
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
        boolean setSplitTable = false;
        while (!isFinished.get()) {
            ++roundCtr;
            double reward = root.sample(roundCtr, joinOrder, this.joinOp, policy);
            // Optimal join order
            int[] optimalJoinOrder = root.optimalJoinOrder();

            JoinPlan prevPlan = nextJoinOrder.get();
            // Maintain the progress and split table for the slowest thread
            if (prevPlan != null && prevPlan.tid == tid) {
                int[] prevOrder = prevPlan.joinOrder;
                State prevProgressState = joinOp.tracker.continueFromSP(new JoinOrder(prevOrder));
                // Update reward
                prevPlan.reward = reward(prevOrder, joinOp.cardinalities, prevProgressState.tupleIndices);
                // Calculate the slowest state from the dp threads
                int splitTable = prevPlan.splitTable;
                double progress = Double.MAX_VALUE;
                int slowThread = -1;
                int nextSplitTable = -1;
                State slowestState = null;
                for (int threadCtr = 0; threadCtr < nrDPThreadsPerSpace; threadCtr++) {
                    int threadIndex = splitTable * nrDPThreadsPerSpace + threadCtr;
                    State threadState = prevPlan.states[threadIndex].get();
//                    double value = prevPlan.progress[threadIndex].get();
                    if (!threadState.isFinished()) {
//                        int largeTable = (int) value;
                        int largeTable = threadState.lastIndex;
//                        double threadProgress = value - largeTable;
                        if (slowestState == null ||
                                threadState.isAhead(prevOrder, slowestState, nrJoined)) {
                            slowThread = threadCtr;
//                            progress = threadProgress;
                            nextSplitTable = largeTable;
                            slowestState = threadState;
                        }
                    }
                    else {
                        setSplitTable = true;
                    }
                }
                progress = slowestState == null ? 0 :
                        Arrays.stream(slowestState.tupleIndices).sum();
                if (slowThread == -1) {
                    isFinished.set(true);
                }
                State prevState = prevPlan.slowestState;
                if (slowestState != null &&
                        prevState.isAhead(prevOrder, slowestState, nrJoined)) {
                    prevPlan.slowestState = slowestState;
                    joinOp.writeLog("Slowest state: " + slowestState);
                }
                // Update the split table
                if (setSplitTable && progress > 0 && nextSplitTable >= 0 && nextSplitTable != splitTable) {
                    prevPlan.splitTable = nextSplitTable;
                    joinOp.writeLog("Set Split Table to: " + nextSplitTable + " " +
                            Arrays.toString(prevOrder));
                }
            }
            JoinOrder order = new JoinOrder(optimalJoinOrder);
            State optimalProgressState = joinOp.tracker.continueFromSP(order);
            double optimalProgress = reward(optimalJoinOrder, joinOp.cardinalities,
                    optimalProgressState.tupleIndices);
            // The optimal join order is different from previous optimal join order
            if (roundCtr % 10 == 0 &&
                    (prevPlan == null || (tid == prevPlan.tid &&
                    !Arrays.equals(optimalJoinOrder, prevPlan.joinOrder)) ||
                    optimalProgress > prevPlan.reward)) {
                int joinHash = order.splitHashCode(-1);
                LeftDeepPartitionPlan plan = planCache.get(joinHash);
                if (plan == null) {
                    plan = new LeftDeepPartitionPlan(query, joinOp.predToEval, order);
                    planCache.put(joinHash, plan);
                }
                // Whether the plan has been generated?
                JoinPlan joinPlan = taskCache.get(joinHash);
                if (joinPlan == null) {
                    joinPlan = new JoinPlan(optimalJoinOrder, nrDPThreadsPerSpace,
                            nrJoined, optimalProgress, tid, plan);
                    joinPlan.splitTable = getSplitTableByCard(optimalJoinOrder, joinOp.cardinalities);
                    taskCache.put(joinHash, joinPlan);
                }
//                // Read stable state from the tracker
//                joinPlan.slowestState = joinOp.tracker.continueFromSP(order);
                joinOp.writeLog("Set Optimal: " + Arrays.toString(optimalJoinOrder));
                nextJoinOrder.set(joinPlan);
            }

            // Count reward except for final sample
            if (!this.joinOp.isFinished()) {
                accReward += reward;
                maxReward = Math.max(reward, maxReward);
            } else {
                isFinished.set(true);
                break;
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
    /**
     * Get the split table candidate based on cardinalities of tables.
     *
     * @param joinOrder         join order
     * @param cardinalities     cardinalities of tables
     * @return
     */
    public int getSplitTableByCard(int[] joinOrder, int[] cardinalities) {
        if (nrDPThreadsPerSpace == 1) {
            return 0;
        }
        int splitLen = 5;
        int splitSize = ParallelConfig.PARTITION_SIZE;
        int nrJoined = query.nrJoined;
        int splitTable = joinOrder[0];
        int end = Math.min(splitLen, nrJoined);
        int start = nrJoined <= splitLen + 1 ? 0 : 1;
        for (int i = start; i < end; i++) {
            int table = joinOrder[i];
            int cardinality = cardinalities[table];
            if (cardinality >= splitSize && !query.temporaryTables.contains(table)) {
                splitTable = table;
                break;
            }
        }
        return splitTable;
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
