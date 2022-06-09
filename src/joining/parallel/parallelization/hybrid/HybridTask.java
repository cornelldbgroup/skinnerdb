package joining.parallel.parallelization.hybrid;

import config.JoinConfig;
import config.ParallelConfig;
import joining.parallel.join.HybridJoin;
import joining.parallel.join.ModJoin;
import joining.parallel.join.OldJoin;
import joining.parallel.parallelization.search.SearchResult;
import joining.parallel.plan.LeftDeepPartitionPlan;
import joining.parallel.progress.ThreadProgressTracker;
import joining.parallel.uct.NSPNode;
import joining.plan.JoinOrder;
import joining.progress.State;
import joining.result.ResultTuple;
import joining.uct.SelectionPolicy;
import preprocessing.Context;
import query.QueryInfo;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class HybridTask implements Callable<SearchResult> {
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
    private final HybridJoin joinOp;
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
     * Number of threads.
     */
    public final int nrThreads;
    /**
     * Concurrent variable to store the next join order.
     */
    public final AtomicReference<JoinPlan> nextJoinOrder;
    /**
     * Concurrent variable to store the next join order.
     */
    public final AtomicReference<JoinPlan>[] nextSPOrders;
    /**
     * Number of forget times.
     */
    public final AtomicInteger nrForgets;
    /**
     * Concurrent map to store the left deep plan.
     */
    public final Map<Integer, LeftDeepPartitionPlan> planCache;
    /**
     * Map to store the next join plan.
     */
    public final Map<Integer, JoinPlan> taskCache;
    /**
     * Number of threads for search parallelization.
     */
    public int nrSPThreads;
    /**
     * Number of threads for data parallelization
     */
    public int nrDPThreads;
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
    public HybridTask(QueryInfo query, Context context,
                      HybridJoin joinOp,
                      int tid, int nrThreads,
                      AtomicBoolean isFinished,
                      AtomicReference<JoinPlan> nextJoinOrder,
                      AtomicInteger nrForgets, AtomicReference<JoinPlan>[] nextSPOrders,
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
        this.nrForgets = nrForgets;
        this.isFinished = isFinished;
        this.tid = tid;
        this.nrThreads = nrThreads;
        this.nextJoinOrder = nextJoinOrder;
        this.nextSPOrders = nextSPOrders;
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
        nrSPThreads = nrThreads;
        nrDPThreads = 0;
        ThreadProgressTracker tracker = tid == 0 ? new ThreadProgressTracker(nrJoined, 1) : null;
        int[] POWERS_OF_10 = {1, 10, 100, 1000, 10000, 100000,
                1000000, 10000000, 100000000, 1000000000};
        while (!isFinished.get()) {
            // Search task
            if (tid < nrSPThreads) {
                ++roundCtr;
                double reward = root.sample(roundCtr, joinOrder, this.joinOp, policy);
                JoinPlan prevPlan = nextJoinOrder.get();
                // Optimal join order
                int[] optimalJoinOrder = root.optimalJoinOrder();
                // Maintain the progress and split table for the slowest thread
//                if (prevPlan != null && prevPlan.tid == tid && nrDPThreads > 0) {
//                    int[] prevOrder = prevPlan.joinOrder;
//                    State prevProgressState = joinOp.tracker.continueFromSP(new JoinOrder(prevOrder));
//                    // Update reward
//                    prevPlan.reward = reward(prevOrder, joinOp.cardinalities, prevProgressState.tupleIndices);
//                    // Calculate the slowest state from the dp threads
//                    int splitTable = prevPlan.splitTable;
//                    double progress;
//                    int slowThread = -1;
//                    int nextSplitTable = -1;
//                    State slowestState = null;
//                    for (int threadCtr = nrSPThreads; threadCtr < nrThreads; threadCtr++) {
//                        int threadIndex = splitTable * nrThreads + threadCtr;
//                        State threadState = prevPlan.states[threadIndex].get();
//                        if (!threadState.isFinished()) {
//                            int largeTable = threadState.lastIndex;
//                            if (slowestState == null ||
//                                    threadState.isAhead(prevOrder, slowestState, nrJoined)) {
//                                slowThread = threadCtr;
//                                nextSplitTable = largeTable;
//                                slowestState = threadState;
//                            }
//                        }
//                        else {
//                            setSplitTable = true;
//                        }
//                    }
//                    progress = slowestState == null ? 0 :
//                            Arrays.stream(slowestState.tupleIndices).sum();
//                    if (slowThread == -1) {
//                        isFinished.set(true);
//                    }
//                    State prevState = prevPlan.slowestState;
//                    if (slowestState != null &&
//                            prevState.isAhead(prevOrder, slowestState, nrJoined)) {
//                        prevPlan.slowestState = slowestState;
//                        joinOp.writeLog("Slowest state: " + slowestState);
//                    }
//                    // Update the split table
//                    if (setSplitTable && progress > 0 && nextSplitTable >= 0 && nextSplitTable != splitTable) {
//                        prevPlan.splitTable = nextSplitTable;
//                        joinOp.writeLog("Set Split Table to: " + nextSplitTable + " " +
//                                Arrays.toString(prevOrder));
//                    }
//                }
//                JoinOrder order = new JoinOrder(optimalJoinOrder);
//                State optimalProgressState = joinOp.tracker.continueFromSP(order);
//                double optimalProgress = reward(optimalJoinOrder, joinOp.cardinalities,
//                        optimalProgressState.tupleIndices);
//                // The optimal join order is different from previous optimal join order
//                if (roundCtr % 10 == 0 &&
//                        (prevPlan == null || (tid == prevPlan.tid &&
//                                !Arrays.equals(optimalJoinOrder, prevPlan.joinOrder)) ||
//                                optimalProgress > prevPlan.reward) && nrDPThreads > 0) {
//                    int joinHash = order.splitHashCode(-1);
//                    LeftDeepPartitionPlan plan = planCache.get(joinHash);
//                    if (plan == null) {
//                        plan = new LeftDeepPartitionPlan(query, joinOp.predToEval, order);
//                        planCache.put(joinHash, plan);
//                    }
//                    // Whether the plan has been generated?
//                    JoinPlan joinPlan = taskCache.get(joinHash);
//                    if (joinPlan == null) {
//                        joinPlan = new JoinPlan(optimalJoinOrder, nrThreads,
//                                nrJoined, optimalProgress, tid, plan);
//                        joinPlan.splitTable = getSplitTableByCard(optimalJoinOrder, joinOp.cardinalities);
//                        taskCache.put(joinHash, joinPlan);
//                    }
////                // Read stable state from the tracker
////                joinPlan.slowestState = joinOp.tracker.continueFromSP(order);
//                    joinOp.writeLog("Set Optimal: " + Arrays.toString(optimalJoinOrder));
//                    nextJoinOrder.set(joinPlan);
//                }
                // Update local optimal join order
                JoinPlan prevLocalPlan = nextSPOrders[tid].get();
                joinOp.writeLog("Optimal Join Order: " + Arrays.toString(optimalJoinOrder));
                if (prevLocalPlan != null && nrDPThreads > 0) {
                    int[] prevOrder = prevLocalPlan.joinOrder;
                    State prevProgressState = joinOp.tracker.continueFromSP(new JoinOrder(prevOrder));
                    // Update reward
                    prevLocalPlan.reward = reward(prevOrder, joinOp.cardinalities, prevProgressState.tupleIndices);
                    joinOp.writeLog("Prev Order: " + Arrays.toString(prevOrder) + "\t" + prevProgressState);
                }
                if ((prevLocalPlan == null ||
                        !Arrays.equals(optimalJoinOrder, prevLocalPlan.joinOrder))
                        && nrDPThreads > 0) {
                    JoinOrder order = new JoinOrder(optimalJoinOrder);
                    State optimalProgressState = joinOp.tracker.continueFromSP(order);
                    double optimalProgress = reward(optimalJoinOrder, joinOp.cardinalities,
                            optimalProgressState.tupleIndices);
                    int joinHash = order.splitHashCode(-1);
                    LeftDeepPartitionPlan plan = planCache.get(joinHash);
                    if (plan == null) {
                        plan = new LeftDeepPartitionPlan(query, joinOp.predToEval, order);
                        planCache.put(joinHash, plan);
                    }
                    // Whether the plan has been generated?
                    JoinPlan joinPlan = taskCache.get(joinHash);
                    if (joinPlan == null) {
                        joinPlan = new JoinPlan(optimalJoinOrder, nrThreads,
                                nrJoined, optimalProgress, tid, plan);
                        joinPlan.splitTable = getSplitTableByCard(optimalJoinOrder, joinOp.cardinalities);
                        taskCache.put(joinHash, joinPlan);
                    }
//                // Read stable state from the tracker
//                joinPlan.slowestState = joinOp.tracker.continueFromSP(order);
                    joinOp.writeLog("Set Local Optimal: " + Arrays.toString(optimalJoinOrder) +
                            "\t" + optimalProgressState);
                    nextSPOrders[tid].set(joinPlan);
                }
                // Master thread need to maintain the progress
                // and choose the best next join order from threads
                if (tid == 0) {
                    if (prevPlan != null) {
                        // Update the slowest progress
                        int splitTable = prevPlan.splitTable;
                        int[] prevOrder = prevPlan.joinOrder;
                        double progress;
                        int slowThread = -1;
                        int nextSplitTable = -1;
                        State slowestState = null;
                        for (int threadCtr = nrSPThreads; threadCtr < nrThreads; threadCtr++) {
                            int threadIndex = splitTable * nrThreads + threadCtr;
                            State threadState = prevPlan.states[threadIndex].get();
                            if (!threadState.isFinished()) {
                                int largeTable = threadState.lastIndex;
                                if (slowestState == null ||
                                        threadState.isAhead(prevOrder, slowestState, nrJoined)) {
                                    slowThread = threadCtr;
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
                        State trackerState = tracker.continueFrom(new JoinOrder(prevOrder), 0);
                        if (slowestState == null || slowestState.isAhead(prevOrder, trackerState, nrJoined)) {
                            slowestState = trackerState;
                        }
                        if (slowestState != null &&
                                prevState.isAhead(prevOrder, slowestState, nrJoined)) {
                            prevPlan.slowestState = slowestState;
                            joinOp.writeLog("Slowest state: " + slowestState);
                            tracker.fastAndUpdate(prevOrder, slowestState, (int) roundCtr,
                                    joinOp.getFirstLargeTable(prevOrder));
                        }
                        // Update the split table
                        if (setSplitTable && progress > 0 && nextSplitTable >= 0 && nextSplitTable != splitTable) {
                            prevPlan.splitTable = nextSplitTable;
                            joinOp.writeLog("Set Split Table to: " + nextSplitTable + " " +
                                    Arrays.toString(prevOrder));
                        }
                    }
                    // Decide whether to change the next optimal join order
                    if (roundCtr % 10 == 0 && nrDPThreads > 0) {
                        double progress = prevPlan == null ? 0 : prevPlan.reward;
                        JoinPlan bestPlan = null;
                        double[] rewards = new double[nrSPThreads];
                        for (int searchCtr = 0; searchCtr < nrSPThreads; searchCtr++) {
                            JoinPlan joinPlan = nextSPOrders[searchCtr].get();
                            double threadProgress = joinPlan == null ? 0 : joinPlan.reward;
                            rewards[searchCtr] = threadProgress;
                            if (threadProgress > progress) {
                                progress = threadProgress;
                                bestPlan = joinPlan;
                            }
                        }
                        joinOp.writeLog("Reward: " + Arrays.toString(rewards));
                        boolean canReplace = bestPlan != null && (prevPlan == null ||
                                !Arrays.equals(bestPlan.joinOrder, prevPlan.joinOrder));
                        if (canReplace) {
                            joinOp.writeLog("Set Optimal: " + Arrays.toString(bestPlan.joinOrder));
                            nextJoinOrder.set(bestPlan);
                        }
                    }
                }

                // Count reward except for final sample
                if (!this.joinOp.isFinished()) {
                    accReward += reward;
                    maxReward = Math.max(reward, maxReward);
                } else {
                    System.out.println("Finish ID: " + tid);
                    isFinished.set(true);
                    break;
                }
                // Consider memory loss
                int curForget = nrForgets.get();
                if (JoinConfig.FORGET && roundCtr >= nextForget) {
                    if (tid == 0) {
                        joinOp.writeLog("Forget Enabled");
                        // TODO: More sophisticated mechanism
                        nrSPThreads = Math.max(1, nrSPThreads / 2);
                        nrDPThreads = nrThreads - nrSPThreads;
                        root = new NSPNode(roundCtr, query, root.useHeuristic,
                                tid, 0, nrSPThreads);
                        nextForget *= 10;
                        nrForgets.getAndIncrement();
                        joinOp.tracker.resetTracker();
                    }
                    else if (POWERS_OF_10[curForget] == nextForget && nrSPThreads > 1) {
                        nrSPThreads = Math.max(1, nrSPThreads / 2);
                        nrDPThreads = nrThreads - nrSPThreads;
                        joinOp.writeLog("Forget Enabled: "  + nrSPThreads);
                        if (tid < nrSPThreads) {
                            root = new NSPNode(roundCtr, query, root.useHeuristic,
                                    tid, 0, nrSPThreads);
                            joinOp.tracker.resetTracker();
                        }
                        else {
                            joinOp.threadTracker.resetTracker();
                        }
                        nextForget *= 10;
                    }
                }
            }
            // Data parallel task
            else {
                JoinPlan joinPlan = nextJoinOrder.get();
                if (joinPlan != null) {
                    ++roundCtr;
                    joinOrder = joinPlan.joinOrder;
                    int splitTable = joinPlan.splitTable;
                    State slowestState = joinPlan.slowestState;

                    joinOp.execute(joinOrder, splitTable, (int) roundCtr, nrDPThreads,
                            slowestState, joinPlan.plan);
                    int largeTable = joinOp.largeTable;
                    boolean threadFinished = this.joinOp.isFinished();
                    double progress = threadFinished ? Double.MAX_VALUE : (joinOp.progress + largeTable);
                    joinPlan.progress[splitTable * nrThreads + tid].set(progress);
                    joinPlan.states[splitTable * nrThreads + tid].set(joinOp.lastState);
                    int curForget = nrForgets.get();
                    if (JoinConfig.FORGET && roundCtr >= nextForget) {
                        if (POWERS_OF_10[curForget] == nextForget && nrSPThreads > 1) {
                            nrSPThreads = Math.max(1, nrSPThreads / 2);
                            nrDPThreads = nrThreads - nrSPThreads;
                            joinOp.writeLog("Forget Enabled: "  + nrSPThreads);
                            joinOp.threadTracker.resetTracker();
                            nextForget *= 10;
                        }
                    }
                    if (threadFinished) {
                        roundCtr--;
                    }
                }
            }
        }
        long timer2 = System.currentTimeMillis();
        String type = tid < nrSPThreads ? "Search" : "Data";
        System.out.println(type + " thread " + tid + " " + (timer2 - timer1)
                + "\tRound: " + roundCtr + "\tOrder: " + Arrays.toString(joinOrder));
        boolean isSearch = tid < nrSPThreads;
        Collection<ResultTuple> tuples = (!isSearch || this.joinOp.isFinished()) ? joinOp.result.getTuples() : null;
        SearchResult searchResult = new SearchResult(tuples, joinOp.logs, tid);
        searchResult.isSearch = isSearch;
        return searchResult;
    }
    /**
     * Get the split table candidate based on cardinalities of tables.
     *
     * @param joinOrder         join order
     * @param cardinalities     cardinalities of tables
     * @return
     */
    public int getSplitTableByCard(int[] joinOrder, int[] cardinalities) {
        if (nrThreads == 1) {
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
