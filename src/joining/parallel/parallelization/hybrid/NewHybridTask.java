package joining.parallel.parallelization.hybrid;

import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMaps;
import config.JoinConfig;
import config.ParallelConfig;
import joining.parallel.join.HybridJoin;
import joining.parallel.parallelization.search.SearchResult;
import joining.parallel.plan.LeftDeepPartitionPlan;
import joining.parallel.progress.ThreadProgressTracker;
import joining.parallel.uct.NSPNode;
import joining.plan.JoinOrder;
import joining.progress.State;
import joining.result.ResultTuple;
import joining.uct.SelectionPolicy;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import preprocessing.Context;
import query.QueryInfo;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class NewHybridTask implements Callable<SearchResult> {
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
    public final AtomicReference<HybridJoinPlan> nextJoinOrder;
    /**
     * Concurrent variable to store the next join order.
     */
    public final AtomicReference<HybridJoinPlan>[] nextSPOrders;
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
    public final Map<Integer, HybridJoinPlan> taskCache;
    /**
     * Thread ids for data parallel.
     */
    public final Set<Integer> dataThreads;
    /**
     * Thread to cover the entire search space
     */
    public final AtomicInteger coverTid;
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
    public NewHybridTask(QueryInfo query, Context context,
                         HybridJoin joinOp,
                         int tid, int nrThreads,
                         AtomicBoolean isFinished,
                         Set<Integer> dataThreads,
                         AtomicInteger coverTid,
                         AtomicReference<HybridJoinPlan> nextJoinOrder,
                         AtomicInteger nrForgets, AtomicReference<HybridJoinPlan>[] nextSPOrders,
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
        this.dataThreads = dataThreads;
        this.nextJoinOrder = nextJoinOrder;
        this.nextSPOrders = nextSPOrders;
        this.planCache = planCache;
        this.taskCache = new HashMap<>();
        this.coverTid = coverTid;
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

        Set<Integer> monitorDPThreads = new HashSet<>(dataThreads);
        Set<Integer> monitorSPThreads = new HashSet<>();
        IntIntMap threadMap = HashIntIntMaps.newMutableMap();
        boolean isData = false;
        for (int threadCtr = 0; threadCtr < nrThreads; threadCtr++) {
            if (!monitorDPThreads.contains(threadCtr)) {
                monitorSPThreads.add(threadCtr);
            }
            else {
                isData = true;
            }
        }
        Iterator<Integer> initDpIter = monitorDPThreads.iterator();
        for (int dpCtr = 0; dpCtr < monitorDPThreads.size(); dpCtr++) {
            int dpTid = initDpIter.next();
            threadMap.put(dpTid, dpCtr);
        }
        boolean isSearch = monitorSPThreads.contains(tid);
        ThreadProgressTracker tracker = tid == 0 ? new ThreadProgressTracker(nrJoined, 1) : null;
        int[] POWERS_OF_10 = {1, 10, 100, 1000, 10000, 100000,
                1000000, 10000000, 100000000, 1000000000};
        int forgetID = 0;
        while (!isFinished.get()) {
            // Search task
            if (isSearch) {
                ++roundCtr;
                double reward = root.sample(roundCtr, joinOrder, this.joinOp, policy);
                joinOp.writeLog("Forget ID: "  + forgetID);
                HybridJoinPlan prevPlan = nextJoinOrder.get();
                // Optimal join order
                int[] optimalJoinOrder = root.optimalJoinOrder();
                // Update local optimal join order
                HybridJoinPlan prevLocalPlan = nextSPOrders[tid].get();
                joinOp.writeLog("Optimal Join Order: " + Arrays.toString(optimalJoinOrder));
                if (prevLocalPlan != null) {
                    int[] prevOrder = prevLocalPlan.joinOrder;
                    State prevProgressState = joinOp.tracker.continueFromSP(new JoinOrder(prevOrder));
                    // Update reward
                    prevLocalPlan.reward = (0.5 * reward(prevOrder, joinOp.cardinalities,
                            prevProgressState.tupleIndices)
                            + 0.5 * joinOp.result.tuples.size() / joinOp.budget) / roundCtr;
                    joinOp.writeLog("Prev Local Order: " + Arrays.toString(prevOrder) + "\t" + prevProgressState +
                            "\t" + prevLocalPlan.reward);
                }
                if ((prevLocalPlan == null ||
                        !Arrays.equals(optimalJoinOrder, prevLocalPlan.joinOrder))) {
                    JoinOrder order = new JoinOrder(optimalJoinOrder);
                    State optimalProgressState = joinOp.tracker.continueFromSP(order);
                    double optimalProgress = (0.5 * reward(optimalJoinOrder, joinOp.cardinalities,
                            optimalProgressState.tupleIndices) +
                            0.5 * joinOp.result.tuples.size() / joinOp.budget) / roundCtr;
                    int joinHash = order.splitHashCode(-1);
                    LeftDeepPartitionPlan plan = planCache.get(joinHash);
                    if (plan == null) {
                        plan = new LeftDeepPartitionPlan(query, joinOp.predToEval, order);
                        planCache.put(joinHash, plan);
                    }
                    // Whether the plan has been generated?
                    HybridJoinPlan joinPlan = taskCache.get(joinHash);
                    if (joinPlan == null) {
                        joinPlan = new HybridJoinPlan(optimalJoinOrder, nrThreads,
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
                        for (int threadCtr: monitorDPThreads) {
                            int threadIndex = splitTable * nrThreads + threadCtr;
                            State threadState = prevPlan.states[forgetID][threadIndex].get();
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
                        State prevState = prevPlan.slowestStates[forgetID].get();
                        State trackerState = tracker.continueFrom(new JoinOrder(prevOrder), 0);
                        trackerState.tid = -2;
                        if (slowestState == null || slowestState.isAhead(prevOrder, trackerState, nrJoined)) {
                            slowestState = trackerState;
                        }
                        if (prevState.isAhead(prevOrder, slowestState, nrJoined)) {
                            prevPlan.slowestStates[forgetID].set(slowestState);
                            joinOp.writeLog("Slowest state: " + slowestState + "\t" + forgetID);
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
                    if (roundCtr % 10 == 0) {
                        double progress = 0;
                        HybridJoinPlan bestPlan = null;
//                        StringBuilder map = new StringBuilder();
                        for (int searchCtr: monitorSPThreads) {
                            HybridJoinPlan joinPlan = nextSPOrders[searchCtr].get();
                            joinOp.writeLog("Checking Plan " + searchCtr + ": " + joinPlan);
                            double threadProgress = joinPlan == null ? 0 : joinPlan.reward;
                            if (threadProgress > progress) {
                                progress = threadProgress;
                                bestPlan = joinPlan;
                            }
                        }
//                        joinOp.writeLog(map.toString());
                        boolean canReplace = bestPlan != null && (prevPlan == null ||
                                !Arrays.equals(bestPlan.joinOrder, prevPlan.joinOrder));
//                        if (canReplace && prevPlan != null && bestPlan.tid != prevPlan.tid &&
//                                (bestPlan.reward - prevPlan.reward) / prevPlan.reward < 0.001) {
//                            canReplace = false;
//                        }
                        joinOp.writeLog("Best Plan: " + bestPlan);
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
                if (JoinConfig.FORGET) {
                    if (tid == 0 && roundCtr >= nextForget) {
                        joinOp.writeLog("Forget Enabled");
                        // TODO: More sophisticated mechanism
                        int nrSPThreads = Math.max(2, monitorSPThreads.size() / 2);
                        List<Pair<Integer, Double>> bestPlans = new ArrayList<>();
                        for (int searchCtr: monitorSPThreads) {
                            HybridJoinPlan joinPlan = nextSPOrders[searchCtr].get();
                            double bestReward = joinPlan == null ? -1 : joinPlan.reward;
                            bestPlans.add(new ImmutablePair<>(searchCtr, bestReward));
                        }
                        bestPlans.sort(Comparator.comparing(Pair::getRight));
                        // Turn inferior threads into data parallel
                        int start = 0;
                        int lastThread = -1;
                        while (monitorSPThreads.size() > nrSPThreads) {
                            Pair<Integer, Double> bestPlan = bestPlans.get(start);
                            int kickTid = bestPlan.getKey();
                            if (kickTid > 0) {
                                monitorSPThreads.remove(kickTid);
                                monitorDPThreads.add(kickTid);
                                dataThreads.add(kickTid);
                            }
                            else {
                                lastThread = 0;
                            }
                            start++;
                        }
                        if (lastThread == -1) {
                            Pair<Integer, Double> bestPlan = bestPlans.get(start);
                            lastThread = bestPlan.getKey();
                        }
                        coverTid.set(lastThread);
                        int endTid = lastThread == 0 ? 1 : nrThreads;
                        root = new NSPNode(roundCtr, query, root.useHeuristic,
                                tid, 0, endTid);
                        nextForget *= 10;
                        forgetID++;
                        nrForgets.getAndIncrement();
                    }
                    else if (curForget > forgetID) {
                        joinOp.writeLog("Forget Enabled: "  + curForget);
                        // Update local threads assignment
                        monitorDPThreads.addAll(dataThreads);
                        if (!monitorDPThreads.contains(tid)) {
                            boolean isCovered = coverTid.get() == tid;
                            if (isCovered) {
                                root = new NSPNode(roundCtr, query, root.useHeuristic,
                                        0, 0, 1);
                            }
                            else {
                                root = new NSPNode(roundCtr, query, root.useHeuristic,
                                        tid, 0, nrThreads);
                            }
                        }
                        else {
                            Iterator<Integer> dpIter = monitorDPThreads.iterator();
                            for (int dpCtr = 0; dpCtr < monitorDPThreads.size(); dpCtr++) {
                                int dpTid = dpIter.next();
                                threadMap.put(dpTid, dpCtr);
                            }
                        }
                        nextForget *= 10;
                        forgetID++;
                        // Check if the thread turns to the data thread.
                        isSearch = !dataThreads.contains(tid);
                    }
                }
            }
            // Data parallel task
            else {
                HybridJoinPlan joinPlan = nextJoinOrder.get();
                if (!isData && forgetID <= 1) {
                    isData = true;
                }
                if (joinPlan != null) {
                    long start = System.currentTimeMillis();
                    ++roundCtr;
                    joinOrder = joinPlan.joinOrder;
                    int splitTable = joinPlan.splitTable;
                    State slowestState = joinPlan.slowestStates[forgetID].get();
                    joinOp.execute(joinOrder, splitTable, (int) roundCtr, monitorDPThreads.size(),
                            slowestState, joinPlan.plan, threadMap);
                    int largeTable = joinOp.largeTable;
                    boolean threadFinished = this.joinOp.isFinished();
                    double progress = threadFinished ? Double.MAX_VALUE : (joinOp.progress + largeTable);
                    joinPlan.progress[forgetID][splitTable * nrThreads + tid].set(progress);
                    joinPlan.states[forgetID][splitTable * nrThreads + tid].set(joinOp.lastState);
                    if (slowestState.tid == tid && threadFinished) {
                        // Check other threads
                        boolean canFinish = true;
                        for (int otherTid: monitorDPThreads) {
                            int threadIndex = splitTable * nrThreads + otherTid;
                            double otherProgress = joinPlan.progress[forgetID][threadIndex].get();
                            if (otherProgress != Double.MAX_VALUE) {
                                canFinish = false;
                                break;
                            }
                        }
                        if (canFinish) {
                            isFinished.set(true);
                            System.out.println(tid + " did it!");
                        }
                    }
                    int curForget = nrForgets.get();
                    if (JoinConfig.FORGET && curForget > forgetID) {
                        joinOp.writeLog("Forget Enabled: "  + curForget);
                        monitorDPThreads.addAll(dataThreads);
                        Iterator<Integer> dpIter = monitorDPThreads.iterator();
                        for (int dpCtr = 0; dpCtr < monitorDPThreads.size(); dpCtr++) {
                            int dpTid = dpIter.next();
                            threadMap.put(dpTid, dpCtr);
                        }
                        joinOp.threadTracker.resetTracker();
                        nextForget *= 10;
                        forgetID++;
                    }
                    if (threadFinished) {
                        roundCtr--;
                    }
                    long end = System.currentTimeMillis();
                    joinOp.writeLog("Episode: " + (end - start));
                }
            }
        }
        long timer2 = System.currentTimeMillis();
        String type = isSearch ? "Search" : "Data";
        System.out.println(type + " thread " + tid + " " + (timer2 - timer1)
                + "\tRound: " + roundCtr + "\tOrder: " + Arrays.toString(joinOrder));
        Collection<ResultTuple> tuples = (!isSearch || this.joinOp.isFinished()) ? joinOp.result.getTuples() : null;
        SearchResult searchResult = new SearchResult(tuples, joinOp.logs, tid);
        searchResult.isSearch = isSearch;
        searchResult.isData = isData;
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
