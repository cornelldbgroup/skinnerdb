package joining.parallel.parallelization.search;

import config.JoinConfig;
import config.ParallelConfig;
import joining.parallel.join.OldJoin;
import joining.parallel.parallelization.hybrid.JoinPlan;
import joining.parallel.plan.LeftDeepPartitionPlan;
import joining.parallel.uct.ASPNode;
import joining.parallel.uct.NASPNode;
import joining.parallel.uct.NSPNode;
import joining.result.ResultTuple;
import joining.uct.SelectionPolicy;
import preprocessing.Context;
import query.QueryInfo;

import java.util.*;
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
public class ASPTask implements Callable<SearchResult> {
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
    private NASPNode root;
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
     * Whether to make the task runnable?
     */
    public boolean isSmall = false;
    /**
     * Atomic reference for adaptive search space assignment.
     */
    public final AtomicReference<List<Set<Integer>>>[] assignment;
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
    public ASPTask(QueryInfo query, Context context,
                   OldJoin joinOp, int tid, int nrThreads,
                   AtomicReference<List<Set<Integer>>>[] assignment,
                   NASPNode firstRoot, AtomicBoolean isFinished) {
        this.query = query;
        this.context = context;
        this.root = firstRoot;
        this.joinOp = joinOp;
        this.isFinished = isFinished;
        this.tid = tid;
        this.assignment = assignment;
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
        long nextForget = 100;
        // Initialize plot counter
        int plotCtr = 0;
        // Iterate until join result was generated
        double accReward = 0;
        double maxReward = Double.NEGATIVE_INFINITY;
        boolean needForget = false;
        List<Set<Integer>> threadAssignment = null;
        double maxProgress = 0;
        while (!isFinished.get()) {
            ++roundCtr;
            double reward = threadAssignment == null ?
                    root.sample(roundCtr, joinOrder, this.joinOp, policy) :
                    root.sample(roundCtr, joinOrder, this.joinOp, threadAssignment, policy);
            // Adaptively re-partition the search space
            if (roundCtr == 100 && tid == 0 && !isSmall) {
                List<List<Set<Integer>>> actionsAssignment = new ArrayList<>();
                for (int threadCtr = 0; threadCtr < nrThreads; threadCtr++) {
                    actionsAssignment.add(new ArrayList<>());
                }
                root.partitionSpace(0, nrThreads, actionsAssignment, root);
                for (int threadCtr = 1; threadCtr < nrThreads; threadCtr++) {
                    this.assignment[threadCtr].set(actionsAssignment.get(threadCtr));
                }
                threadAssignment = actionsAssignment.get(0);
                root = new NASPNode(roundCtr, query, root.useHeuristic, threadAssignment);
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
            if (root.nodeStatistics != null) {
                double progress = reward(joinOrder,
                        joinOp.cardinalities, joinOp.lastState.tupleIndices);
                root.nodeStatistics[tid].updateProgress(joinOrder[0], progress);
            }

            // Consider memory loss
            if (JoinConfig.FORGET && (roundCtr==nextForget && tid != 0)) {
                needForget = true;
                nextForget *= 10;
            }
            if (needForget && !isSmall) {
                threadAssignment = assignment[tid].get();
                if (threadAssignment != null) {
                    root = new NASPNode(roundCtr, query, root.useHeuristic, threadAssignment);
                    needForget = false;
                }
            }
        }
        long timer2 = System.currentTimeMillis();
        System.out.println("Thread " + tid + " " + (timer2 - timer1)
                + "\tRound: " + roundCtr + "\tOrder: " + Arrays.toString(joinOrder));
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
