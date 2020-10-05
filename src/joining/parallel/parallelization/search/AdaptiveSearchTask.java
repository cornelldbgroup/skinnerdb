package joining.parallel.parallelization.search;


import com.koloboke.collect.set.IntSet;
import com.koloboke.collect.set.hash.HashIntSets;
import config.ParallelConfig;
import joining.parallel.join.SPJoin;
import joining.parallel.join.SubJoin;
import joining.parallel.uct.ASPNode;
import joining.plan.HotSet;
import joining.result.ResultTuple;
import joining.uct.SelectionPolicy;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import query.QueryInfo;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * The thread task for adptive search parallelization.
 * In the task, a thread will run learning samples
 * and collect results that satisfy with predicates.
 *
 * @author Anonymous
 */
public class AdaptiveSearchTask implements Callable<SearchResult> {
    /**
     * The query to process.
     */
    private final QueryInfo query;
    /**
     * The root of UCT tree.
     */
    private final ASPNode root;
    /**
     * Multi-way join operator.
     */
    private final SubJoin spJoin;
    /**
     * All join operators.
     */
    private final List<SubJoin> joinOps;
    /**
     * Shared atomic flags among all threads.
     * It indicates whether the join finishes.
     */
    private final AtomicBoolean finish;

    public AdaptiveSearchTask(QueryInfo query,
                              ASPNode root,
                              SubJoin spJoin,
                              List<SubJoin> joinOps,
                              AtomicBoolean finish) {
        this.query = query;
        this.root = root;
        this.spJoin = spJoin;
        this.joinOps = joinOps;
        this.finish = finish;
    }
    @Override
    public SearchResult call() throws Exception {
        long timer1 = System.currentTimeMillis();
        // thread id
        int tid = spJoin.tid;
        int nrThreads = ParallelConfig.EXE_THREADS;
        int nrConstraints = nrThreads == 1 ? 0 : Integer.toBinaryString(nrThreads).length() - 1;
        int nrJoined = query.nrJoined;

        int[] joinOrder = new int[nrJoined];
        long roundCtr = 0;
        // Get default action selection policy
        SelectionPolicy policy = SelectionPolicy.UCB1;
        // Initialize counter until scale down
        long nextScaleDown = 1;
        // Initialize counter until memory loss
        int nextDetect = 100;
        spJoin.nextDetect = 0;
        // Iterate until join result was generated
        double accReward = 0;
        boolean isFinished = false;
        int[] lastOffsets = new int[nrJoined];
        int[] threads = new int[nrJoined];
        Arrays.fill(threads, nrThreads);
        while (!isFinished) {
            ++roundCtr;
            double reward;
            if (ParallelConfig.PARALLEL_SPEC == 8) {
                reward = root.sample(roundCtr, joinOrder, spJoin, policy);
            }
            else {
                List<Pair<Integer, Integer>> constraints = spJoin.constraints;
                int detect = spJoin.nextDetect;
                reward = root.sample(roundCtr, joinOrder, spJoin, policy, constraints, detect);
            }

            // Count reward except for final sample
            if (!spJoin.isFinished()) {
                accReward += reward;
            }
            // broadcasting the finished plan.
            else {
                if (finish.compareAndSet(false, true)) {
                    System.out.println("Finish id: " + tid + "\t" + Arrays.toString(joinOrder) + "\t" + roundCtr);
                    spJoin.roundCtr = roundCtr;
                }
                break;
            }

            if (ParallelConfig.PARALLEL_SPEC == 8) {
                if (tid == 0 && roundCtr == nextDetect) {
                    double max = -1;
                    int[] bestJoinOrder = new int[nrJoined];
                    root.bestJoinOrder(bestJoinOrder, nrThreads - 1);
                    System.out.println(Arrays.toString(bestJoinOrder));
                    double[] utilities = new double[nrJoined];
                    int[] newOffsets = new int[nrJoined];
                    for (int table = 0; table < nrJoined; table++) {
                        for (int i = 0; i < nrThreads; i++) {
                            newOffsets[table] = Math.max(newOffsets[table], spJoin.tracker.tableOffsetMaps[i][0][table]);
                        }
                    }

//                for (int table = 0; table < nrJoined; table++) {
//                    utilities[table] = (newOffsets[table] - lastOffsets[table] + 0.0) * spJoin.visits[table] / threads[table];
//                }

                    int sum = 0;
                    // first level
                    Arrays.fill(utilities, 1);
//                    if (root.recommendedActions.size() < nrThreads - 1) {
//                        for (int i = 0; i < nrJoined; i++) {
//                            int table = bestJoinOrder[i];
//                            int tableThreads = Math.min(root.nodesInLevel(table), nrThreads - 1 - sum);
//                            utilities[table] += tableThreads;
//                            sum += tableThreads;
//                            if (sum >= nrThreads - 1) {
//                                break;
//                            }
//                        }
//                    }


                    root.partitionSpaceUtility(new int[]{0, nrThreads-1}, utilities, threads, bestJoinOrder);
                    nextDetect = nextDetect * 10;
                    lastOffsets = newOffsets;
                }
            }

            else {
                // CAPS
                if (tid == nrThreads - 1 && roundCtr == nextDetect && nrConstraints > 0) {
                    List<Pair<Integer, Integer>> newConstraints = new ArrayList<>();
                    IntSet cycle = HashIntSets.newMutableSet();

                    // hot set
                    Map<HotSet, Double> setCounts = new HashMap<>();
                    for (SPJoin joinOp: joinOps) {
                        for (Map.Entry<HotSet, Integer> entry: joinOp.joinStats.entrySet()) {
                            double value = joinOp.statsCount == 0 ? 0 : (entry.getValue() + 0.0) / joinOp.statsCount;
                            setCounts.merge(entry.getKey(), value, Double::sum);
                        }
                    }

                    int joinSize = setCounts.size();
                    List<HotSet> sortedJoin = setCounts.keySet().stream().sorted(
                            Comparator.comparing(setCounts::get)).collect(Collectors.toList());
                    List<HotSet> topHostSet = new ArrayList<>();

                    if (ParallelConfig.CONSTRAINT_PER_THREAD) {
                        List<List<Pair<Integer, Integer>>> threadsConstraints = ASPNode.getNodeConstraints(nrThreads, root);
                        for (int i = 0; i < nrThreads; i++) {
                            SPJoin joinOp = joinOps.get(i);
                            joinOp.constraints = threadsConstraints.get(i);
                            joinOp.nextDetect = nextDetect;
                        }
                        nextDetect = nextDetect * 10;
//                    nextDetect = Integer.MAX_VALUE;
                    }
                    else {
                        System.out.println("Hot Set: ");
                        for(int i = 0; i < joinSize; i++) {
                            HotSet hotSet = sortedJoin.get(joinSize - 1 - i);
                            topHostSet.add(hotSet);
                            if (topHostSet.size() == nrConstraints) {
                                topHostSet.sort(Comparator.comparing(set -> set.nrJoinedTables));
                                Map<Integer, Integer> priority = new HashMap<>();
                                for (int hi = 0; hi < topHostSet.size(); hi++) {
                                    HotSet set = topHostSet.get(hi);
                                    System.out.println(set.toString());
                                    IntSet next = hi == topHostSet.size() - 1 ?
                                            HashIntSets.newMutableSet() : topHostSet.get(hi+1).hotSet;
                                    Pair<Integer, Integer> constraint = set.getConstraint(
                                            cycle, query, next, priority);
                                    if (constraint != null) {
                                        newConstraints.add(constraint);
                                        System.out.println("New Constraints: " + constraint.toString());
                                        if (newConstraints.size() == nrConstraints) {
                                            break;
                                        }
                                    }

                                }
                                topHostSet.clear();
                            }
                            if (newConstraints.size() == nrConstraints || cycle.size() == query.nrJoined) {
                                break;
                            }
                        }

                        // broadcast constraints to all of threads.
                        for (SPJoin joinOp: joinOps) {
                            int spID = joinOp.tid;
                            StringBuilder binary = new StringBuilder(Integer.toBinaryString(spID));
                            List<Pair<Integer, Integer>> threadConstraints = new ArrayList<>();
                            while (binary.length() < nrConstraints) {
                                binary.insert(0, "0");
                            }
                            for (int i = 0; i < newConstraints.size(); i++) {
                                char tag = binary.charAt(i);
                                Pair<Integer, Integer> originalConstraint = newConstraints.get(i);
                                Pair<Integer, Integer> constraint = tag == '0' ? originalConstraint :
                                        new ImmutablePair<>(originalConstraint.getRight(), originalConstraint.getLeft());
                                threadConstraints.add(constraint);
                            }
                            joinOp.constraints = threadConstraints;
                            joinOp.nextDetect = nextDetect;
                        }
                        nextDetect = nextDetect * 10;
//                    nextDetect = Integer.MAX_VALUE;
                    }
                }
            }
            isFinished = finish.get();

        }
        // Materialize result table
        long timer2 = System.currentTimeMillis();
        System.out.println("Thread " + tid + " " + (timer2 - timer1)
                + "\tRound: " + roundCtr + "\tOrder: " + Arrays.toString(joinOrder));
        Collection<ResultTuple> tuples = spJoin.result.getTuples();
        return new SearchResult(tuples, spJoin.logs, tid);
    }
}
