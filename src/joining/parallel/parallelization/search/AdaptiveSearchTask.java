package joining.parallel.parallelization.search;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMaps;
import com.koloboke.collect.set.IntSet;
import com.koloboke.collect.set.hash.HashIntSet;
import com.koloboke.collect.set.hash.HashIntSets;
import config.ParallelConfig;
import joining.parallel.join.SPJoin;
import joining.parallel.uct.ASPNode;
import joining.parallel.uct.SPNode;
import joining.plan.HotSet;
import joining.result.ResultTuple;
import joining.uct.SelectionPolicy;
import logs.LogUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import preprocessing.Context;
import query.QueryInfo;
import statistics.QueryStats;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * The thread task for adptive search parallelization.
 * In the task, a thread will run learning samples
 * and collect results that satisfy with predicates.
 *
 * @author Ziyun Wei
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
    private final SPJoin spJoin;
    /**
     * All join operators.
     */
    private final List<SPJoin> joinOps;
    /**
     * Shared atomic flags among all threads.
     * It indicates whether the join finishes.
     */
    private final AtomicBoolean finish;

    public AdaptiveSearchTask(QueryInfo query,
                              ASPNode root,
                              SPJoin spJoin,
                              List<SPJoin> joinOps,
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

        int[] joinOrder = new int[query.nrJoined];
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
        int[] last = new int[1];
        while (!finish.get()) {
            ++roundCtr;
            double reward;
            last[0] = nrThreads - 1;
            List<Pair<Integer, Integer>> constraints = spJoin.constraints;
            int detect = spJoin.nextDetect;
//            reward = root.sample(roundCtr, joinOrder, spJoin, policy, tags, weights, 0, last);
//            spJoin.writeLog("Constraints: " + Arrays.toString(constraints.toArray()));
            reward = root.sample(roundCtr, joinOrder, spJoin, policy, constraints, detect);

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

            if (tid == nrThreads - 1 && roundCtr == nextDetect && nrConstraints > 0) {
                List<Pair<Integer, Integer>> newConstraints = new ArrayList<>();
                IntSet cycle = HashIntSets.newMutableSet();

                // hot predicates
//                Map<Pair<Integer, Integer>, Double> counts = new HashMap<>();
//                spJoin.constraintsStats.keySet().forEach(pair -> {
//                    double less = 0;
//                    double greater = 0;
//                    for (SPJoin joinOp: joinOps) {
//                        int[] stats = joinOp.constraintsStats.get(pair);
//                        less += joinOp.statsCount == 0 ? 0 : (stats[0] + 0.0) / joinOp.statsCount;
//                        greater += joinOp.statsCount == 0 ? 0 : (stats[1] + 0.0) / joinOp.statsCount;
//                    }
//                    counts.put(pair, Math.max(greater, less));
//                });
//
//
//                int size = counts.size();
//                List<Pair<Integer, Integer>> sortedConstraints = counts.keySet().stream().sorted(
//                        Comparator.comparing(counts::get)).collect(Collectors.toList());
//
//                for (int i = size - 1; i >= 0; i--) {
//                    Pair<Integer, Integer> pair = sortedConstraints.get(i);
//                    int left = pair.getLeft();
//                    int right = pair.getRight();
//                    if (!cycle.contains(left) || !cycle.contains(right)) {
//                        newConstraints.add(pair);
//                        cycle.add(left);
//                        cycle.add(right);
//                        if (newConstraints.size() == nrConstraints) {
//                            break;
//                        }
//                    }
//                }

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
//                IntIntMap count = HashIntIntMaps.newMutableMap(query.nrJoined);
                System.out.println("Hot Set: ");
//                Set<Pair<Integer, Integer>> queryConstraints = new HashSet<>(query.constraints);
                List<HotSet> topHostSet = new ArrayList<>();
                for(int i = 0; i < joinSize; i++) {
                    HotSet hotSet = sortedJoin.get(joinSize - 1 - i);
                    topHostSet.add(hotSet);
                    if (topHostSet.size() == nrConstraints) {
                        topHostSet.sort(Comparator.comparing(set -> set.nrJoinedTables));
                        Map<Integer, Integer> priority = new HashMap<>();
                        for (int hi = 0; hi < topHostSet.size(); hi++) {
                            HotSet set = topHostSet.get(hi);
                            System.out.println(set.toString());
//                            Pair<Integer, Integer> constraint = set.getConstraint(
//                                    cycle, queryConstraints, count, query.joinConnection);
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
//                nextDetect = nextDetect * 10;
                nextDetect = Integer.MAX_VALUE;
            }


//            if (roundCtr == 100000 && tid < 8) {
//                List<String>[] logs = new List[1];
//                for (int i = 0; i < 1; i++) {
//                    logs[i] = spJoin.logs;
//                }
//                LogUtils.writeLogs(logs, "verbose/adaptive_search/" + QueryStats.queryName);
//                System.out.println("Write to logs!");
//                System.exit(0);
//            }
        }
        // Materialize result table
        long timer2 = System.currentTimeMillis();
        System.out.println("Thread " + tid + " " + (timer2 - timer1)
                + "\tRound: " + roundCtr + "\tOrder: " + Arrays.toString(joinOrder));
        Collection<ResultTuple> tuples = spJoin.result.getTuples();
        return new SearchResult(tuples, spJoin.logs, tid);
    }
}
