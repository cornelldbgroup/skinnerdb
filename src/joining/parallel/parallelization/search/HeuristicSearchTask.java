package joining.parallel.parallelization.search;


import com.koloboke.collect.set.IntSet;
import com.koloboke.collect.set.hash.HashIntSets;
import config.ParallelConfig;
import joining.parallel.join.SPJoin;
import joining.parallel.uct.ASPNode;
import joining.parallel.uct.HSPNode;
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
public class HeuristicSearchTask implements Callable<SearchResult> {
    /**
     * The query to process.
     */
    private final QueryInfo query;
    /**
     * The root of UCT tree.
     */
    private final HSPNode root;
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

    public HeuristicSearchTask(QueryInfo query,
                               HSPNode root,
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
        int nrJoined = query.nrJoined;
        int[] joinOrder = new int[query.nrJoined];
        long roundCtr = 0;
        // Get default action selection policy
        SelectionPolicy policy = SelectionPolicy.UCB1;
        // Initialize counter until scale down
        long nextScaleDown = 1;
        // Initialize counter until memory loss
        // Iterate until join result was generated
        double accReward = 0;
        boolean isFinished = false;
        int nrLevels = Math.min(nrJoined, Integer.toBinaryString(nrThreads).length() - 1);
        StringBuilder tidStr = new StringBuilder(Integer.toBinaryString(tid));
        while (tidStr.length() < nrLevels) {
            tidStr.insert(0, "0");
        }
        while (!isFinished) {
            ++roundCtr;
            double reward;
            reward = root.sample(roundCtr, joinOrder, spJoin, policy, true, nrLevels, tidStr.toString());
//            reward = spJoin.execute(new int[]{8, 2, 9, 1, 5, 7, 3, 6, 4, 0}, (int) roundCtr);

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
