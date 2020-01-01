package joining.parallel.parallelization.search;

import config.ParallelConfig;
import joining.parallel.join.SPJoin;
import joining.parallel.uct.SPNode;
import joining.result.ResultTuple;
import joining.uct.SelectionPolicy;
import preprocessing.Context;
import query.QueryInfo;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

/**
 * The thread task for search parallelization.
 * In the task, a thread will run learning samples
 * and collect results that satisfy with predicates.
 *
 * @author Ziyun Wei
 */
public class SearchTask implements Callable<SearchResult> {
    private final QueryInfo query;
    private final Context context;
    private final SPNode root;
    private final SPJoin spJoin;
    private final AtomicBoolean finish;
    private final SearchScheduler scheduler;

    public SearchTask(QueryInfo query, Context context,
                      SPNode root, SPJoin spJoin, SearchScheduler scheduler, AtomicBoolean finish) {
        this.query = query;
        this.context = context;
        this.root = root;
        this.spJoin = spJoin;
        this.scheduler = scheduler;
        this.finish = finish;
    }
    @Override
    public SearchResult call() throws Exception {
        long timer1 = System.currentTimeMillis();
        int tid = spJoin.tid;
        int nrThreads = ParallelConfig.EXE_THREADS;
        int[] joinOrder = new int[query.nrJoined];
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
        while (!finish.get()) {
            ++roundCtr;
            double reward;
            int adaptive = scheduler.adaptiveCtr;
            if (ParallelConfig.PARALLEL_SPEC == 3) {
                if (tid == nrThreads - 1 &&
                        roundCtr == Math.pow(100, adaptive)) {
                    scheduler.redistribution();
                }
            }
            if (scheduler.isChanged[tid]) {
                spJoin.initSearchRoot(scheduler.preNodes, scheduler.nodePerThread.get(tid), scheduler.treeLevel);
            }
            spJoin.sid = -1;
            reward = root.sample(roundCtr, joinOrder, spJoin, policy, true);
            scheduler.updateStatistics(spJoin.sid, reward);
            // Count reward except for final sample
            if (!spJoin.isFinished()) {
                accReward += reward;
            }
            // broadcasting the finished plan.
            else {
                if (!finish.get()) {
                    finish.set(true);
                    System.out.println("Finish id: " + tid + "\t" + Arrays.toString(joinOrder));
                }
                break;
            }
//            joinOp.writeLog("Episode Time: " + (end - start) + "\tReward: " + reward);
        }
        // Materialize result table
        long timer2 = System.currentTimeMillis();
        spJoin.roundCtr = roundCtr;
        System.out.println("Thread " + tid + " " + (timer2 - timer1) + "\t Round: " + roundCtr);
        Collection<ResultTuple> tuples = spJoin.result.getTuples();
        return new SearchResult(tuples, spJoin.logs, tid);
    }
}
