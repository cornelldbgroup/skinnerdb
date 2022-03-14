package joining.parallel.parallelization.search;

import joining.parallel.join.ModJoin;
import joining.parallel.join.OldJoin;
import joining.parallel.parallelization.hybrid.JoinPlan;
import joining.progress.State;
import joining.result.ResultTuple;
import preprocessing.Context;
import query.QueryInfo;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class SPWorkTask implements Callable<SearchResult> {
    /**
     * The query to process.
     */
    private final QueryInfo query;
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
     * Thread identification
     */
    private final int tid;
    /**
     * Thread identification
     */
    private final int nrThreads;
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
    public SPWorkTask(QueryInfo query, Context context,
                      OldJoin joinOp, int tid, int nrThreads,
                      AtomicBoolean isFinished, AtomicReference<JoinPlan> nextJoinOrder) {
        this.query = query;
        this.nrThreads = nrThreads;
        this.joinOp = joinOp;
        this.isFinished = isFinished;
        this.tid = tid;
        this.nextJoinOrder = nextJoinOrder;
    }

    @Override
    public SearchResult call() throws Exception {
        long timer1 = System.currentTimeMillis();
        int[] joinOrder = null;
        long roundCtr = 0;
        int nrJoined = query.nrJoined;
        while (!isFinished.get()) {
            JoinPlan joinPlan = nextJoinOrder.get();
            if (joinPlan != null) {
                ++roundCtr;
                joinOrder = joinPlan.joinOrder;
                joinOp.executeWorker(joinOrder, (int) roundCtr, joinPlan.plan);
                if (joinOp.lastState.isFinished()) {
                    isFinished.set(true);
                }
//                if (roundCtr == 10000) {
//                    isFinished.set(true);
//                    long timer2 = System.currentTimeMillis();
//                    joinOp.roundCtr = roundCtr;
//                    System.out.println("Thread " + tid + ": " + (timer2 - timer1) + "\t Round: " + roundCtr);
//                    Collection<ResultTuple> tuples = joinOp.result.getTuples();
//                    SearchResult searchResult = new SearchResult(tuples, joinOp.logs, tid);
//                    searchResult.isSearch = false;
//                    return searchResult;
//                }
            }
        }
        long timer2 = System.currentTimeMillis();
        System.out.println("Work thread " + tid + " " + (timer2 - timer1)
                + "\tRound: " + roundCtr + "\tOrder: " + Arrays.toString(joinOrder));
        Collection<ResultTuple> tuples = joinOp.result.getTuples();
        SearchResult searchResult = new SearchResult(tuples, joinOp.logs, tid);
        searchResult.isSearch = false;
        return searchResult;
    }
}
