package joining.parallel.parallelization.hybrid;


import config.LoggingConfig;
import joining.parallel.join.ModJoin;
import joining.parallel.parallelization.search.SearchResult;
import joining.progress.State;
import joining.result.ResultTuple;
import preprocessing.Context;
import query.QueryInfo;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class HDataTask implements Callable<SearchResult> {
    /**
     * The query to process.
     */
    private final QueryInfo query;
    /**
     * Search parallel operator.
     */
    private final ModJoin joinOp;
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
    public HDataTask(QueryInfo query, Context context,
                     ModJoin joinOp, int tid, int nrThreads,
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
        while (!isFinished.get()) {
            JoinPlan joinPlan = nextJoinOrder.get();
            if (joinPlan != null) {
                ++roundCtr;
                joinOrder = joinPlan.joinOrder;
                int splitTable = joinPlan.splitTable;
                State slowestState = joinPlan.slowestState;
                joinOp.execute(joinOrder, splitTable, (int) roundCtr, slowestState, joinPlan.plan);
                int largeTable = joinOp.largeTable;
                boolean threadFinished = this.joinOp.isFinished();
                double progress = threadFinished ? Double.MAX_VALUE : (joinOp.progress + largeTable);
                joinPlan.progress[splitTable * nrThreads + tid].set(progress);
                joinPlan.states[splitTable * nrThreads + tid].set(joinOp.lastState);
                if (threadFinished) {
                    roundCtr--;
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
        System.out.println("Data thread " + tid + " " + (timer2 - timer1)
                + "\tRound: " + roundCtr + "\tOrder: " + Arrays.toString(joinOrder));
        Collection<ResultTuple> tuples = joinOp.result.getTuples();
        SearchResult searchResult = new SearchResult(tuples, joinOp.logs, tid);
        searchResult.isSearch = false;
        return searchResult;
    }
}
