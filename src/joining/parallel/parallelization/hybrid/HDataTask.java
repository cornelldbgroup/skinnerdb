package joining.parallel.parallelization.hybrid;

import config.JoinConfig;
import config.ParallelConfig;
import joining.parallel.join.ModJoin;
import joining.parallel.parallelization.search.SearchResult;
import joining.parallel.uct.NSPNode;
import joining.plan.JoinOrder;
import joining.result.ResultTuple;
import joining.uct.SelectionPolicy;
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
                int splitTable = getSplitTableByCard(joinOrder, joinOp.cardinalities);
                joinOp.execute(joinOrder, splitTable, (int) roundCtr);
                if (this.joinOp.isFinished()) {
                    boolean isFinished = joinPlan.setFinished(tid, splitTable);
                    if (isFinished) {
                        this.isFinished.set(true);
                        break;
                    }
                }
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
}
