package joining.parallel.parallelization;

import config.ParallelConfig;
import joining.parallel.uct.BaseUctInner;
import joining.parallel.uct.SimpleUctNode;
import joining.progress.State;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.UnaryOperator;

/**
 * The plan information of the first terminated thread.
 *
 * @author Ziyun Wei
 */
public class EndPlan {
    /**
     * Finished join order.
     */
    private volatile int[] joinOrder;
    /**
     * Finished split table.
     */
    private volatile int splitTable;
    /**
     * finish flags for different tables
     */
    public final boolean[][] finishFlags;
    /**
     * Array of slowest state for different thread
     */
    public final AtomicReference<State> slowestState;
    /**
     * the slowest thread id for each split table
     */
    public int[] slowThreads;

    public final ReentrantLock lock;

    public EndPlan(int nrThreads, int nrTables, int[] cardinalities) {
        joinOrder = new int[nrTables];
        finishFlags = new boolean[nrThreads][nrTables];
        splitTable = -1;
        this.slowThreads = new int[nrTables];
        Arrays.fill(slowThreads, -1);
        this.slowestState = new AtomicReference<>(new State(nrTables));
        this.lock = new ReentrantLock();
    }

    public int[] getJoinOrder() {
        return joinOrder;
    }

    public void setJoinOrder(int[] joinOrder) {
        System.arraycopy(joinOrder, 0, this.joinOrder, 0, joinOrder.length);
    }

    public int getSplitTable() {
        return splitTable;
    }

    public void setSplitTable(int splitTable) {
        this.splitTable = splitTable;
    }

    public State setSplitTable(int largeTable, State state) {
        int nrTables = joinOrder.length;
        return slowestState.updateAndGet(previousState -> {
            if (previousState.isAhead(joinOrder, state, nrTables)) {
                this.splitTable = largeTable;
                return state;
            }
            else {
                return previousState;
            }
        });
    }

    public boolean setFinished(int tid, int splitTable) {
        finishFlags[tid][splitTable] = true;
        int number = 0;
        int slowID = 0;
        int firstFinish = -1;
        for (int i = 0; i < finishFlags.length && number <= 1; i++) {
            if (!finishFlags[i][splitTable]) {
                number++;
                slowID = i;
            }
            else if (firstFinish < 0){
                firstFinish = i;
            }
        }

        if (number == 1 && tid == firstFinish) {
            slowThreads[splitTable] = slowID;
        }

        return number == 0;
    }
}
