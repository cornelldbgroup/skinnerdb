package joining.parallel.parallelization;

import config.ParallelConfig;
import joining.parallel.uct.BaseUctInner;
import joining.parallel.uct.DPNode;
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
    /**
     * Root of
     */
    public volatile DPNode root;

    public volatile State[][] threadSlowStates;

    public EndPlan(int nrThreads, int nrTables, DPNode root) {
        joinOrder = new int[nrTables];
        finishFlags = new boolean[nrThreads][nrTables];
        splitTable = -1;
        this.slowThreads = new int[nrTables];
        Arrays.fill(slowThreads, -1);
        this.slowestState = new AtomicReference<>(new State(nrTables));
        this.root = root;
        this.threadSlowStates = new State[nrThreads][nrTables];
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

    public boolean isSlow(State state, int tid, int splitTable) {
        threadSlowStates[tid][splitTable] = state;
        int nrTables = joinOrder.length;
        boolean isSlow = true;
        for (int i = 0; i < threadSlowStates.length; i++) {
            if (tid != i) {
                State threadState = threadSlowStates[i][splitTable];
                if (threadState == null ||
                        (threadState.lastIndex >= 0 && threadState.isAhead(joinOrder, state, nrTables))) {
                    isSlow = false;
                    break;
                }
            }
        }
        return isSlow;
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

    public State getSlowState(int largeTable) {
        int nrTables = joinOrder.length;
        State state = null;
        for (int i = 0; i < threadSlowStates.length; i++) {
            State threadState = threadSlowStates[i][largeTable];
            if (threadState == null) {
                state = new State(joinOrder.length);
                state.tid = i;
                return state;
            }
            else if (state == null ||
                    (threadState.lastIndex >=0 && threadState.isAhead(joinOrder, state, nrTables))) {
                state = threadState;
            }
        }
        return state;
    }
}
