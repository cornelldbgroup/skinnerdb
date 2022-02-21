package joining.parallel.parallelization;

import config.ParallelConfig;
import joining.parallel.join.DPJoin;
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
 * @author Anonymous
 */
public class EndPlan {
    /**
     * Finished join order.
     */
    private final int[] joinOrder;
    /**
     * Finished split table.
     */
    private volatile int splitTable;
    /**
     * finish flags for different tables
     */
    public final boolean[][] finishFlags;
    /**
     * Array of the slowest state for different thread
     */
    public final AtomicReference<State> slowestState;
    /**
     * Root of UCT tree。
     */
    public volatile DPNode root;

    public volatile State[][] threadSlowStates;

    public EndPlan(int nrThreads, int nrTables, DPNode root) {
        joinOrder = new int[nrTables];
        finishFlags = new boolean[nrThreads][nrTables];
        splitTable = -1;
        State firstState = new State(nrTables);
        firstState.tid = -1;
        this.slowestState = new AtomicReference<>(firstState);
        this.root = root;
        this.threadSlowStates = new State[nrThreads][nrTables];
    }

    public int[] getJoinOrder() {
        return joinOrder;
    }

    public void setJoinOrder(int[] joinOrder) {
        System.arraycopy(joinOrder, 0,
                this.joinOrder, 0, joinOrder.length);
    }

    public int getSplitTable() {
        return splitTable;
    }

    public void setSplitTable(int splitTable) {
        this.splitTable = splitTable;
    }

    public State setSplitTable(int optimalTable, State state) {
        int nrTables = joinOrder.length;
        return slowestState.updateAndGet(previousState -> {
            this.splitTable = optimalTable;
            boolean isAhead = previousState.isAhead(joinOrder, state, nrTables);
            return isAhead ? state : previousState;
        });
    }

    public boolean isSlow(State state, int tid, int splitTable, DPJoin dpJoin) {
        threadSlowStates[tid][splitTable] = state;
        int nrTables = joinOrder.length;
        boolean isSlow = true;
        for (int threadCtr = 0; threadCtr < threadSlowStates.length; threadCtr++) {
            if (tid != threadCtr) {
                State threadState = threadSlowStates[threadCtr][splitTable];
                if (threadState == null
                        || (threadState.lastIndex >= 0
                        && threadState.isAhead(joinOrder, state, nrTables))) {
                    isSlow = false;
                    break;
                }
            }
        }
        return isSlow;
    }

    public boolean setFinished(int tid, int splitTable) {
        finishFlags[tid][splitTable] = true;
        for (int thread = 0; thread < finishFlags.length; thread++) {
            if (!finishFlags[thread][splitTable]) {
                return false;
            }
        }

        return true;
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
            else if (state == null || (threadState.lastIndex >= 0
                            && threadState.isAhead(joinOrder, state, nrTables))) {
                state = threadState;
                state.tid = i;
            }
        }
        return state;
    }
}
