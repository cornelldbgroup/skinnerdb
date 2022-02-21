package joining.parallel.parallelization;

import joining.parallel.join.DPJoin;
import joining.parallel.uct.DPNode;
import joining.progress.State;

import java.util.concurrent.atomic.AtomicReference;

/**
 * The plan information of the first terminated thread.
 *
 * @author Anonymous
 */
public class NewEndPlan {
    /**
     * Finished join order.
     */
    public final int[] joinOrder;
    /**
     * Finished split table.
     */
    private volatile int splitTable;
    /**
     * finish flags for different tables
     */
    public final boolean[][] finishFlags;
    /**
     * Array of the slowest states for split tables.
     */
    public final AtomicReference<State>[] slowestStates;
    /**
     * Root of UCT tree。
     */
    public volatile DPNode root;
    /**
     * Initialize the final plan shared by different threads.
     *
     * @param nrThreads     Number of threads
     * @param nrTables      Number of tables
     */
    public NewEndPlan(int nrThreads, int nrTables, DPNode root) {
        joinOrder = new int[nrTables];
        finishFlags = new boolean[nrThreads][nrTables];
        splitTable = -1;
        this.slowestStates = new AtomicReference[nrTables];
        for (int tableCtr = 0; tableCtr < nrTables; tableCtr++) {
            State firstState = new State(nrTables);
            firstState.tid = -1;
            slowestStates[tableCtr] = new AtomicReference<>(firstState);
        }
        this.root = root;
    }
    /**
     * Set the final join order
     *
     * @param joinOrder     optimal join order
     */
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

    public State setSplitTable(int optimalTable, State newState, int splitTable) {
        int nrTables = joinOrder.length;
        return slowestStates[splitTable].updateAndGet(previousState -> {
            this.splitTable = optimalTable;
            boolean isAhead = previousState.isAhead(joinOrder, newState, nrTables);
            return isAhead ? newState : previousState;
        });
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
//        for (int i = 0; i < threadSlowStates.length; i++) {
//            State threadState = threadSlowStates[i][largeTable];
//            if (threadState == null) {
//                state = new State(joinOrder.length);
//                state.tid = i;
//                return state;
//            }
//            else if (state == null || (threadState.lastIndex >= 0
//                            && threadState.isAhead(joinOrder, state, nrTables))) {
//                state = threadState;
//                state.tid = i;
//            }
//        }
        return state;
    }
}
