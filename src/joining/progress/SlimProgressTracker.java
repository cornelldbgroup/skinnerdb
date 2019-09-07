package joining.progress;

import joining.plan.JoinOrder;
import statistics.JoinStats;

import java.util.Arrays;

/**
 * Keeps track of progress made in evaluating different
 * join orders. Offers methods to store progress made
 * and methods to retrieve the state from which evaluation
 * for one specific join order should continue.
 * In slim version, we only store one progress in a state node,
 * representing the progress of according table.
 *
 * @author Ziyun Wei
 */
public class SlimProgressTracker {
    /**
     * Number of tables in the query.
     */
    final int nrTables;
    /**
     * Cardinality of each query table.
     */
    final int[] cardinalities;
    /**
     * Stores progress made for each join order prefix.
     */
    final SlimProgress sharedProgress;
    /**
     * For each table the last tuple that was fully treated.
     */
    public final int[] tableOffset;
    /**
     * Indicates whether processing is finished.
     */
    public boolean isFinished = false;
    /**
     * Initializes progress tracking for given tables.
     *
     * @param nrTables 		number of tables joined
     * @param cardinalities	cardinality of each table
     */
    public SlimProgressTracker(int nrTables, int[] cardinalities) {
        this.nrTables = nrTables;
        this.cardinalities = cardinalities;
        sharedProgress = new SlimProgress(nrTables);
        tableOffset = new int[nrTables];
        //Arrays.fill(tableOffset, -1);
        Arrays.fill(tableOffset, 0);
    }
    /**
     * Integrates final state achieved when evaluating one specific
     * join order.
     *
     * @param joinOrder a join order evaluated with a specific time budget
     * @param state     final state achieved when evaluating join order
     */
    public void updateProgress(JoinOrder joinOrder, State state) {
        // Update termination flag
        isFinished = state.isFinished();
        // Update state for all join order prefixes
        SlimProgress curPrefixProgress = sharedProgress;
        // Iterate over position in join order
        int nrJoinedTables = joinOrder.nrJoinedTables;
        for (int joinCtr = 0; joinCtr < nrJoinedTables; ++joinCtr) {
            int table = joinOrder.order[joinCtr];
            if (curPrefixProgress.childNodes[table] == null) {
                curPrefixProgress.childNodes[table] = new SlimProgress(nrTables);
            }
            curPrefixProgress = curPrefixProgress.childNodes[table];
            if (curPrefixProgress.latestState == null) {
                curPrefixProgress.latestState = new SlimState();
            }
            int newProgress = state.tupleIndices[table];
            long episode = JoinStats.roundCtr;
            curPrefixProgress.latestState.updateProgress(state, newProgress, episode);
        }
        // Update table offset considering last fully treated tuple -
        // consider first table and all following tables in join order
        // if their cardinality is one.
        for (int joinCtr=0; joinCtr<nrJoinedTables; ++joinCtr) {
            int table = joinOrder.order[joinCtr];
            int lastTreated = state.tupleIndices[table]-1;
            tableOffset[table] = Math.max(lastTreated, tableOffset[table]);
            // Stop after first table with cardinality >1
            int cardinality = cardinalities[table];
            if (cardinality > 1) {
                break;
            }
        }
    }
    /**
     * Returns state from which evaluation of the given join order
     * must start in order to guarantee that all results are generated.
     *
     * @param joinOrder a join order
     * @return start state for evaluating join order
     */
    public State continueFrom(JoinOrder joinOrder) {
        int nrJoinedTables = joinOrder.nrJoinedTables;
        int[] order = joinOrder.order;
        State state = new State(nrTables);
        // Integrate progress from join orders with same prefix
        SlimProgress curPrefixProgress = sharedProgress;
        for (int joinCtr = 0; joinCtr < nrJoinedTables; ++joinCtr) {
            int table = order[joinCtr];
            curPrefixProgress = curPrefixProgress.childNodes[table];
            if (curPrefixProgress == null) {
                break;
            }
            if (!curPrefixProgress.latestState.restoreState(state, table)) {
                break;
            }
        }
        return state;
    }
}
