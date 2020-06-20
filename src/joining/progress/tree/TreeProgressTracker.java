package joining.progress.tree;

import joining.plan.JoinOrder;
import joining.progress.hash.State;

/**
 * Keeps track of progress made in evaluating different
 * join orders. Offers methods to store progress made
 * and methods to retrieve the state from which evaluation
 * for one specific join order should continue. The main difference
 * over the old progress tracker is saving only three integers in
 * each node instead of increasing by the number of tables.
 *
 * @author Ziyun Wei
 */
public class TreeProgressTracker {
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
    final ProgressNode sharedProgress;
    /**
     * For each table the last tuple that was fully treated.
     */
    public final int[][] tableOffset;
    /**
     * Number of ways to split different tables.
     */
    private int nrSplitTables;
    /**
     * Indicates whether processing is finished.
     */
    public boolean isFinished = false;

    /**
     * Initializes progress tracking for given tables.
     *
     * @param nrTables      number of tables joined
     * @param cardinalities cardinality of each table
     * @param nrSplitTables number of tables to split
     */
    public TreeProgressTracker(int nrTables, int[] cardinalities, int nrSplitTables) {
        this.nrTables = nrTables;
        this.cardinalities = cardinalities;
        this.sharedProgress = new ProgressNode(nrTables);
        this.tableOffset = new int[nrSplitTables][nrTables];
        this.nrSplitTables = nrSplitTables;
    }

    /**
     * Integrates final state achieved when evaluating one specific
     * join order.
     *
     * @param joinOrder     a join order evaluated with a specific time budget
     * @param state         final state achieved when evaluating join order
     * @param splitTable    table to split
     * @param roundCtr      round number taken as the current time stamp
     */
    public void updateProgress(JoinOrder joinOrder, State state, int splitTable, int roundCtr) {
        // Update termination flag
        isFinished = state.isFinished();
        // Update state for all join order prefixes
        ProgressNode curPrefixProgress = sharedProgress;
        // Iterate over position in join order
        int nrJoinedTables = joinOrder.nrJoinedTables;
        // the time stamp we have seen along with the join path.
        int nodeTimeStamp = 0;
        for (int joinCtr = 0; joinCtr < nrJoinedTables; ++joinCtr) {
            int table = joinOrder.order[joinCtr];
            if (curPrefixProgress.childNodes[table] == null) {
                curPrefixProgress.childNodes[table] = new ProgressNode(nrTables);
            }
            curPrefixProgress = curPrefixProgress.childNodes[table];
            if (curPrefixProgress.latestState == null) {
                curPrefixProgress.latestState = new NodeState(nrSplitTables);
            }
            int latestTupleIndex = state.tupleIndices[table];
            nodeTimeStamp = curPrefixProgress.latestState.updateProgress(
                    nodeTimeStamp, splitTable, roundCtr, latestTupleIndex);
        }
        // Update table offset considering last fully treated tuple -
        // consider first table and all following tables in join order
        // if their cardinality is one.
        for (int joinCtr=0; joinCtr<nrJoinedTables; ++joinCtr) {
            int table = joinOrder.order[joinCtr];
            int lastTreated = state.tupleIndices[table]-1;
            tableOffset[splitTable][table] = Math.max(lastTreated, tableOffset[splitTable][table]);
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
     * @param joinOrder     a join order
     * @param splitTable    table to split
     * @return start state for evaluating join order
     */
    public State continueFrom(JoinOrder joinOrder, int splitTable) {
        int nrJoinedTables = joinOrder.nrJoinedTables;
        int[] order = joinOrder.order;
        State state = new State(nrTables);
        // the time stamp we have seen along with the join path.
        int nodeTimeStamp = 0;
        // Integrate progress from join orders with same prefix
        ProgressNode curPrefixProgress = sharedProgress;
        for (int joinCtr = 0; joinCtr < nrJoinedTables; ++joinCtr) {
            int table = order[joinCtr];
            curPrefixProgress = curPrefixProgress.childNodes[table];
            if (curPrefixProgress == null) {
                break;
            }
            nodeTimeStamp = curPrefixProgress.latestState.continueFrom(
                    state, nodeTimeStamp, splitTable, table);
            // the current progress is out-of-date
            if (nodeTimeStamp < 0) {
                break;
            }
        }
        return state;
    }
}
