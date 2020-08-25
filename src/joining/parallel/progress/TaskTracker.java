package joining.parallel.progress;


import joining.parallel.join.ParaJoin;
import joining.parallel.parallelization.join.JoinMicroTask;
import joining.plan.JoinOrder;

public class TaskTracker {
    /**
     * Number of tables in the query.
     */
    private final int nrTables;
    /**
     * Stores task made for each join order prefix.
     */
    private final TaskNode sharedTask;
    /**
     * For each table the last tuple that was fully treated.
     */
    public final int[] tableOffset;
    /**
     * Cardinality of each query table.
     */
    final int[] cardinalities;
    /**
     * Initializes pointers to child nodes.
     *
     * @param nrTables number of tables in the database
     */
    public TaskTracker(int nrTables, int[] cardinalities) {
        this.nrTables = nrTables;
        this.cardinalities = cardinalities;
        sharedTask = new TaskNode(nrTables);
        tableOffset = new int[nrTables];
    }

    /**
     * Integrates final state achieved when evaluating one specific
     * join order.
     *
     * @param joinOrder a join order evaluated with a specific time budget
     * @param state     final state achieved when evaluating join order
     */
    public void updateProgress(JoinOrder joinOrder, TaskState state) {
        // Update state for all join order prefixes
        TaskNode curPrefixProgress = sharedTask;
        // Iterate over position in join order
        int nrJoinedTables = joinOrder.nrJoinedTables;
        for (int joinCtr = 0; joinCtr < nrJoinedTables; ++joinCtr) {
            int table = joinOrder.order[joinCtr];
            if (curPrefixProgress.childNodes[table] == null) {
                curPrefixProgress.childNodes[table] = new TaskNode(nrTables);
            }
            curPrefixProgress = curPrefixProgress.childNodes[table];
            if (curPrefixProgress.latestState == null) {
                curPrefixProgress.latestState = new TaskState(nrTables);
                curPrefixProgress.latestState.lastIndex = joinCtr;
            }
            state.lastIndex = joinCtr;
            curPrefixProgress.latestState.fastForward(
                    joinOrder.order, state, joinCtr + 1);
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
            if (cardinality>1) {
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
    public TaskState continueFrom(JoinOrder joinOrder, ParaJoin paraJoin) {
        int nrJoinedTables = joinOrder.nrJoinedTables;
        int[] order = joinOrder.order;
        TaskState state = new TaskState(nrTables);
        // Integrate progress from join orders with same prefix
        TaskNode curPrefixProgress = sharedTask;
        boolean clear = false;
        for (int joinCtr = 0; joinCtr < nrJoinedTables; ++joinCtr) {
            int table = order[joinCtr];
            curPrefixProgress = curPrefixProgress.childNodes[table];
            if (curPrefixProgress == null) {
                state.tasks[table].add(new JoinMicroTask(state.tupleIndices.clone(),
                        joinCtr, paraJoin, -1));
                break;
            }
            state.fastForward(order, curPrefixProgress.latestState, joinCtr + 1);
            // Is outdated? -> filter outdated tasks
            if (clear) {
                state.tasks[table].clear();

            }
            if (!clear && curPrefixProgress.latestState.isAhead(order, state, joinCtr + 1)) {
                clear = true;
                state.tasks[table].clear();
                state.tasks[table].add(new JoinMicroTask(state.tupleIndices.clone(),
                        joinCtr, paraJoin, -1));
            }
        }
        return state;
    }
}
