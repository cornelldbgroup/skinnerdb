package joining.progress;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import joining.plan.JoinOrder;
import query.QueryInfo;

/**
 * Keeps track of progress made in evaluating different
 * join orders. Offers methods to store progress made
 * and methods to retrieve the state from which evaluation
 * for one specific join order should continue.
 *
 * @author immanueltrummer
 */
public class ProgressTracker {
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
    final Progress sharedProgress;
    /**
     * For each table the last tuple that was fully treated.
     */
    public final int[] tableOffset;
    /**
     * Maps join orders to the last state achieved during evaluation.
     */
    Map<JoinOrder, State> orderToState = new HashMap<JoinOrder, State>();
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
    public ProgressTracker(int nrTables, int[] cardinalities) {
        this.nrTables = nrTables;
        this.cardinalities = cardinalities;
        sharedProgress = new Progress(nrTables);
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
        // Update state for specific join order
        orderToState.put(joinOrder, state);
        // Update state for all join order prefixes
        Progress curPrefixProgress = sharedProgress;
        // Iterate over position in join order
        int nrJoinedTables = joinOrder.nrJoinedTables;
        for (int joinCtr = 0; joinCtr < nrJoinedTables; ++joinCtr) {
            int table = joinOrder.order[joinCtr];
            if (curPrefixProgress.childNodes[table] == null) {
                curPrefixProgress.childNodes[table] = new Progress(nrTables);
            }
            curPrefixProgress = curPrefixProgress.childNodes[table];
            if (curPrefixProgress.latestState == null) {
                curPrefixProgress.latestState = new State(nrTables);
            }
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
        /*
        int firstTable = joinOrder.order[0];
        int lastTreatedTuple = state.tupleIndices[firstTable] - 1;
        tableOffset[firstTable] = Math.max(lastTreatedTuple, 
        		tableOffset[firstTable]);
        */
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
        State state = orderToState.get(joinOrder);
        if (state == null) {
            state = new State(nrTables);
        }
        // Integrate progress from join orders with same prefix
        Progress curPrefixProgress = sharedProgress;
        for (int joinCtr = 0; joinCtr < nrJoinedTables; ++joinCtr) {
            int table = order[joinCtr];
            curPrefixProgress = curPrefixProgress.childNodes[table];
            if (curPrefixProgress == null) {
                break;
            }
            state.fastForward(order, curPrefixProgress.latestState, joinCtr + 1);
        }
        // Integrate table offset
        /*
		int firstTable = order[0];
		int offset = Math.max(state.tupleIndices[firstTable], tableOffset[firstTable]);
		state.tupleIndices[firstTable] = offset;
		*/
		/*
		int firstChange = nrTables + 1;
		for (int joinCtr=0; joinCtr<nrTables; ++joinCtr) {
			int table = order[joinCtr];
			int offset = Math.max(state.tupleIndices[table], tableOffset[table]);
			if (offset > state.tupleIndices[table]) {
				firstChange = Math.min(firstChange, joinCtr);
			}
			state.tupleIndices[table] = offset;
		}
		state.lastIndex = 0;
		*/
        //state.lastIndex = Math.min(state.lastIndex, firstChange);
        return state;
    }
}