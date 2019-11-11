package joining.progress;

import java.util.Arrays;

import joining.join.JoinMove;

/**
 * Captures processing state for a specific join order.
 * 
 * @author immanueltrummer
 *
 */
public class State {
	/**
	 * Last position index in join order
	 * over whose tuples we have iterated.
	 */
	public int lastIndex;
	/**
	 * Last move of join index.
	 */
	//public JoinMove lastMove;
	/**
	 * Last combination of tuple indices that
	 * we have seen.
	 */
	public int[] tupleIndices;
	/**
	 * Time version of the state.
	 */
	public int roundCtr;
	/**
	 * Initializes tuple indices to appropriate size.
	 * 
	 * @param nrTables	number of joined tables
	 */
	public State(int nrTables) {
		tupleIndices = new int[nrTables];
		//Arrays.fill(tupleIndices, -1);
		Arrays.fill(tupleIndices, 0);
		lastIndex = 0;
		//lastMove = JoinMove.RIGHT;
	}
	/**
	 * Checks whether processing is finished.
	 * 
	 * @return	true if result was generated
	 */
	public boolean isFinished() {
		return lastIndex < 0;
	}
	/**
	 * Returns true iff the other state is ahead in evaluation,
	 * considering only the shared join order prefix.
	 * 
	 * @param order			join order of tables
	 * @param otherState	processing state for join order with same prefix	
	 * @param prefixLength	length of shared prefix
	 * @return				true iff the other state is ahead
	 */
	boolean isAhead(int[] order, State otherState, int prefixLength) {
		// Determine up to which join order index we compare
		int nrCompared = prefixLength;
		//int nrCompared = Math.min(prefixLength, lastIndex);
		//nrCompared = Math.min(nrCompared, otherState.lastIndex);
		// Whichever state had higher progress in the first table
		// (in shared prefix in join order) wins.
		for (int joinCtr=0; joinCtr<nrCompared; ++joinCtr) {
			int table = order[joinCtr];
			if (tupleIndices[table] > otherState.tupleIndices[table]) {
				// this state is ahead
				return false;
			} else if (otherState.tupleIndices[table] > tupleIndices[table]) {
				// other state is ahead
				return true;
			}
		}
		// Equal progress in both states for shared prefix
		return false;
	}
	/**
	 * Considers another state achieved for a join order
	 * sharing the same prefix in the table ordering.
	 * Updates ("fast forwards") this state if the
	 * other state is ahead.
	 * 
	 * @param order			join order for which other state was achieved
	 * @param otherState	evaluation state achieved for join order sharing prefix
	 * @param prefixLength	length of prefix shared with other join order
	 */
	public void fastForward(int[] order, State otherState, int prefixLength) {
		// Fast forward is only executed if other state is more advanced
		if (isAhead(order, otherState, prefixLength)) {
			// Determine up to which join order index we compare
			//int nrUsed = Math.min(prefixLength, otherState.lastIndex);
			int nrUsed = prefixLength;
			// Adopt tuple indices from other state for common prefix -
			// set the remaining indices to zero.
			int nrTables = order.length;
			for (int joinCtr=0; joinCtr<nrTables; ++joinCtr) {
				int table = order[joinCtr];
				int otherIndex = otherState.tupleIndices[table];
				//int newIndex = joinCtr < nrUsed ? otherIndex : -1;
				int newIndex = joinCtr < nrUsed ? otherIndex : 0;
				tupleIndices[table] = newIndex;
			}
			// Start from last position that was changed
			lastIndex = nrUsed-1;
			// Current tuple is considered "fresh"
			//lastMove = JoinMove.DOWN;
		}
	}
	@Override
	public String toString() {
		return "Last index " + lastIndex + " on " + Arrays.toString(tupleIndices);
	}
}