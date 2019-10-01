package joining.progress;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ThreadState {
    /**
     * the epoch of progress
     */
    public int threadID;
    /**
     * Last index progress and time version for each split table
     * we have seen.
     */
    public Map<Integer, int[]> tableTupleIndexEpoch;

    /**
     * Initializes tuple indices using specified start state.
     * @param tupleIndex	indices of start state
     */
    public ThreadState(int tupleIndex, int splitKey, int roundCtr, int joinIndex, int threadID) {
        tableTupleIndexEpoch = new HashMap<>();
        tableTupleIndexEpoch.put(splitKey, new int[]{tupleIndex, roundCtr, joinIndex});
        this.threadID = threadID;
    }

    public void updateProgress(State finalState, int newIndex, int key, int roundCtr, int joinIndex) {
        if (tableTupleIndexEpoch.containsKey(key)) {
            int[] tupleState = tableTupleIndexEpoch.get(key);
            if (tupleState[0] != newIndex || finalState.roundCtr > tupleState[1]) {
                tupleState[0] = newIndex;
                tupleState[1] = roundCtr;
                tupleState[2] = joinIndex;
            }
            finalState.roundCtr = tupleState[1];
        }
        else {
            tableTupleIndexEpoch.put(key, new int[]{newIndex, roundCtr, joinIndex});
            finalState.roundCtr = roundCtr;
        }
    }

    public void fastForward(State finalState, int table, int splitKey, int index) {
        // Fast forward is only executed if other state is more advanced
        // Adopt tuple indices from other state for common prefix -
        // set the remaining indices to start index.
        int[] tupleState = tableTupleIndexEpoch.get(splitKey);
        if (tupleState != null) {
            int tupleIndex = tupleState[0];
            int roundCtr = tupleState[1];
            int joinIndex = tupleState[2];
            if (finalState.roundCtr <= roundCtr) {
                finalState.tupleIndices[table] = tupleIndex;
                finalState.roundCtr = roundCtr;
                finalState.lastIndex = Math.min(index, joinIndex);
            }
            else {
                finalState.roundCtr = -1;
            }
        }
        else {
            finalState.roundCtr = -1;
        }
    }
    // TODO
    @Override
    public String toString() {
        List<String> keys = new ArrayList<>();
        return "Last index on Table: " + String.join(", ", keys);
    }

    public boolean hasProgress(int splitKey) {
        return tableTupleIndexEpoch.get(splitKey) != null;
    }

    public int getProgress(int splitKey) {
        int[] state = tableTupleIndexEpoch.get(splitKey);
        return state == null ? -1 : state[0];
    }
}
