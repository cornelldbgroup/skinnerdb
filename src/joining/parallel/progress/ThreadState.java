package joining.parallel.progress;

import joining.progress.State;

import java.util.ArrayList;
import java.util.List;

/**
 * The state of progress saved for
 * each working thread individually.
 *
 * @author Ziyun Wei
 */
public class ThreadState {
    /**
     * the epoch of progress
     */
    int threadID;
    /**
     * Last index progress and time version for each split table
     * we have seen.
     */
    int[][] tableTupleIndexEpoch;

    /**
     * Initializes tuple indices using specified start state.
     * @param tupleIndex	indices of start state
     */
    public ThreadState(int tupleIndex, int splitKey, int roundCtr, int joinIndex, int threadID, int nrSplits) {
        tableTupleIndexEpoch = new int[nrSplits][3];
        tableTupleIndexEpoch[splitKey][0] = tupleIndex;
        tableTupleIndexEpoch[splitKey][1] = roundCtr;
        tableTupleIndexEpoch[splitKey][2] = joinIndex;
        this.threadID = threadID;
    }

    /**
     * After sampling, update progress in each node.
     *
     * @param finalState    The final state that has the latest round count.
     * @param newIndex      New progress.
     * @param splitKey      Split strategy.
     * @param roundCtr      The count number of sampling.
     * @param joinIndex     The join position of table.
     */
    void updateProgress(State finalState, int newIndex, int splitKey, int roundCtr, int joinIndex) {
        int round = tableTupleIndexEpoch[splitKey][1];
        if (round > 0) {
            if (tableTupleIndexEpoch[splitKey][0] != newIndex) {
                tableTupleIndexEpoch[splitKey][0] = newIndex;
                tableTupleIndexEpoch[splitKey][1] = roundCtr;
                tableTupleIndexEpoch[splitKey][2] = joinIndex;
                finalState.roundCtr = roundCtr;
            }
            else if (finalState.roundCtr > round) {
                tableTupleIndexEpoch[splitKey][1] = roundCtr;
                finalState.roundCtr = roundCtr;
            }
            else {
                finalState.roundCtr = round;
            }
        }
        else {
            tableTupleIndexEpoch[splitKey][0] = newIndex;
            tableTupleIndexEpoch[splitKey][1] = roundCtr;
            tableTupleIndexEpoch[splitKey][2] = joinIndex;
            finalState.roundCtr = roundCtr;
        }
    }

    /**
     * Considers another state achieved for a join order
     * sharing the same prefix in the table ordering.
     * Updates ("fast forwards") this state if the
     * other state is ahead.
     * The "forward" only happens for table in the node.
     *
     * @param finalState    evaluation state achieved for join order sharing prefix
     * @param table         table to update
     * @param splitKey      the split strategy
     * @param index         The join position of table.
     */
    void fastForward(State finalState, int table, int splitKey, int index) {
        // Fast forward is only executed if other state is more advanced
        // Adopt tuple indices from other state for common prefix -
        // set the remaining indices to start index.
        if (tableTupleIndexEpoch[splitKey][1] > 0) {
            int tupleIndex = tableTupleIndexEpoch[splitKey][0];
            int roundCtr = tableTupleIndexEpoch[splitKey][1];
            int joinIndex = tableTupleIndexEpoch[splitKey][2];
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
            System.out.println(table + "\t" + "No progress");
            finalState.roundCtr = -1;
        }
    }

    @Override
    public String toString() {
        List<String> keys = new ArrayList<>();
        return "Last index on Table: " + String.join(", ", keys);
    }

    boolean hasProgress(int splitKey) {
        return tableTupleIndexEpoch[splitKey][1] > 0;
    }

    int getProgress(int splitKey) {
        return tableTupleIndexEpoch[splitKey][0];
    }
}
