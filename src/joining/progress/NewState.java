package joining.progress;
/**
 * Captures processing state for a specific join order.
 * The new state object consumes less memory than super class.
 *
 * @author Ziyun Wei
 *
 */
public class NewState {
    /**
     * Progress information: time stamp, tuple index and join index
     * for each split table.
     */
    public int[][] progressForSplitTables;



    /**
     * Initializes progress information using specified start state.
     *
     * @param nrSplitTables	 the number of different split tables.
     */
    public NewState(int nrSplitTables) {
        progressForSplitTables = new int[nrSplitTables][3];
    }

    /**
     * Considers another state achieved for a join order
     * sharing the same prefix in the table ordering.
     * Updates ("fast forwards") this state if the
     * other state is ahead.
     *
     * @param seenTimeStamp			the time stamp we have seen along with the join path
     * @param splitTable	        the table we split
     * @param roundCtr	            the current time stamp
     * @param latestTupleIndex	    the latest tuple index for given table
     */
    public int fastForward(int seenTimeStamp, int splitTable, int roundCtr, int latestTupleIndex) {
        int[] progressInformation = progressForSplitTables[splitTable];
        // retrieve the time stamp in the progress for the split table
        int timeStamp = progressInformation[0];
        // retrieve the tuple index in the progress for the split table
        int tupleIndex = progressInformation[1];
        // the according progress has been written by other join samples
        if (timeStamp > 0) {
            // if tuple index is different, the new index is definitely more advanced
            // because the previous join starts from the saved tuple index
            if (tupleIndex != latestTupleIndex) {
                progressInformation[0] = roundCtr;
                progressInformation[1] = latestTupleIndex;
                seenTimeStamp = roundCtr;
            }
            // if the time stamp that we have seen is larger than saved the time stamp,
            // it means the saved progress is out-of-date.
            else if (seenTimeStamp > timeStamp) {
                progressInformation[0] = roundCtr;
                seenTimeStamp = roundCtr;
            }
            // otherwise return the
            else {
                seenTimeStamp = timeStamp;
            }
        }
        // no progress saved in the current node.
        else {
            progressInformation[0] = roundCtr;
            progressInformation[1] = latestTupleIndex;
            seenTimeStamp = roundCtr;
        }
        return seenTimeStamp;
    }

    /**
     * Restore tuple index and join index from the current node.
     * The node represents a specific table in the join order
     *
     *
     * @param state         the target state to restore
     * @param seenTimeStamp	the time stamp we have seen along with the join path
     * @param splitTable    the table to split
     * @param table         the table in the join order to restore
     */
    public int continueFrom(State state, int seenTimeStamp, int splitTable, int table) {
        int[] progressInformation = progressForSplitTables[splitTable];
        // retrieve the time stamp in the progress for the split table
        int timeStamp = progressInformation[0];
        // the according progress has been written by other join samples
        if (timeStamp > 0) {
            int tupleIndex = progressInformation[1];
            // if the current progress is up-to-date
            if (seenTimeStamp <= timeStamp) {
                state.tupleIndices[table] = tupleIndex;
                seenTimeStamp = timeStamp;
            }
            else {
                seenTimeStamp = -1;
            }
        }
        else {
            seenTimeStamp = -1;
        }
        return seenTimeStamp;
    }
}
