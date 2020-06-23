package joining.progress.tree;

import joining.progress.hash.State;

/**
 * Captures processing state for a specific node in the join order path.
 * In the state, progress is saved for different split tables.
 *
 * @author Ziyun Wei
 *
 */
public class NodeState {
    /**
     * The outer dimension represents different choices of split tables.
     * The inner dimension represent progress information:
     * time stamp, tuple index and join index
     * for each split table.
     */
    public Progress[] progressForSplitTables;
    /**
     * Initializes the state for all potential split tables.
     *
     * @param nrSplitTables	 the number of different split tables.
     */
    public NodeState(int nrSplitTables) {
        progressForSplitTables = new Progress[nrSplitTables];
    }
    /**
     * Update tuple index and join index in the current node.
     * Considering different split tables, progress are updated
     * in the according position of the array.
     *
     * @param nodeTimeStamp			the time stamp we have seen along with the join path
     * @param splitTable	        the table we split
     * @param roundCtr	            the current time stamp
     * @param latestTupleIndex	    the latest tuple index for given table
     * @param lastIndex             last position index in join order
     * @return                      the time stamp considering the current node.
     */
    public int updateProgress(int nodeTimeStamp,
                              int splitTable,
                              int roundCtr,
                              int latestTupleIndex,
                              int lastIndex) {
        Progress progressInformation = progressForSplitTables[splitTable];
        if (progressInformation == null) {
            progressForSplitTables[splitTable] =
                    new Progress(roundCtr, latestTupleIndex, lastIndex);
            return roundCtr;
        }
        // retrieve the time stamp in the progress for the split table
        int timeStamp = progressInformation.timeStamp;
        // retrieve the tuple index in the progress for the split table
        int tupleIndex = progressInformation.tupleIndex;
        // the according progress has been written by other join samples
        // if tuple index is different, the new index is definitely more advanced
        // because the previous join starts from the saved tuple index
        if (tupleIndex != latestTupleIndex) {
            progressInformation.timeStamp = roundCtr;
            progressInformation.tupleIndex = latestTupleIndex;
            nodeTimeStamp = roundCtr;
        }
        // if the time stamp that we have seen is larger than saved the time stamp,
        // it means the saved progress is out-of-date.
        else if (nodeTimeStamp > timeStamp) {
            progressInformation.timeStamp = roundCtr;
            nodeTimeStamp = roundCtr;
        }
        // otherwise return the
        else {
            nodeTimeStamp = timeStamp;
        }
        return nodeTimeStamp;
    }
    /**
     * Restore tuple index and join index from the current node.
     * The node represents a specific table in the join order
     *
     * @param state         the target state to restore
     * @param nodeTimeStamp	the time stamp we have seen along with the join path
     * @param splitTable    the table to split
     * @param table         the table in the join order to restore
     * @return              the time stamp considering the current node.
     *                      -1 means the node is out-of-date
     */
    public int continueFrom(State state, int nodeTimeStamp, int splitTable, int table) {
        Progress progressInformation = progressForSplitTables[splitTable];
        if (progressInformation == null) {
            return -1;
        }
        // retrieve the time stamp in the progress for the split table
        int timeStamp = progressInformation.timeStamp;
        // the according progress has been written by other join samples
        int tupleIndex = progressInformation.tupleIndex;
        // if the current progress is up-to-date
        if (nodeTimeStamp <= timeStamp) {
            state.tupleIndices[table] = tupleIndex;
            nodeTimeStamp = timeStamp;
        }
        else {
            nodeTimeStamp = -1;
        }
        return nodeTimeStamp;
    }
}
