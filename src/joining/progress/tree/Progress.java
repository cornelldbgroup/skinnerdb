package joining.progress.tree;

/**
 * The progress wrapper for given joined table and split table.
 * The wrapper includes progress information:
 * 1) time stamp: the time stamp when the node is updated.
 * 2) tuple index: the index achieved in the joined table
 *    when the node is updated.
 * 3) last index: last position index in join order
 * 	  over whose tuples we have iterated.
 *
 * @author Ziyun Wei
 */
public class Progress {
    /**
     * The time stamp when the node is updated.
     */
    int timeStamp;
    /**
     * Last tuple index in the joined table.
     */
    int tupleIndex;
    /**
     * Last position index in join order.
     */
    int lastIndex;

    /**
     * Initialize the progress wrapper for the joined table and split table.
     *
     * @param timeStamp     time stamp of the last learning sample
     * @param tupleIndex    last tuple index in the joined table of the last learning sample
     * @param lastIndex     last position index of the last learning sample
     */
    Progress(int timeStamp, int tupleIndex, int lastIndex) {
        this.timeStamp = timeStamp;
        this.tupleIndex = tupleIndex;
        this.lastIndex = lastIndex;
    }
}
