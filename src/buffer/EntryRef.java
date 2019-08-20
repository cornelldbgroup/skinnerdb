package buffer;

/**
 * Represents a reference to a specific entry
 * saved in the buffer.
 *
 * @author Ziyun Wei
 *
 */
public class EntryRef {
    /**
     * id of the column (unique within query).
     */
    public final int cid;
    /**
     * start position (unique within column).
     */
    public final int start;
    public final int[] positions;
    /**
     * Initializes reference for given column id and value.
     *
     * @param cid		id of the column
     * @param start	    start position of the column
     */
    public EntryRef(int cid, int start, int[] positions) {
        this.cid = cid;
        this.start = start;
        this.positions = positions;
    }
    @Override
    public boolean equals(Object other) {
        if (other instanceof EntryRef) {
            EntryRef otherRef = (EntryRef)other;
            return cid == otherRef.cid && start == otherRef.start;
        } else {
            return false;
        }
    }
}
