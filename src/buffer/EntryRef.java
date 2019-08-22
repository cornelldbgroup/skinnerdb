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
     * id of the entry (unique within query).
     */
    public final int id;
    /**
     * start position (unique within column).
     */
    public final int start;
    /**
     * actual positions array
     */
    public int[] positions;
    /**
     * Initializes reference for given column id and value.
     *
     * @param id		id of the entry
     * @param start	    start position of the column
     */
    public EntryRef(int id, int start, int[] positions) {
        this.id = id;
        this.start = start;
        this.positions = positions;
    }
    @Override
    public boolean equals(Object other) {
        if (other instanceof EntryRef) {
            EntryRef otherRef = (EntryRef)other;
            return id == otherRef.id;
        } else {
            return false;
        }
    }
    @Override
    public int hashCode() {
        return id * 31 + start;
    }
}
