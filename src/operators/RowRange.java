package operators;

/**
 * Represents a range of row indices.
 * 
 * @author immanueltrummer
 *
 */
public class RowRange {
	/**
	 * First tuple index in range.
	 */
	public final int firstTuple;
	/**
	 * Last tuple index in range.
	 */
	public final int lastTuple;
	/**
	 * Initialize range for first and
	 * last tuple index.
	 * 
	 * @param firstTuple	index of first tuple in range
	 * @param lastTuple		index of last tuple in range
	 */
	public RowRange(int firstTuple, int lastTuple) {
		this.firstTuple = firstTuple;
		this.lastTuple = lastTuple;
	}
}
