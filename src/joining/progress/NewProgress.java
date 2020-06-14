package joining.progress;

/**
 * Captures evaluation progress made for one specific table
 * for a given join order prefix.
 * 
 * @author Ziyun Wei
 *
 */
public class NewProgress {
	/**
	 * Latest state reached by any join order sharing
	 * a certain table prefix.
	 */
	NewState latestState;
	/**
	 * Points to nodes describing progress for next table.
	 */
	final NewProgress[] childNodes;

	public NewProgress(int nrTables) {
		childNodes = new NewProgress[nrTables];
	}
}