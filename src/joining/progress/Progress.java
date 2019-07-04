package joining.progress;

/**
 * Captures evaluation progress made for one specific table
 * for a given join order prefix.
 * 
 * @author immanueltrummer
 *
 */
public class Progress {
	/**
	 * Latest state reached by any join order sharing
	 * a certain table prefix.
	 */
	State latestState;
	/**
	 * Points to nodes describing progress for next table.
	 */
	final Progress[] childNodes;
	
	public Progress(int nrTables) {
		childNodes = new Progress[nrTables];
	}
}