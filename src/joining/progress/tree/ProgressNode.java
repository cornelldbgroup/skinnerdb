package joining.progress.tree;

/**
 * Captures evaluation progress made for one specific table
 * for a given join order prefix.
 * 
 * @author Ziyun Wei
 *
 */
public class ProgressNode {
	/**
	 * Latest state reached by any join order sharing
	 * a certain table prefix.
	 */
	NodeState latestState;
	/**
	 * Points to nodes describing progress for next table.
	 */
	final ProgressNode[] childNodes;

	/**
	 * Initialize a node representing a joined table.
	 *
	 * @param nrTables		number of joining tables
	 */
	public ProgressNode(int nrTables) {
		childNodes = new ProgressNode[nrTables];
	}
}