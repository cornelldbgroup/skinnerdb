package joining.result;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Represents the result of a query in compact form
 * (indices of joined result tuples in base tables).
 *
 * @author immanueltrummer
 *
 */
public class JoinResult {
	/**
	 * Contains join result tuples, represented as
	 * integer vectors (each vector component
	 * captures the tuple index for one of the
	 * join tables). The value is true for each
	 * key (no specific concurrent hash set
	 * implementation available in Java and
	 * ConcurrentSkipListSet has logarithmic
	 * lookup complexity).
	 */
	public Map<ResultTuple, Boolean> tuples = 
			new ConcurrentHashMap<>();
	/**
	 * Number of tables being joined.
	 */
	final int nrTables;
	/**
	 * Root of tree representing join result.
	 */
	final ResultNode resultRoot;
	/**
	 * Initializes join result for query of given size.
	 * 
	 * @param nrTables	number of tables being joined
	 */
	public JoinResult(int nrTables) {
		this.nrTables = nrTables;
		this.resultRoot = new ResultNode();
	}
	/**
	 * Add tuple indices to the partial result set.
	 *
	 * @param tupleIndices  tuple indices
	 */
	public void add(int[] tupleIndices) {
		tuples.put(new ResultTuple(tupleIndices), true);
	}
	/**
	 * Returns result tuples as tuple set.
	 * 
	 * @return	set of result tuples
	 */
	public Collection<ResultTuple> getTuples() {
		return tuples.keySet();
	}
}