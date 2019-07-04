package joining.result;

import java.util.*;
import java.util.Map.Entry;

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
	 * join tables).
	 */
	public Set<ResultTuple> tuples = new HashSet<>();
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
		tuples.add(new ResultTuple(tupleIndices));
		/*
		ResultNode curNode = resultRoot;
		for (int tableCtr=0; tableCtr<nrTables; ++tableCtr) {
			int curTuple = tupleIndices[tableCtr];
			if (!curNode.childNodes.containsKey(curTuple)) {
				curNode.childNodes.put(curTuple, new ResultNode());
			}
			curNode = curNode.childNodes.get(curTuple);
		}
		*/
	}
	
	void addSubtree(ResultNode resultNode, int level, 
			int[] resultIndices, List<ResultTuple> tuples) {
		if (level==nrTables) {
			tuples.add(new ResultTuple(resultIndices));
		} else {
			for (Entry<Integer, ResultNode> entry : 
				resultNode.childNodes.entrySet()) {
				resultIndices[level] = entry.getKey();
				addSubtree(entry.getValue(), level+1, 
						resultIndices, tuples);
			}
		}
	}
	/**
	 * Returns result tuples as tuple set.
	 * 
	 * @return	set of result tuples
	 */
	public Collection<ResultTuple> getTuples() {
		return tuples;
		/*
		List<ResultTuple> tuples = new ArrayList<>();
		int[] resultIndices = new int[nrTables];
		addSubtree(resultRoot, 0, resultIndices, tuples);
		return tuples;
		*/
	}
}