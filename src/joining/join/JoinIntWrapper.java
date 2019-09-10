package joining.join;

import java.util.Set;

import data.IntData;
import indexing.IntIndex;
import preprocessing.Context;
import query.ColumnRef;
import query.QueryInfo;

/**
 * Uses index on join column to identify next
 * tuple to satisfy binary equality condition
 * on two integer columns.
 * 
 * @author immanueltrummer
 *
 */
public class JoinIntWrapper extends JoinIndexWrapper {
	/**
	 * Reference to prior integer column data.
	 */
	final IntData priorIntData;
	/**
	 * Reference to next integer index.
	 */
	final IntIndex nextIntIndex;
	/**
	 * Initializes wrapper providing access to integer index
	 * on column that appears in equi-join predicate.
	 * 
	 * @param queryInfo		query meta-data
	 * @param preSummary	maps query columns to intermediate result columns
	 * @param joinCols		pair of columns in equi-join predicate
	 * @param order			join order
	 */
	public JoinIntWrapper(QueryInfo queryInfo, 
			Context preSummary, Set<ColumnRef> joinCols, 
			int[] order) throws Exception {
		super(queryInfo, preSummary, joinCols, order);
		priorIntData = (IntData)priorData;
		nextIntIndex = (IntIndex)nextIndex;
	}
	@Override
	public int nextIndex(int[] tupleIndices) {
		int priorTuple = tupleIndices[priorTable];
		int priorVal = priorIntData.data[priorTuple];
		int curTuple = tupleIndices[nextTable];
		return nextIntIndex.nextTuple(priorVal, curTuple);
	}
	@Override
	public int nrIndexed(int[] tupleIndices) {
		int priorTuple = tupleIndices[priorTable];
		int priorVal = priorIntData.data[priorTuple];
		return nextIntIndex.nrIndexed(priorVal);
	}
}
