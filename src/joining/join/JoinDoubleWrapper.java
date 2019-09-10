package joining.join;

import java.util.Set;

import data.DoubleData;
import indexing.DoubleIndex;
import preprocessing.Context;
import query.ColumnRef;
import query.QueryInfo;

/**
 * Uses index on join column to identify next
 * tuple to satisfy binary equality condition
 * on two double columns.
 * 
 * @author immanueltrummer
 *
 */
public class JoinDoubleWrapper extends JoinIndexWrapper {
	/**
	 * Reference to prior double column data.
	 */
	final DoubleData priorDoubleData;
	/**
	 * Reference to next double index.
	 */
	final DoubleIndex nextDoubleIndex;
	/**
	 * Initializes wrapper providing access to double index
	 * on column that appears in equi-join predicate.
	 * 
	 * @param queryInfo		query meta-data
	 * @param preSummary	maps query columns to intermediate result columns
	 * @param joinCols		pair of columns in equi-join predicate
	 * @param order			join order
	 */
	public JoinDoubleWrapper(QueryInfo queryInfo, 
			Context preSummary, Set<ColumnRef> joinCols, 
			int[] order) throws Exception {
		super(queryInfo, preSummary, joinCols, order);
		priorDoubleData = (DoubleData)priorData;
		nextDoubleIndex = (DoubleIndex)nextIndex;
	}
	@Override
	public int nextIndex(int[] tupleIndices) {
		int priorTuple = tupleIndices[priorTable];
		double priorVal = priorDoubleData.data[priorTuple];
		int curTuple = tupleIndices[nextTable];
		return nextDoubleIndex.nextTuple(priorVal, curTuple);
	}
	@Override
	public int nrIndexed(int[] tupleIndices) {
		int priorTuple = tupleIndices[priorTable];
		double priorVal = priorDoubleData.data[priorTuple];
		return nextDoubleIndex.nrIndexed(priorVal);
	}
}
