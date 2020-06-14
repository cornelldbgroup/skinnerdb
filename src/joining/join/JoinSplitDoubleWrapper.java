package joining.join;

import data.DoubleData;
import data.IntData;
import indexing.DoubleIndex;
import indexing.IntIndex;
import preprocessing.Context;
import query.ColumnRef;
import query.QueryInfo;

import java.util.Set;

/**
 * Uses index on join column to identify next
 * tuple to satisfy binary equality condition
 * on two integer columns. The join column is split column.
 *
 * @author Ziyun Wei
 *
 */
public class JoinSplitDoubleWrapper extends JoinIndexWrapper  {
    /**
     * Reference to prior integer column data.
     */
    final DoubleData priorDoubleData;
    /**
     * Reference to next integer index.
     */
    final DoubleIndex nextDoubleIndex;
    /**
     * Identification for according predicate.
     */
    final int splitTableID;
    /**
     * Identification for the current thread.
     */
    final int tid;
    /**
     * Initializes wrapper providing access to integer index
     * on column that appears in equi-join predicate.
     *
     * @param queryInfo		query meta-data
     * @param preSummary	maps query columns to intermediate result columns
     * @param joinCols		pair of columns in equi-join predicate
     * @param order			join order
     */
    public JoinSplitDoubleWrapper(QueryInfo queryInfo,
                          Context preSummary, Set<ColumnRef> joinCols,
                          int[] order, int splitTableID, int tid) throws Exception {
        super(queryInfo, preSummary, joinCols, order);
        priorDoubleData = (DoubleData)priorData;
        nextDoubleIndex = (DoubleIndex)nextIndex;
        this.splitTableID = splitTableID;
        this.tid = tid;
    }

    @Override
    public int nextIndex(int[] tupleIndices) {
        int priorTuple = tupleIndices[priorTable];
        double priorVal = priorDoubleData.data[priorTuple];
        int curTuple = tupleIndices[nextTable];
//        lastProposed = nextDoubleIndex.nextTuple(priorVal, curTuple);
        nextDoubleIndex.nextTuple(priorVal, curTuple, priorTuple, tid);
        return lastProposed;
    }
    @Override
    public int nrIndexed(int[] tupleIndices) {
        int priorTuple = tupleIndices[priorTable];
        double priorVal = priorDoubleData.data[priorTuple];
        return nextDoubleIndex.nrIndexed(priorVal);
    }
}
