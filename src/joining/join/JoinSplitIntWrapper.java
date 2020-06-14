package joining.join;

import data.IntData;
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
public class JoinSplitIntWrapper extends JoinIndexWrapper {
    /**
     * Reference to prior integer column data.
     */
    final IntData priorIntData;
    /**
     * Reference to next integer index.
     */
    final IntIndex nextIntIndex;
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
     * @param splitTableID	table to split
     */
    public JoinSplitIntWrapper(QueryInfo queryInfo,
                                  Context preSummary, Set<ColumnRef> joinCols,
                                  int[] order, int splitTableID, int tid) throws Exception {
        super(queryInfo, preSummary, joinCols, order);
        priorIntData = (IntData)priorData;
        nextIntIndex = (IntIndex)nextIndex;
        this.splitTableID = splitTableID;
        this.tid = tid;
    }
    @Override
    public int nextIndex(int[] tupleIndices) {
        int priorTuple = tupleIndices[priorTable];
        int priorVal = priorIntData.data[priorTuple];
        int curTuple = tupleIndices[nextTable];
//        lastProposed = nextIntIndex.nextTuple(priorVal, curTuple);
        lastProposed = nextIntIndex.nextTuple(priorVal, curTuple, priorTuple, tid);
        return lastProposed;
    }
    @Override
    public int nrIndexed(int[] tupleIndices) {
        int priorTuple = tupleIndices[priorTable];
        int priorVal = priorIntData.data[priorTuple];
        return nextIntIndex.nrIndexed(priorVal);
    }
}
