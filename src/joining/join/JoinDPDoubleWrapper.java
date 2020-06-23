package joining.join;

import preprocessing.Context;
import query.ColumnRef;
import query.QueryInfo;

import java.util.Set;
/**
 * Uses index on join column to identify next
 * tuple to satisfy binary equality condition
 * on two integer columns. For multi-thread version,
 * the wrapper needs to know which thread creates
 * it so that the statistics can be stored locally
 * for each thread.
 *
 * @author Ziyun Wei
 *
 */
public class JoinDPDoubleWrapper extends JoinDoubleWrapper {
    /**
     * The join operator that initializes this wrapper.
     */
    final DPJoin dpJoin;
    /**
     * Initializes wrapper providing access to double index
     * on column that appears in equi-join predicate.
     *
     * @param queryInfo  query meta-data
     * @param preSummary maps query columns to intermediate result columns
     * @param joinCols   pair of columns in equi-join predicate
     * @param order      join order
     * @param dpJoin	 join operator that creates the wrapper
     */
    public JoinDPDoubleWrapper(QueryInfo queryInfo, Context preSummary,
                               Set<ColumnRef> joinCols, int[] order, DPJoin dpJoin) throws Exception {
        super(queryInfo, preSummary, joinCols, order);
        this.dpJoin = dpJoin;
    }

    @Override
    public int nextIndex(int[] tupleIndices) {
        int priorTuple = tupleIndices[priorTable];
        double priorVal = priorDoubleData.data[priorTuple];
        int curTuple = tupleIndices[nextTable];
        lastProposed = nextDoubleIndex.nextTuple(priorVal, curTuple, dpJoin);
        return lastProposed;
    }
}
