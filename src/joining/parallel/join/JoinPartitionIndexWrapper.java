package joining.parallel.join;
import data.ColumnData;
import data.IntData;
import expressions.ExpressionInfo;
import indexing.Index;
import joining.parallel.indexing.PartitionIndex;
import query.ColumnRef;
import query.QueryInfo;

import java.util.Iterator;

/**
 * Uses index on join column to identify next
 * tuple to satisfy binary equality condition
 * on two partitioned columns.
 *
 * @author Ziyun Wei
 */
public abstract class JoinPartitionIndexWrapper {
    /**
     * Prior table in join order from
     * which we obtain value for lookup.
     */
    final int priorTable;
    /**
     * Next table in join order for
     * which we propose a tuple index.
     */
    final int nextTable;
    /**
     * Reference to prior column data.
     */
    final ColumnData priorData;
    /**
     * Index on join column to use.
     */
    final Index nextIndex;
    /**
     * Initialize index wrapper for
     * given query and join order.
     *
     * @param equiPred      join predicate associated with join index wrapper.
     * @param order         the order of join tables.
     */
    public JoinPartitionIndexWrapper(ExpressionInfo equiPred, int[] order) {
        Iterator<Integer> tableIter = equiPred.indexMentioned.keySet().iterator();
        int table1 = tableIter.next();
        int table2 = tableIter.next();
        // Determine position of tables in join order
        int pos1 = tablePos(order, table1);
        int pos2 = tablePos(order, table2);
        // Assign prior and next table accordingly
        priorTable = pos1<pos2?table1:table2;
        nextTable = pos1<pos2?table2:table1;
        // Get column data reference for prior table
        priorData = equiPred.dataMentioned.get(priorTable);
        // Get index for next table
        nextIndex = equiPred.indexMentioned.get(nextTable);
    }

    /**
     * Extracts index of table in query column reference.
     *
     * @param query		query to process
     * @param queryCol	column that appears in query
     * @return			index of query table
     */
    int tableIndex(QueryInfo query, ColumnRef queryCol) {
        return query.aliasToIndex.get(queryCol.aliasName);
    }
    /**
     * Returns position of given table in join order
     * or -1 if the table is not found.
     *
     * @param order		join order
     * @param table		index of table
     * @return			position of table in join order
     */
    int tablePos(int[] order, int table) {
        int nrTables = order.length;
        for (int pos=0; pos<nrTables; ++pos) {
            if (order[pos] == table) {
                return pos;
            }
        }
        return -1;
    }
    /**
     * Propose next index in next table that
     * satisfies equi-join condition with
     * current tuple in prior table, returns
     * cardinality if no such tuple is found.
     *
     * @param tupleIndices	current tuple indices
     * @return	next interesting tuple index or cardinality
     */
    public abstract int nextIndex(int[] tupleIndices, int[] nextSize);

    /**
     * Propose next index in next table that
     * satisfies equi-join condition in partitions with
     * current tuple in prior table, returns
     * cardinality if no such tuple is found.
     *
     * @param tupleIndices  current tuple indices
     * @param tid           thread id
     * @return              next interesting tuple index or cardinality
     */
    public abstract int nextIndexInScope(int[] tupleIndices, int tid, int[] nextSize);
    /**
     * Propose next index in next table that
     * satisfies equi-join condition with
     * current tuple in prior table, returns
     * cardinality if no such tuple is found.
     *
     * @param tupleIndices	current tuple indices
     * @return	next interesting tuple index or cardinality
     */
    public abstract boolean evaluate(int[] tupleIndices);
    /**
     * Propose next index in next table that
     * satisfies equi-join condition with
     * current tuple in prior table, returns
     * cardinality if no such tuple is found.
     *
     * @param tupleIndices	current tuple indices
     * @return	next interesting tuple index or cardinality
     */
    public abstract boolean evaluateInScope(int[] tupleIndices, int tid);

    @Override
    public String toString() {
        return "Prior table:\t" + priorTable + "; Next:\t" + nextTable;
    }
}
