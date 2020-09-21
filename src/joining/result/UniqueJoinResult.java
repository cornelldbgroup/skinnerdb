package joining.result;

import data.ColumnData;

import java.util.Arrays;
import java.util.List;

public abstract class UniqueJoinResult {
    /**
     * Number of columns selected.
     */
    final int nrColumns;
    /**
     * Contains join result tuples, represented as
     * integer vectors (each vector component
     * captures the tuple index for one of the
     * join tables).
     */
    public final int[] tuples;
    /**
     * Contains data for each column that appears in
     * the select clause.
     */
    public final ColumnData[] uniqueColumns;
    /**
     * Contains table index in the join order for each column
     * that appears in the select clause.
     */
    final int[] tableIndices;

    public UniqueJoinResult(int nrColumns,
                            ColumnData[] uniqueColumns,
                            int[] tableIndices) {
        this.nrColumns = nrColumns;
        this.uniqueColumns = uniqueColumns;
        tuples = new int[uniqueColumns.length];
        Arrays.fill(tuples, -1);
        this.tableIndices = tableIndices;
    }

    public abstract void add(int[] tupleIndices);

    public abstract void merge(UniqueJoinResult result);
}
