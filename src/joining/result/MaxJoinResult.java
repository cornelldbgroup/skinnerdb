package joining.result;

import data.ColumnData;

public class MaxJoinResult extends UniqueJoinResult {

    public MaxJoinResult(int nrColumns, ColumnData[] uniqueColumns, int[] tableIndices) {
        super(nrColumns, uniqueColumns, tableIndices);
    }

    @Override
    public void add(int[] tupleIndices) {
        for (int columnCtr = 0; columnCtr < nrColumns; columnCtr++) {
            int tableIdx = tableIndices[columnCtr];
            int row = tupleIndices[tableIdx];
            ColumnData srcData = uniqueColumns[columnCtr];
            int prevRow = tuples[columnCtr];
            if (!srcData.isNull.get(row) && srcData.longForRow(row) != Integer.MIN_VALUE) {
                // Is this the first row?
                if (prevRow == -1) {
                    tuples[columnCtr] = row;
                } else {
                    int cmp = srcData.compareRows(prevRow, row);
                    if (cmp == -1) {
                        tuples[columnCtr] = row;
                    }
                }
            }
        }
    }

    @Override
    public void merge(UniqueJoinResult result) {
        for (int columnCtr = 0; columnCtr < nrColumns; columnCtr++) {
            ColumnData srcData = uniqueColumns[columnCtr];
            int row = result.tuples[columnCtr];
            int prevRow = tuples[columnCtr];
            if (!srcData.isNull.get(row) && srcData.longForRow(row) != Integer.MIN_VALUE) {
                // Is this the first row?
                if (prevRow == -1) {
                    tuples[columnCtr] = row;
                } else {
                    int cmp = srcData.compareRows(prevRow, row);
                    if (cmp == -1) {
                        tuples[columnCtr] = row;
                    }
                }
            }
        }
    }
}
