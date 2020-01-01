package data;

import indexing.Index;
import joining.result.ResultTuple;

import java.util.BitSet;
import java.util.Collection;
import java.util.List;

public class ConstantData extends ColumnData {
    /**
     * Holds double data.
     */
    public final int cardinality;
    /**
     * The constant value.
     */
    public final long constant;

    /**
     * Initializes data array for given cardinality.
     *
     * @param cardinality	number of rows
     */
    public ConstantData(int cardinality, long constant) {
        super(cardinality);
        this.cardinality = cardinality;
        this.constant = constant;
    }

    @Override
    public int compareRows(int row1, int row2) {
        return 0;
    }

    @Override
    public long longForRow(int row) {
        return constant;
    }

    @Override
    public int hashForRow(int row) {
        return 0;
    }

    @Override
    public void store(String path) throws Exception {

    }

    @Override
    public ColumnData copyRows(List<Integer> rowsToCopy) {
        return null;
    }

    @Override
    public ColumnData copyRangeRows(int first, int last, Index index) {
        return null;
    }

    @Override
    public ColumnData copyRows(BitSet rowsToCopy) {
        return null;
    }

    @Override
    public ColumnData copyRows(Collection<ResultTuple> tuples, int tableIdx) {
        return null;
    }
}
