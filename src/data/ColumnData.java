package data;

import java.io.Serializable;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;

import joining.result.ResultTuple;

/**
 * Represents data contained in one table column.
 * 
 * @author immanueltrummer
 *
 */
public abstract class ColumnData implements Serializable {
	/**
	 * Cardinality of this column.
	 */
	public final int cardinality;
	/**
	 * I-th bit is set if the i-th row contains an SQL NULL value.
	 */
	public final BitSet isNull;
	/**
	 * Initializes flags indicating NULL values.
	 * 
	 * @param cardinality	number of rows in column
	 */
	public ColumnData(int cardinality) {
		this.cardinality = cardinality;
		this.isNull = new BitSet(cardinality);
	}
	/**
	 * Contains -1 if element in first row is ordered before
	 * element in second row, 0 if both elements are equal,
	 * 1 if the second element is ordered before the first.
	 * 
	 * @param row1	index of first row
	 * @param row2	index of second row
	 * @return		-1, 0, 1 depending on element comparison
	 */
	public abstract int compareRows(int row1, int row2);
	/**
	 * Returns hash code for element in specified row.
	 * 
	 * @param row	index of row to hash
	 * @return	hash code for given row element
	 */
	public abstract int hashForRow(int row);
	/**
	 * Swaps elements of the two specified rows.
	 * 
	 * @param row1	first row index
	 * @param row2	second row index
	 */
	public void swapRows(int row1, int row2) {
		// Swap NULL flags
		boolean tempNull = isNull.get(row1);
		isNull.set(row1, isNull.get(row2));
		isNull.set(row2, tempNull);
	}
	/**
	 * Writes data to disk at specified path.
	 * 
	 * @param path	store data here
	 * @throws Exception
	 */
	public abstract void store(String path) throws Exception;
	/**
	 * Produces new column by copying rows with given indices
	 * (the same row may be copied multiple times).
	 * 
	 * @param rowsToCopy	indices of rows to copy
	 * @return				new column with copied rows
	 */
	public abstract ColumnData copyRows(List<Integer> rowsToCopy);
	/**
	 * Produces new column by copying rows with indices
	 * given as a bit set.
	 * 
	 * @param rowsToCopy	indices of rows to copy
	 * @return				new column with copied rows
	 */
	public abstract ColumnData copyRows(BitSet rowsToCopy);
	/**
	 * Produces new column by copying rows that appear
	 * at given table index within the given (composite)
	 * join result tuples.
	 * 
	 * @param tuples	list of join result tuples
	 * @param tableIdx	copy tuple indices for that table
	 * @return			new column that copies given rows
	 */
	public abstract ColumnData copyRows(
			Collection<ResultTuple> tuples, int tableIdx);
	/**
	 * Returns number of rows stored for column.
	 * 
	 * @return	cardinality
	 */
	public int getCardinality() {
		return cardinality;
	}
}
