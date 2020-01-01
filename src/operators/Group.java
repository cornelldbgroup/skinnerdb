package operators;

import java.util.Collection;

import data.ColumnData;

/**
 * Represents a group of rows sharing the
 * same value in a set of columns of the
 * same cardinality.
 * 
 * @author immanueltrummer
 *
 */
public class Group {
	/**
	 * Index of row that represents this group
	 * (used for comparisons).
	 */
	public final int representativeRow;
	/**
	 * Data of columns according to which groups
	 * are defined (rows in the same group share
	 * the same values in each of those columns).
	 */
	public final Collection<ColumnData> groupCols;
	/**
	 * Long array where each element represents a
	 * representative data of group.
	 */
	public final long[] rowValues;
	/**
	 * Initializes this group for a given row as
	 * representative, using given set of columns
	 * for calculating groups.
	 * 
	 * @param rowID		representative row ID
	 * @param groupCols	set of columns for grouping
	 */
	public Group(int rowID, Collection<ColumnData> groupCols) {
		this.representativeRow = rowID;
		this.groupCols = groupCols;
		this.rowValues = new long[groupCols.size()];
		int cid = 0;
		for (ColumnData col : groupCols) {
			rowValues[cid] = col.longForRow(rowID);
			cid++;
		}
	}
	@Override
	public boolean equals(Object other) {
		Group otherGroup = (Group)other;
		for (int i = 0; i < groupCols.size(); i++) {
			if (otherGroup.rowValues[i] != rowValues[i]) {
				return false;
			}
		}
		return true;
	}
	@Override
	public int hashCode() {
		int hash = 0;
		for (long value: rowValues) {
			hash += Long.hashCode(value);
		}
		return hash;
	}
}
