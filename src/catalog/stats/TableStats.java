package catalog.stats;

import buffer.BufferManager;
import catalog.info.TableInfo;
import data.ColumnData;
import query.ColumnRef;

/**
 * Statistical meta-data about table content.
 * 
 * @author immanueltrummer
 *
 */
public class TableStats {
	/**
	 * The number of rows in the analyzed table.
	 */
	public final int cardinality;
	/**
	 * Calculate statistics for given table.
	 * 
	 * @param tableInfo	table to analyze
	 */
	public TableStats(TableInfo tableInfo) throws Exception {
		String tableName = tableInfo.name;
		if (tableInfo.columnNames.isEmpty()) {
			this.cardinality = 0;
		} else {
			String firstColName = tableInfo.columnNames.get(0);
			ColumnRef firstColRef = new ColumnRef(tableName, firstColName);
			ColumnData firstColData = BufferManager.getData(firstColRef);
			this.cardinality = firstColData.cardinality;			
		}
	}
	@Override
	public String toString() {
		return "Cardinality: " + cardinality;
	}
}
