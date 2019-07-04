package catalog.info;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

/**
 * Meta-data about one database table.
 * 
 * @author immanueltrummer
 *
 */
public class TableInfo implements Serializable {
	/**
	 * Name of the table.
	 */
	public final String name;
	/**
	 * Ordered list of column names.
	 */
	public final List<String> columnNames;
	/**
	 * Maps column names to column meta-data.
	 */
	public final Map<String, ColumnInfo> nameToCol;
	/**
	 * Whether the table is a temporary table (which
	 * can be deleted after processing the current query).
	 */
	public final boolean tempTable;
	/**
	 * Initializes table with given name.
	 * 
	 * @param name		table name
	 * @param tempTable	whether the table is a temporary table
	 */
	public TableInfo(String name, boolean tempTable) {
		this.name = name;
		this.tempTable = tempTable;
		this.columnNames = new ArrayList<String>();
		this.nameToCol = new HashMap<String, ColumnInfo>();
	}
	/**
	 * Adds given column to this table.
	 * 
	 * @param column	new column to add
	 */
	public void addColumn(ColumnInfo column) throws Exception {
		// Check whether column of same name exists
		if (columnNames.contains(column.name)) {
			throw new Exception("Error - column " + column.name + 
					" exists in table " + name);
		}
		// Insert new column
		columnNames.add(column.name);
		nameToCol.put(column.name, column);
	}
	@Override
	public String toString() {
		List<String> columns = new ArrayList<String>();
		for (String columnName : columnNames) {
			ColumnInfo column = nameToCol.get(columnName);
			columns.add(column.toString());
		}
		return name + "(" + StringUtils.join(columns, ", ") + ")";
	}
}
