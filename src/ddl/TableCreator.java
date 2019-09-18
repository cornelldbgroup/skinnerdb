package ddl;

import java.util.List;

import org.apache.commons.lang3.StringUtils;

import buffer.BufferManager;
import catalog.CatalogManager;
import catalog.info.ColumnInfo;
import catalog.info.TableInfo;
import data.DoubleData;
import data.IntData;
import data.LongData;
import data.StringData;
import diskio.PathUtil;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import query.ColumnRef;
import query.SQLexception;
import types.JavaType;
import types.SQLtype;
import types.TypeUtil;

/**
 * Processes statements for creating tables.
 * 
 * @author immanueltrummer
 *
 */
public class TableCreator {
	/**
	 * Adds a new table to current database by executing
	 * given table creation command.
	 * 
	 * @param createTable	table creation command
	 * @return table	new table or null
	 */
	public static TableInfo addTable(CreateTable createTable) throws Exception {
		// Check whether table exists already
		String tableName = createTable.getTable().getName().toLowerCase();
		if (CatalogManager.currentDB.nameToTable.containsKey(tableName)) {
			throw new SQLexception("Error - table " + tableName + " exists");
		}
		// Extract table name
		TableInfo table = new TableInfo(tableName, false);
		CatalogManager.currentDB.addTable(table);
		// Process column definitions
		for (ColumnDefinition colDef : createTable.getColumnDefinitions()) {
			String colName = colDef.getColumnName().toLowerCase();
			String colTypeStr = colDef.getColDataType().getDataType();
			SQLtype colType = TypeUtil.parseString(colTypeStr);
			// Parse column specifications
			boolean isPrimary = false;
			boolean isUnique = false;
			boolean isNotNull = false;
			boolean isForeign = false;
			List<String> colSpecStrings = colDef.getColumnSpecStrings();
			if (colSpecStrings != null) {
				String colSpecs = StringUtils.join(colSpecStrings, " ").toLowerCase();
				isPrimary = colSpecs.contains("primary key");
				isUnique = isPrimary | colSpecs.contains("unique");
				isNotNull = isPrimary | colSpecs.contains("not null");
				isForeign = colSpecs.contains("foreign key");				
			}
			// Make sure that type is known
			if (colType == null) {
				throw new SQLexception("Error - unknown column "
						+ "data type " + colTypeStr);
			}
			// Generate column meta-data and add to table
			ColumnInfo column = new ColumnInfo(colName, colType,
					isPrimary, isUnique, isNotNull, isForeign);
			table.addColumn(column);
		}
		// Add data paths for new column content
		PathUtil.initDataPaths(CatalogManager.currentDB);
		// Initialize with empty data (on disk and in memory)
		for (ColumnInfo colInfo : table.nameToCol.values()) {
			String colName = colInfo.name;
			ColumnRef colRef = new ColumnRef(tableName, colName);
			String dataPath = PathUtil.colToPath.get(colInfo);
			SQLtype type = colInfo.type;
			JavaType jType = TypeUtil.toJavaType(type);
			switch (jType) {
			case INT:
				IntData intData = new IntData(0);
				intData.store(dataPath);
				BufferManager.colToData.put(colRef, intData);
				break;
			case LONG:
				LongData longData = new LongData(0);
				longData.store(dataPath);
				BufferManager.colToData.put(colRef, longData);
				break;
			case DOUBLE:
				DoubleData doubleData = new DoubleData(0);
				doubleData.store(dataPath);
				BufferManager.colToData.put(colRef, doubleData);
				break;
			case STRING:
				StringData stringData = new StringData(0);
				stringData.store(dataPath);
				BufferManager.colToData.put(colRef, stringData);
				break;
			}
		}
		CatalogManager.updateStats(tableName);
		return table;
	}
}
