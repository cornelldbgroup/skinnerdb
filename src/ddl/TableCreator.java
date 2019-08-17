package ddl;

import java.util.ArrayList;
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
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import query.ColumnRef;
import query.SQLexception;
import types.JavaType;
import types.SQLtype;
import types.TypeUtil;

/**
 * Processes statements for creating (permanent) tables.
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
	/**
	 * Updates the catalog by copying the schema of the
	 * source table into a newly created target table.
	 * The target table may be specified either as
	 * temporary or permanent.
	 * 
	 * @param fromTbl		copy schema from this table
	 * @param toTbl			newly created table copying schema
	 * @return				new table or null
	 * @throws Exception
	 */
	public static TableInfo copyTable(String fromTbl, 
			String toTbl) throws Exception {
		// Generate table creation statement
		CreateTable create = new CreateTable();
		// Specify table to create
		Table table = new Table(toTbl);
		create.setTable(table);
		// Retrieve source table schema
		TableInfo fromInfo = CatalogManager.currentDB.nameToTable.get(fromTbl);
		// Specify columns to create
		List<ColumnDefinition> colDefs = new ArrayList<>();
		create.setColumnDefinitions(colDefs);
		for (String colName : fromInfo.columnNames) {
			ColumnInfo colInfo = fromInfo.nameToCol.get(colName);
			ColumnDefinition colDef = new ColumnDefinition();
			colDef.setColumnName(colName);
			ColDataType colDataType = new ColDataType();
			colDataType.setDataType(colInfo.type.toString());
			colDef.setColDataType(colDataType);
			List<String> colSpecList = new ArrayList<>();
			if (colInfo.isPrimary) {
				colSpecList.add("primary key");				
			}
			if (colInfo.isForeign) {
				colSpecList.add("foreign key");
			}
			if (colInfo.isNotNull) {
				colSpecList.add("not null");
			}
			if (colInfo.isUnique) {
				colSpecList.add("unique");
			}
			colDef.setColumnSpecStrings(colSpecList);
			colDefs.add(colDef);
		}
		return addTable(create);
	}
}
