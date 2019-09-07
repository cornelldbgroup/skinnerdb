package catalog;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import catalog.info.ColumnInfo;
import catalog.info.DbInfo;
import catalog.info.TableInfo;
import catalog.stats.DbStats;
import catalog.stats.TableStats;
import query.ColumnRef;

/**
 * Provides access to schema information for current database.
 * 
 * @author immanueltrummer
 *
 */
public class CatalogManager {
	/**
	 * Database that is currently loaded.
	 */
	public static DbInfo currentDB;
	/**
	 * Statistics about current database.
	 */
	public static DbStats currentStats;
	/**
	 * Returns description of table with given name
	 * in current database.
	 * 
	 * @param tableName	name of table to describe
	 * @return			information on table schema
	 */
	public static TableInfo getTable(String tableName) {
		return currentDB.nameToTable.get(tableName);
	}
	/**
	 * Returns description of referenced column in current database.
	 * 
	 * @param columnRef	reference to column
	 * @return			column info
	 */
	public static ColumnInfo getColumn(ColumnRef columnRef) {
		String tableName = columnRef.aliasName;
		String columnName = columnRef.columnName;
		TableInfo table = currentDB.nameToTable.get(tableName);
		return table.nameToCol.get(columnName);
	}
	/**
	 * Retrieves cardinality of table with given name.
	 * 
	 * @param tableName	retrieve cardinality for this table
	 * @return			table cardinality (number of rows)
	 */
	public static int getCardinality(String tableName) {
		TableStats tableStats = currentStats.tableToStats.get(tableName);
		return tableStats.cardinality;
	}
	/**
	 * Loads schema information from disk as current database
	 * and generates associated statistics.
	 * 
	 * @param path	path to file with schema information
	 * @throws Exception
	 */
	public static void loadDB(String path) throws Exception {
		FileInputStream fileIn = new FileInputStream(path);
		ObjectInputStream objIn = new ObjectInputStream(fileIn);
		currentDB = (DbInfo)objIn.readObject();
		objIn.close();
		fileIn.close();
		// Generate database statistics
		currentStats = new DbStats(currentDB);
	}
	/**
	 * Deletes all temporary tables after query processing is
	 * finished.
	 * 
	 * @param except	do not remove those tables
	 * 
	 * @throws Exception
	 */
	public static void removeTempTables(Set<String> except) throws Exception {
		Iterator<Entry<String, TableInfo>> entries = 
				currentDB.nameToTable.entrySet().iterator();
		while (entries.hasNext()) {
			Entry<String, TableInfo> entry = entries.next();
			TableInfo tableInfo = entry.getValue();
			if (tableInfo.tempTable && !except.contains(tableInfo.name)) {
				entries.remove();
			}
		}
	}
	/**
	 * Deletes all temporary tables after query processing is
	 * finished.
	 * 
	 * @throws Exception
	 */
	public static void removeTempTables() throws Exception {
		removeTempTables(new HashSet<String>());
	}
	/**
	 * Updates statistics (e.g., cardinality) for
	 * table of given name. 
	 * 
	 * @param tableName		name of table
	 * @throws Exception
	 */
	public static void updateStats(String tableName) throws Exception {
		TableInfo tableInfo = currentDB.nameToTable.get(tableName);
		TableStats tableStats = new TableStats(tableInfo);
		currentStats.tableToStats.put(tableName, tableStats);
	}
}
