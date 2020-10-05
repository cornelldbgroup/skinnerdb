package catalog.stats;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import catalog.info.DbInfo;
import catalog.info.TableInfo;
import diskio.PathUtil;

/**
 * Statistics about database content (e.g., the
 * cardinality of each database table).
 * 
 * @author Anonymous
 *
 */
public class DbStats {
	/**
	 * Maps each table name to associated table statistics.
	 */
	public final Map<String, TableStats> tableToStats;
	/**
	 * Generate statistics about database.
	 * 
	 * @param dbInfo	database meta-data
	 */
	public DbStats(DbInfo dbInfo) throws Exception {
		// Make sure that data paths are initialized
		PathUtil.initDataPaths(dbInfo);
		// Collect statistics about each table
		tableToStats = new ConcurrentHashMap<>();
		for (TableInfo tableInfo : dbInfo.nameToTable.values()) {
			String tableName = tableInfo.name;
			TableStats tableStats = new TableStats(tableInfo);
			tableToStats.put(tableName, tableStats);
		}
	}
}
