package diskio;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import catalog.CatalogManager;
import catalog.info.ColumnInfo;
import catalog.info.DbInfo;
import catalog.info.TableInfo;

/**
 * Maps database elements to paths on disk.
 * 
 * @author immanueltrummer
 *
 */
public class PathUtil {
	/**
	 * Path to main database directory.
	 */
	public static String dbDir = null;
	/**
	 * Path to database schema file.
	 */
	public static String schemaPath = null;
	/**
	 * Path to directory containing data.
	 */
	public static String dataPath = null;
	/**
	 * If a string dictionary was created, it will
	 * be stored under this path.
	 */
	public static String dictionaryPath = null;
	/**
	 * Maps column to path of associated data file on disk.
	 */
	public static Map<ColumnInfo, String> colToPath = null;
	/**
	 * Initializes all paths to schema elements.
	 * 
	 * @param dbDir	main database directory
	 */
	public static void initSchemaPaths(String dbDir) {
		PathUtil.dbDir = dbDir;
		schemaPath = Paths.get(dbDir, "schema.sdb").toString();
	}
	/**
	 * Initializes all paths related to data files of
	 * given database. Assumes that schema has been
	 * initialized before.
	 * 
	 * @param dbInfo	database schema information
	 */
	public static void initDataPaths(DbInfo dbInfo) {
		// Initialize dictionary and data directory paths
		dictionaryPath = Paths.get(dbDir, "stringdic.sdb").toString();
		dataPath = Paths.get(dbDir, "data").toString();
		// Iterate over database tables
		colToPath = new HashMap<>();
		for (TableInfo tblInfo : dbInfo.nameToTable.values()) {
			String tblName = tblInfo.name;
			for (ColumnInfo colInfo : tblInfo.nameToCol.values()) {
				String colName = colInfo.name;
				String colPath = Paths.get(dataPath, tblName, colName).toString();
				colToPath.put(colInfo, colPath);
			}
		}
	}
}
