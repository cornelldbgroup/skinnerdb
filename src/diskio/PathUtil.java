package diskio;

import java.io.FileOutputStream;
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
 * @author Anonymous
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
	 * Path to configuration file.
	 */
	public static String configPath = null;
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
	/**
	 * Initializes all paths related to configuration file.
	 *
	 * @param dbDir	main database directory
	 */
	public static void initConfigPaths(String dbDir) {
		configPath = Paths.get(dbDir, "config.sdb").toString();
	}
	/**
	 * Create the configuration file for specific database
	 * and set parameters to default values.
	 *
	 * @throws Exception
	 */
	public static void createConfig() throws Exception {
		String configPath = PathUtil.configPath;
		FileOutputStream fileOut = new FileOutputStream(configPath);
		String[] parameters = new String[]{
				"PARALLEL_ALGO", "NR_WARMUP",
				"NR_EXECUTORS", "NR_BATCHES",
				"WRITE_RESULTS"};
		String[] values = new String[]{"DP", "1", "1", "120", "true"};
		for (int paraCtr = 0; paraCtr < parameters.length; paraCtr++) {
			String line = parameters[paraCtr] + "=" + values[paraCtr] + "\n";
			byte[] strToBytes = line.getBytes();
			fileOut.write(strToBytes);
		}
		fileOut.close();
	}
}
