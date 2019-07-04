package tools;

import catalog.CatalogManager;
import catalog.info.DbInfo;
import diskio.PathUtil;

/**
 * Creates a new database.
 * 
 * @author immanueltrummer
 *
 */
public class CreateDB {
	/**
	 * Creates a new database with specified name
	 * (first argument), given database directory
	 * (second argument), and stores it at
	 * specified location.
	 * 
	 * @param args	database name, database directory
	 */
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("Error - specify database name "
					+ "and database directory!");
			return;
		}
		String name = args[0];
		String dbDir = args[1];
		PathUtil.initSchemaPaths(dbDir);
		CatalogManager.currentDB = new DbInfo(name);
		PathUtil.initDataPaths(CatalogManager.currentDB);
		CatalogManager.currentDB.storeDB();
	}
}
