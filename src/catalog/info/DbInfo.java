package catalog.info;

import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import diskio.PathUtil;

/**
 * Meta-data about a database.
 * 
 * @author immanueltrummer
 *
 */
public class DbInfo implements Serializable {
	/**
	 * Name of the database.
	 */
	public final String name;
	/**
	 * Whether the database has been compressed.
	 */
	public boolean compressed = false;
	/**
	 * Maps table names to corresponding tables.
	 */
	public final Map<String, TableInfo> nameToTable;
	/**
	 * Initializes a database of specified name.
	 * 
	 * @param name			name of database
	 */
	public DbInfo(String name) {
		this.name = name;
		// Using concurrent data structure allows threads
		// to update catalog in joining.parallel (e.g., if generating
		// multiple intermediate results in joining.parallel).
		this.nameToTable = new ConcurrentHashMap<>();
	}
	/**
	 * Adds a new table to the database.
	 * 
	 * @param table	new table to add
	 */
	public void addTable(TableInfo table) throws Exception {
		// Make sure that no table of same name exists
		if (nameToTable.containsKey(table.name)) {
			throw new Exception("Error - table " + 
					table.name + " already exists!");
		}
		// Insert table
		nameToTable.put(table.name, table);
	}
	/**
	 * Serializes schema information of current database
	 * and stores in the DB info path.
	 * 
	 * @throws Exception
	 */
	public void storeDB() throws Exception {
		String schemaPath = PathUtil.schemaPath;
		FileOutputStream fileOut = new FileOutputStream(schemaPath);
		ObjectOutputStream objOut = new ObjectOutputStream(fileOut);
		objOut.writeObject(this);
		objOut.close();
		fileOut.close();
	}
	
	@Override
	public String toString() {
		StringBuilder resultBuilder = new StringBuilder();
		resultBuilder.append("***********" + System.lineSeparator());
		resultBuilder.append("Database '" + name + "':" + System.lineSeparator());
		for (Entry<String, TableInfo> entry : nameToTable.entrySet()) {
			resultBuilder.append(entry.getValue().toString());
			resultBuilder.append(System.lineSeparator());
		}
		resultBuilder.append("**********");
		return resultBuilder.toString();
	}
}
