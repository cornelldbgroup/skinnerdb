package tools;

import java.util.ArrayList;
import java.util.List;

import buffer.BufferManager;
import catalog.CatalogManager;
import catalog.info.ColumnInfo;
import catalog.info.TableInfo;
import data.ColumnData;
import data.IntData;
import data.StringData;
import query.ColumnRef;
import types.SQLtype;

/**
 * Copies part of a database into a new repository.
 * 
 * @author immanueltrummer
 *
 */
public class CopyData {
	/**
	 * Creates a new database by copying parts of
	 * a given database.
	 * 
	 * @param args	path to schema, target data dir, 
	 * 				number of rows to copy
	 */
	public static void main(String[] args) throws Exception {
		// Check number of command line parameters
		if (args.length != 3) {
			System.out.println("Specify schema path, target path, "
					+ "and maximal number of rows to copy!");
			return;
		}
		String schemaPath = args[0];
		String targetDataDir = args[1];
		int maxNrRows = Integer.parseInt(args[2]);
		// Load schema and data dictionary
		CatalogManager.loadDB(schemaPath);
		BufferManager.loadDictionary();
		// Iterate over database tables
		for (TableInfo table : 
			CatalogManager.currentDB.nameToTable.values()) {
			String tableName = table.name;
			// Iterate over table columns
			for (ColumnInfo column : table.nameToCol.values()) {
				String columnName = column.name;
				ColumnRef colRef = new ColumnRef(tableName, columnName);
				copyColumn(colRef, targetDataDir, maxNrRows);
			}
		}
	}
	/**
	 * Copies at most the specified number of rows from the source
	 * column to the target directory.
	 * 
	 * @param colRef		reference to source column
	 * @param targetDir		target data dictionary
	 * @param maxNrRows		maximal number of rows to copy
	 * @throws Exception
	 */
	static void copyColumn(ColumnRef colRef, String targetDir, 
			int maxNrRows) throws Exception {
		// Determine new cardinality
		ColumnData sourceData = BufferManager.getData(colRef);
		int oldCard = sourceData.getCardinality();
		int newCard = Math.min(oldCard, maxNrRows);
		ColumnInfo colInfo = CatalogManager.getColumn(colRef);
		ColumnData copyData = null;
		// Distinguish compressed string columns
		if (colInfo.type.equals(SQLtype.STRING_CODE)) {
			StringData stringData = new StringData(newCard);
			IntData intSource = (IntData)sourceData;
			for (int i=0; i<newCard; ++i) {
				int code = intSource.data[i];
				String token = BufferManager.dictionary.getString(code);
				stringData.data[i] = token;
			}
			copyData = stringData;
		} else {
			// Determine indices of rows to copy
			List<Integer> rowIndices = new ArrayList<Integer>();
			for (int i=0; i<newCard; ++i) {
				rowIndices.add(i);
			}
			// Copy previously determined rows
			copyData = sourceData.copyRows(rowIndices);
		}
		// Store copy to hard disk
		String tableName = colRef.aliasName;
		String colName = colRef.columnName;
		String targetPath = targetDir + "/" + tableName + "/" + colName;
		copyData.store(targetPath);
		// Remove unnecessary data from buffer
		BufferManager.unloadColumn(colRef);
	}
}
