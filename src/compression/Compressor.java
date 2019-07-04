package compression;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import buffer.BufferManager;
import catalog.CatalogManager;
import catalog.info.ColumnInfo;
import catalog.info.TableInfo;
import data.Dictionary;
import data.IntData;
import data.StringData;
import diskio.PathUtil;
import query.ColumnRef;
import types.SQLtype;

/**
 * Compresses the current database for smaller memory footprint
 * and faster processing. Currently focused on compressing string
 * columns.
 * 
 * @author immanueltrummer
 *
 */
public class Compressor {
	/**
	 * Compress database columns, store encoded data on disk
	 * and update catalog accordingly.
	 */
	public static void compress() throws Exception {
		// Collect columns to compress.
		System.out.println("Collecting string columns ...");
		List<ColumnRef> stringCols = stringColumns();
		System.out.println("Columns to compress:\t" + stringCols.toString());
		// Create and store dictionary.
		System.out.println("Creating dictionary ...");
		Dictionary dic = createDictionary(stringCols);
		BufferManager.dictionary = dic;
		String dicPath = PathUtil.dictionaryPath;
		dic.store(dicPath);
		System.out.println("Dictionary created.");
		// Create and store compressed data.
		System.out.println("Compressing columns ...");
		compressColumns(stringCols, dic);
		System.out.println("Columns compressed.");
		// Update and store catalog.
		updateCatalog(stringCols);
		System.out.println("Updated catalog.");
	}
	/**
	 * Returns references to all string columns in the database.
	 * 
	 * @return	list of string column references
	 */
	static List<ColumnRef> stringColumns() {
		List<ColumnRef> columns = new ArrayList<ColumnRef>();
		// Iterate over database tables
		for (TableInfo tableInfo : 
			CatalogManager.currentDB.nameToTable.values()) {
			String tableName = tableInfo.name;
			// Iterate over table columns
			for (ColumnInfo colInfo : tableInfo.nameToCol.values()) {
				// We compress string columns
				if (colInfo.type.equals(SQLtype.STRING)) {
					String columnName = colInfo.name;
					columns.add(new ColumnRef(tableName, columnName));
				}
			}
		}
		return columns;
	}
	/**
	 * Generates string dictionary.
	 * 
	 * @param columns	columns to compress
	 * @return	newly generated string dictionary
	 * @throws Exception
	 */
	static Dictionary createDictionary(
			List<ColumnRef> columns) throws Exception {
		// Ordered set of strings
		TreeSet<String> strings = new TreeSet<String>();
		// Iterate over columns to compress
		for (ColumnRef colRef : columns) {
			System.out.println("Encoding " + colRef.toString());
			StringData rawData = (StringData)BufferManager.getData(colRef);
			for (String string : rawData.data) {
				strings.add(string);
			}			
		}
		return new Dictionary(strings);
	}
	/**
	 * Iterate over all string columns in database and replace
	 * original column by compressed version (replacing strings
	 * by codes from the dictionary).
	 * 
	 * @param columns		columns to compress
	 * @param dictionary	dictionary to use for string encoding
	 * @throws Exception
	 */
	static void compressColumns(List<ColumnRef> columns, 
			Dictionary dictionary) throws Exception {
		// Iterate over columns to compress
		columns.stream().parallel().forEach(colRef -> {
			try {
				// Update data on disk
				ColumnInfo colInfo = CatalogManager.getColumn(colRef);
				StringData stringData = (StringData)BufferManager.getData(colRef);
				IntData codedData = compressData(stringData, dictionary);
				String dataPath = PathUtil.colToPath.get(colInfo);
				codedData.store(dataPath);
			} catch (Exception e) {
				e.printStackTrace();
			}			
		});
	}
	/**
	 * Generate meta-data for compressed column based on original.
	 * 
	 * @param stringColumn	meta-data for string column
	 * @return				meta-data of compressed column
	 */
	static ColumnInfo compressedColumn(ColumnInfo stringColumn) {
		String columnName = stringColumn.name;
		return new ColumnInfo(columnName, SQLtype.STRING_CODE, 
				stringColumn.isPrimary, stringColumn.isUnique,
				stringColumn.isNotNull, stringColumn.isForeign);
	}
	/**
	 * Generates compressed version for input string column.
	 * 
	 * @param stringData	content of string column
	 * @param dictionary	associates strings with codes
	 * @return				compressed column
	 */
	static IntData compressData(StringData stringData, Dictionary dictionary) {
		int cardinality = stringData.getCardinality();
		IntData codedData = new IntData(cardinality);
		// Copy data (encoded)
		for (int i=0; i<cardinality; ++i) {
			String string = stringData.data[i];
			int code = dictionary.getCode(string);
			codedData.data[i] = code;
		}
		// Copy null flags
		for (int i=stringData.isNull.nextSetBit(0); i!=-1; 
				i=stringData.isNull.nextSetBit(i+1)) {
			codedData.isNull.set(i);
		}
		return codedData;
	}
	/**
	 * Update database catalog after compression.
	 * 
	 * @param columns	columns that were compressed
	 */
	static void updateCatalog(List<ColumnRef> columns) throws Exception {
		// Iterate over columns that were compressed
		for (ColumnRef colRef : columns) {
			// Clear buffer manager
			BufferManager.unloadColumn(colRef);
			// Update catalog
			String tableName = colRef.aliasName;
			String colName = colRef.columnName;
			TableInfo tableInfo = CatalogManager.currentDB.
					nameToTable.get(tableName);
			ColumnInfo colInfo = tableInfo.nameToCol.get(colName);
			ColumnInfo compressedInfo = compressedColumn(colInfo);
			tableInfo.nameToCol.put(colName, compressedInfo);
		}
		// Mark database as compressed
		CatalogManager.currentDB.compressed = true;
		// Store updated catalog on disk
		CatalogManager.currentDB.storeDB();
	}
}
