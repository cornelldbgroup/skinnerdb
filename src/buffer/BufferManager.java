package buffer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import catalog.CatalogManager;
import catalog.info.ColumnInfo;
import catalog.info.TableInfo;
import config.BufferConfig;
import config.GeneralConfig;
import config.LoggingConfig;
import data.ColumnData;
import data.Dictionary;
import data.DoubleData;
import data.IntData;
import data.LongData;
import data.StringData;
import diskio.DiskUtil;
import diskio.PathUtil;
import indexing.Index;
import indexing.IntIndex;
import query.ColumnRef;
import types.JavaType;
import types.TypeUtil;

/**
 * Manages the main memory database buffer.
 * 
 * @author immanueltrummer
 *
 */
public class BufferManager {
	/**
	 * Dictionary for interpreting compressed columns.
	 */
	public static Dictionary dictionary;
	/**
	 * Maps table and column names to corresponding data.
	 * Using a thread-safe data structure allows inserting
	 * columns by parallel processing threads.
	 */
	public final static Map<ColumnRef, ColumnData> colToData =
			new ConcurrentHashMap<ColumnRef, ColumnData>();
	/**
	 * Maps column references to associated indices.
	 */
	public final static Map<ColumnRef, Index> colToIndex =
			new ConcurrentHashMap<ColumnRef, Index>();
	/**
	 * Maps column references to associated IDs.
	 */
	public final static Map<ColumnRef, Integer> colToID = new HashMap<>();
	/**
	 * Maps column IDs to associated indices.
	 */
	public final static Map<Integer, Index> idToIndex =
			new ConcurrentHashMap<>();
	/**
	 * Implementation of buffer management algorithm.
	 */
	public final static IDataManager manager =
			new LRUDataManager();

	/**
	 * Loads dictionary from hard disk.
	 */
	public static void loadDictionary() throws Exception {
		// Check whether database has associated dictionary
		if (CatalogManager.currentDB.compressed) {
			System.out.println("Loading dictionary ...");
			long startMillis = System.currentTimeMillis();
			String dictionaryPath = PathUtil.dictionaryPath;
			Object object = DiskUtil.loadObject(dictionaryPath);
			dictionary = (Dictionary)object;
			long totalMillis = System.currentTimeMillis() - startMillis;
			System.out.println("Loaded dictionary in " + totalMillis + " ms.");	
			// Generate debugging output
			log("*** String dictionary sample ***");
			int sampleSize = Math.min(10, dictionary.strings.length);
			for (int i=0; i<sampleSize; ++i) {
				log(i + "\t" + dictionary.getString(i));
			}
			log("******");
		} else {
			System.out.println("No data dictionary found.");
		}
	}
	/**
	 * Loads data for current database into main memory.
	 * 
	 * @throws Exception
	 */
	public static void loadDB() throws Exception {
		// Remove previously loaded data
		colToData.clear();
		// Load dictionary from disk
		loadDictionary();
		// Collect columns to load in parallel
		List<ColumnRef> colsToLoad = new ArrayList<ColumnRef>();
		for (TableInfo table : CatalogManager.currentDB.nameToTable.values()) {
			String tableName = table.name;
			for (ColumnInfo column : table.nameToCol.values()) {
				String columnName = column.name;
				colsToLoad.add(new ColumnRef(tableName, columnName));
			}
		}
		int id = BufferManager.colToID.values()
				.stream()
				.mapToInt(v -> v)
				.max().orElse(0) + 1;
		for (ColumnRef columnRef: colsToLoad) {
			BufferManager.colToID.putIfAbsent(columnRef, id++);
		}
		// Load columns
		colsToLoad.stream().parallel().forEach((colRef) -> {
			try {
				System.out.println("Loading column " + colRef.toString());
				loadColumn(colRef);				
			} catch (Exception e) {
				System.err.println("Error loading column " + colRef.toString());
				e.printStackTrace();
			}
		});
		System.out.println("Loaded database.");
	}
	/**
	 * Loads data for specified column from hard disk.
	 * 
	 * @param columnRef	reference to column to load
	 * 
	 * @throws Exception
	 */
	public static void loadColumn(ColumnRef columnRef) throws Exception {
		// Check whether column is already loaded
		if (!colToData.containsKey(columnRef)) {
			long startMillis = System.currentTimeMillis();
			// Get column information from catalog
			ColumnInfo column = CatalogManager.getColumn(columnRef);
			log("Loaded column meta-data: " + column.toString());
			// Read generic object from file
			String dataPath = PathUtil.colToPath.get(column);
			Object object = DiskUtil.loadObject(dataPath);	
			// Cast object according to column type
			JavaType javaType = TypeUtil.toJavaType(column.type);
			log("Column data type:\t" + javaType);
			switch (javaType) {
			case INT:
				colToData.put(columnRef, (IntData)object);
				break;
			case LONG:
				colToData.put(columnRef, (LongData)object);
				break;
			case DOUBLE:
				colToData.put(columnRef, (DoubleData)object);
				break;
			case STRING:
				colToData.put(columnRef, (StringData)object);
				break;
			}
			// Generate statistics for output
			if (LoggingConfig.BUFFER_VERBOSE) {
				long totalMillis = System.currentTimeMillis() - startMillis;
				System.out.println("Loaded " + columnRef.toString() + 
						" in " + totalMillis + " milliseconds");
			}
			// Generate debugging output
			log("*** Column " + columnRef.toString() + " sample ***");
			int cardinality = colToData.get(columnRef).getCardinality();
			int sampleSize = Math.min(10, cardinality);
			for (int i=0; i<sampleSize; ++i) {
				switch (column.type) {
				case STRING_CODE:
					int code = ((IntData)object).data[i];
					log(dictionary.getString(code));
					break;
				}
			}
			log("******");
		}
	}
	/**
	 * Returns data of specified column, loads data from disk if
	 * currently not loaded.
	 * 
	 * @param columnRef	request data for this column
	 * @return			data of requested column
	 * @throws Exception
	 */
	public static ColumnData getData(ColumnRef columnRef) throws Exception {
		// Load data if necessary
		if (!colToData.containsKey(columnRef)) {
			loadColumn(columnRef);
		}
		return colToData.get(columnRef);
	}

	/**
	 * Returns data of specified column, loads data from disk if
	 * currently not loaded.
	 *
	 * @param columnRef	request data for this column
	 * @return			data of requested column
	 * @throws Exception
	 */
	public static ColumnData getManagerData(ColumnRef columnRef) {
		// Load data by data manager
		try {
			// Get column information from catalog
			ColumnInfo column = CatalogManager.getColumn(columnRef);
			// Read generic object from file
			String dataPath = PathUtil.colToPath.get(column);
			if (dataPath == null) {
				return BufferManager.colToData.get(columnRef);
			}
			else {
				return manager.getData(columnRef);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * Returns data of specified column, loads data from disk if
	 * currently not loaded.
	 *
	 * @param intIndex	request index for this column
	 * @return			data of requested column
	 * @throws Exception
	 */
	public static int getIndexData(IntIndex intIndex, int pos) throws Exception {
		// Load data by data manager
		if (GeneralConfig.indexInMemory) {
			return intIndex.positions[pos];
		}
		else {
			if (BufferConfig.loadPage) {
				return manager.getDataInWindow(intIndex, pos);
			}
			else {
				return manager.getIndexData(intIndex, pos);
			}
		}
	}

	/**
	 * Get the data of given column and row id.
	 *
	 * @param column	the id of column.
	 * @param rid		the id of row
	 * @return			the value located in specified column and row.
	 * @throws Exception
	 */
	public static int getRowData(int column, int rid) throws Exception {
		if (idToIndex.containsKey(column)) {
			IntIndex intIndex = (IntIndex) idToIndex.get(column);
		}
		return 0;
	}

	/**
	 * Store given column into buffer space.
	 *
	 * @param columnRef	reference to column to remove
	 * @throws Exception
	 */
	public static void storeColumn(ColumnRef columnRef, ColumnData columnData) {
		if (LoggingConfig.BUFFER_VERBOSE) {
			System.out.println("Storing column " + columnRef);
		}
		BufferManager.colToData.put(columnRef, columnData);
	}


	/**
	 * Remove given column from buffer space.
	 * 
	 * @param columnRef	reference to column to remove
	 * @throws Exception
	 */
	public static void unloadColumn(ColumnRef columnRef) throws Exception {
		if (LoggingConfig.BUFFER_VERBOSE) {
			System.out.println("Unloading column " + columnRef);
		}
		colToData.remove(columnRef);
		colToIndex.remove(columnRef);
		colToID.remove(columnRef);
	}
	/**
	 * Unload all columns of temporary tables (typically after
	 * query processing is finished).
	 * 
	 * @throws Exception
	 */
	public static void unloadTempData() throws Exception {
		for (TableInfo table : CatalogManager.currentDB.nameToTable.values()) {
			if (table.tempTable) {
				String tableName = table.name;
				for (ColumnInfo colInfo : table.nameToCol.values()) {					
					ColumnRef colRef = new ColumnRef(
							tableName, colInfo.name);
					// close file channels
					if (colToIndex.containsKey(colRef)) {
						// close file channels
						if (!GeneralConfig.indexInMemory) {
							((IntIndex)colToIndex.get(colRef)).closeChannels();
						}
					}
					unloadColumn(colRef);
				}
			}
		}
		if (!GeneralConfig.indexInMemory) {
			manager.clearCache();
		}
	}
	/**
	 * Log given text if buffer logging activated.
	 * 
	 * @param text	text to output
	 */
	static void log(String text) {
		if (LoggingConfig.BUFFER_VERBOSE) {
			System.out.println(text);
		}
	}
}
