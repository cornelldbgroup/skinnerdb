package buffer;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import catalog.CatalogManager;
import catalog.info.ColumnInfo;
import catalog.info.TableInfo;
import config.LoggingConfig;
import config.StartupConfig;
import data.ColumnData;
import data.Dictionary;
import data.DoubleData;
import data.IntData;
import data.LongData;
import data.StringData;
import diskio.DiskUtil;
import diskio.PathUtil;
import indexing.Index;
import joining.parallel.indexing.DoublePartitionIndex;
import joining.parallel.indexing.IntPartitionIndex;
import query.ColumnRef;
import types.JavaType;
import types.TypeUtil;

/**
 * Manages the main memory database buffer.
 * 
 * @author Anonymous
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
	 * columns by joining.parallel processing threads.
	 */
	public final static Map<ColumnRef, ColumnData> colToData =
			new ConcurrentHashMap<ColumnRef, ColumnData>();
	/**
	 * Maps column references to associated indices.
	 */
	public final static Map<ColumnRef, Index> colToIndex =
			new ConcurrentHashMap<ColumnRef, Index>();
	/**
	 * Filtering cache used in pre-processing.
	 */
	public final static Map<Integer, List<Integer>> indexCache =
			new ConcurrentHashMap<>();
	/**
	 * Maps predicate string to associated id.
	 */
	public final static Map<String, Integer> predicateToID =
			new HashMap<>();
	/**
	 * Previous query name.
	 */
	public static String prevQuery;

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
		// Collect columns to load in joining.parallel
		List<ColumnRef> colsToLoad = new ArrayList<ColumnRef>();
		for (TableInfo table : CatalogManager.currentDB.nameToTable.values()) {
			String tableName = table.name;
			for (ColumnInfo column : table.nameToCol.values()) {
				String columnName = column.name;
				colsToLoad.add(new ColumnRef(tableName, columnName));
			}
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
	}
	/**
	 * Unload all columns of temporary tables (typically after
	 * query processing is finished).
	 * 
	 * @param except	names of tables to keep in each case
	 * @throws Exception
	 */
	public static void unloadTempData(Set<String> except) throws Exception {
		for (TableInfo table : CatalogManager.currentDB.nameToTable.values()) {
			if (table.tempTable && !except.contains(table.name)) {
				String tableName = table.name;
				for (ColumnInfo colInfo : table.nameToCol.values()) {
					ColumnRef colRef = new ColumnRef(
							tableName, colInfo.name);
					unloadColumn(colRef);
				}
			}
		}
	}
	/**
	 * Unload all cache rows of temporary tables (typically after
	 * a group of query processing is finished).
	 *
	 * @param queryName		the name of query group.
	 */
	public static void unloadCache(String queryName) {
		if (prevQuery == null) {
			prevQuery = queryName;
		}
		else if (!queryName.equals(prevQuery)) {
			prevQuery = queryName;
			indexCache.clear();
			predicateToID.clear();
			System.out.println("Clear the cache!");
		}
	}
	/**
	/**
	 * Unload all columns of temporary tables (typically after
	 * query processing is finished).
	 * 
	 * @throws Exception
	 */
	public static void unloadTempData() throws Exception {
		unloadTempData(new HashSet<>());
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

	public static long getTempDataSize(Set<String> except) {
		int size = 0;
		for (TableInfo table : CatalogManager.currentDB.nameToTable.values()) {
			if (table.tempTable && !except.contains(table.name)) {
				String tableName = table.name;
				for (ColumnInfo colInfo : table.nameToCol.values()) {
					ColumnRef colRef = new ColumnRef(
							tableName, colInfo.name);
					Index index = colToIndex.get(colRef);
					if (index instanceof DoublePartitionIndex) {
						int positions = index.positions == null ? 0 : index.positions.length * 4;
						size += index.cardinality * 8 + positions +
								((DoublePartitionIndex) index).keyToPositions.size() * 12;
					}
					else if (index instanceof IntPartitionIndex) {
						int positions = index.positions == null ? 0 : index.positions.length * 4;
						size += index.cardinality * 4 + positions +
								((IntPartitionIndex) index).keyToPositions.size() * 8;
					}
				}
			}
		}
		return size;
	}
}
