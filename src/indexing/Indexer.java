package indexing;

import buffer.BufferManager;
import catalog.CatalogManager;
import config.GeneralConfig;
import config.IndexingMode;
import data.ColumnData;
import data.IntData;
import diskio.PathUtil;
import preprocessing.Preprocessor;
import query.ColumnRef;
import query.QueryInfo;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

/**
 * Features utility functions for creating indexes.
 * 
 * @author immanueltrummer
 *
 */
public class Indexer {
	/**
	 * Create an index on the specified column.
	 * 
	 * @param colRef	create index on this column
	 */
	public static void index(ColumnRef colRef) throws Exception {
		// Check if index already exists
		if (!BufferManager.colToIndex.containsKey(colRef)) {
			ColumnData data = BufferManager.getData(colRef);
			if (data instanceof IntData) {
				IntData intData = (IntData)data;
				IntIndex index = new IntIndex(colRef, intData);
				BufferManager.colToIndex.put(colRef, index);
			}
		}
	}

	public static void index(ColumnRef colRef, int tid) throws Exception {
		index(colRef);
		IntIndex index = (IntIndex) BufferManager.colToIndex.get(colRef);
		index.tid = tid;
	}
	/**
	 * Creates an index for each key/foreign key column.
	 * 
	 * @param mode	determines on which columns to create indices
	 * @throws Exception
	 */
	public static void indexAll(IndexingMode mode) throws Exception {
		System.out.println("Indexing all key columns ...");
		long startMillis = System.currentTimeMillis();
		// delete temporary data
		Path indexPath = Paths.get(PathUtil.indexPath);
		if (Files.exists(indexPath)) {
			Files.walk(indexPath)
					.sorted(Comparator.reverseOrder())
					.map(Path::toFile)
					.forEach(File::delete);
		}
		Files.createDirectory(indexPath);

		// create temporary files for non-volatile data
		CatalogManager.currentDB.nameToTable.values().parallelStream().forEach(
			tableInfo -> {
				tableInfo.nameToCol.values().parallelStream().forEach(
					columnInfo -> {
						try {
							if (mode.equals(IndexingMode.ALL) ||
								(mode.equals(IndexingMode.ONLY_KEYS) &&
							(columnInfo.isPrimary || columnInfo.isForeign))) {
								String table = tableInfo.name;
								String column = columnInfo.name;
								ColumnRef colRef = new ColumnRef(table, column);
								System.out.println("Indexing " + colRef + " ...");
								index(colRef);
							}
						} catch (Exception e) {
							System.err.println("Error indexing " + columnInfo);
							e.printStackTrace();
						}
					}
				);
			}
		);
		long totalMillis = System.currentTimeMillis() - startMillis;
		System.out.println("Indexing took " + totalMillis + " ms.");
	}
}
