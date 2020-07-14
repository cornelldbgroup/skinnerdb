package indexing;

import buffer.BufferManager;
import catalog.CatalogManager;
import config.IndexingMode;
import data.ColumnData;
import data.DoubleData;
import data.IntData;
import query.ColumnRef;

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
	public static Index index(ColumnRef colRef) throws Exception {
		Index index = null;
		// Check if index already exists
		if (!BufferManager.colToIndex.containsKey(colRef)) {
			ColumnData data = BufferManager.getData(colRef);
			if (data instanceof IntData) {
				IntData intData = (IntData)data;
				// Use specialized index for columns without duplicates
				if (CatalogManager.getColumn(colRef).isUnique) {
					index = new UniqueIntIndex(intData);
				} else {
					index = new DefaultIntIndex(intData);
				}
			} else if (data instanceof DoubleData) {
				DoubleData doubleData = (DoubleData)data;
				index = new DoubleIndex(doubleData);
			}
			BufferManager.colToIndex.put(colRef, index);
		}
		return index;
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
								Index index = index(colRef);
								if (index != null) {
									index.groupRows();
								}
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
