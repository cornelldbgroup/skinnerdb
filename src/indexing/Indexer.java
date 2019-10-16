package indexing;

import buffer.BufferManager;
import catalog.CatalogManager;
import config.IndexingMode;
import config.ParallelConfig;
import data.ColumnData;
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
	public static void index(ColumnRef colRef, ColumnRef queryRef, Index oldIndex, boolean isPrimeKey) throws Exception {
		// Check if index already exists
		if (!BufferManager.colToIndex.containsKey(colRef)) {
			ColumnData data = BufferManager.getData(colRef);
			ThreadIntIndex intIndex = oldIndex == null ? null : (ThreadIntIndex) oldIndex;
			if (data instanceof IntData) {
				IntData intData = (IntData)data;
//				IntIndex old_index = new IntIndex(intData);
				long timer0 = System.currentTimeMillis();

				IndexPolicy policy;
				if (isPrimeKey) {
					policy = IndexPolicy.Key;
				}
				else if (!ParallelConfig.PARALLEL_PRE) {
					policy = IndexPolicy.Sequential;
				}
				else if (intIndex != null && intIndex.keyToPositions.size() >= 10000) {
					policy = IndexPolicy.Sparse;
				}
				else {
					policy = IndexPolicy.Dense;
				}
				ThreadIntIndex index = new ThreadIntIndex(intData, ParallelConfig.EXE_THREADS, colRef, queryRef, intIndex, policy);

				long timer1 = System.currentTimeMillis();
				long totalMills = timer1 - timer0;
				String results = totalMills + "-" + index.cardinality + "-" + index.keyToPositions.size() + "-" + index.intData.isNull.cardinality();
				System.out.println(colRef + ": " + results + "\tpolicy: " + policy);
				BufferManager.colToIndex.put(colRef, index);
			}					
		}
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
								index(colRef, colRef, null, columnInfo.isPrimary);
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
