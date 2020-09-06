package indexing;

import buffer.BufferManager;
import catalog.CatalogManager;
import catalog.info.ColumnInfo;
import config.GeneralConfig;
import config.IndexingMode;
import config.ParallelConfig;
import data.ColumnData;
import data.DoubleData;
import data.IntData;
import joining.parallel.indexing.DoublePartitionIndex;
import joining.parallel.indexing.IndexPolicy;
import joining.parallel.indexing.IntPartitionIndex;
import joining.parallel.indexing.PartitionIndex;
import query.ColumnRef;
import types.SQLtype;

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
	public static void index(ColumnRef colRef, boolean sorted) throws Exception {
		// Check if index already exists
		if (!BufferManager.colToIndex.containsKey(colRef)) {
			ColumnData data = BufferManager.getData(colRef);
			if (data instanceof IntData) {
				IntData intData = (IntData)data;
				IntIndex index = new IntIndex(intData);
				BufferManager.colToIndex.put(colRef, index);
				if (sorted) {
					index.sortRows();
				}
			} else if (data instanceof DoubleData) {
				DoubleData doubleData = (DoubleData)data;
				DoubleIndex index = new DoubleIndex(doubleData);
				BufferManager.colToIndex.put(colRef, index);
			}
		}
	}

	/**
	 * Create an index on the specified column.
	 *
	 * @param colRef	create index on this column
	 */
	public static Index partitionIndex(ColumnRef colRef, ColumnRef queryRef, PartitionIndex oldIndex,
									  boolean isPrimary, boolean isSeq, boolean sorted) throws Exception {
		// Check if index already exists
		if (!BufferManager.colToIndex.containsKey(colRef)) {
			ColumnData data = BufferManager.getData(colRef);
			if (data instanceof IntData) {
				IntData intData = (IntData)data;
				IntPartitionIndex intIndex = oldIndex == null ? null : (IntPartitionIndex) oldIndex;
				int keySize = intIndex == null ? 0 : intIndex.keyToPositions.size();
				IndexPolicy policy = Indexer.indexPolicy(isPrimary, isSeq, keySize, intData.cardinality);
				IntPartitionIndex index = new IntPartitionIndex(intData, ParallelConfig.EXE_THREADS, colRef, queryRef,
						intIndex, policy);
				if (sorted) {
					index.sortRows();
				}
				BufferManager.colToIndex.put(colRef, index);
				return index;
			} else if (data instanceof DoubleData) {
				DoubleData doubleData = (DoubleData)data;
				DoublePartitionIndex doubleIndex = oldIndex == null ? null : (DoublePartitionIndex) oldIndex;
				int keySize = doubleIndex == null ? 0 : doubleIndex.keyToPositions.size();
				IndexPolicy policy = Indexer.indexPolicy(isPrimary, isSeq, keySize, doubleData.cardinality);
				DoublePartitionIndex index = new DoublePartitionIndex(doubleData, ParallelConfig.EXE_THREADS,
						colRef, queryRef, doubleIndex, policy);
				if (sorted) {
					index.sortRows();
				}
				BufferManager.colToIndex.put(colRef, index);
				return index;
			}
		}
		return null;
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
								boolean sorted = columnInfo.type == SQLtype.DATE;
//								if (GeneralConfig.isParallel) {
//									partitionIndex(colRef, colRef, null,
//											columnInfo.isPrimary, true, sorted);
//								}
//								else {
//									index(colRef, sorted);
//								}
								partitionIndex(colRef, colRef, null,
										columnInfo.isPrimary, true, sorted);
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

	public static IndexPolicy indexPolicy(boolean isPrimary, boolean isSeq, int keySize, int cardinality) {
		IndexPolicy policy;
		if (isPrimary) {
			policy = IndexPolicy.Key;
		}
		else if (cardinality <= ParallelConfig.PARALLEL_SIZE || isSeq) {
			policy = IndexPolicy.Sequential;
		}
		else if (keySize >= ParallelConfig.SPARSE_KEY_SIZE) {
			policy = IndexPolicy.Sparse;
		}
		else {
			policy = IndexPolicy.Dense;
		}
		return policy;
	}
}
