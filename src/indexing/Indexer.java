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
import query.QueryInfo;
import types.SQLtype;

import java.util.Arrays;
import java.util.Deque;

/**
 * Features utility functions for creating indexes.
 * 
 * @author Anonymous
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
				IntIndex index = new IntIndex(intData);
				BufferManager.colToIndex.put(colRef, index);
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
		int nrThreads;
		switch (ParallelConfig.PARALLEL_SPEC) {
			case 18: {
				nrThreads = Math.max(1, ParallelConfig.EXE_THREADS - ParallelConfig.SEARCH_THREADS);
				break;
			}
			case 0: {
				nrThreads = Math.max(1, ParallelConfig.EXE_THREADS - 1);
				break;
			}
			case 20: {
				nrThreads = 24;
				break;
			}
			default: {
				nrThreads = ParallelConfig.EXE_THREADS;
				break;
			}
		}
		if (!BufferManager.colToIndex.containsKey(colRef)) {
			ColumnData data = BufferManager.getData(colRef);
			if (data instanceof IntData) {
				IntData intData = (IntData)data;
				IntPartitionIndex intIndex = oldIndex == null ? null : (IntPartitionIndex) oldIndex;
				int keySize = intIndex == null ? 0 : intIndex.keyToPositions.size();
				IndexPolicy policy = Indexer.indexPolicy(isPrimary, isSeq, keySize, intData.cardinality);
				IntPartitionIndex index = new IntPartitionIndex(intData, nrThreads, colRef, queryRef,
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
				DoublePartitionIndex index = new DoublePartitionIndex(doubleData, nrThreads,
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
	 * Create an index on the specified column.
	 *
	 * @param colRef	create index on this column
	 */
	public static Index partitionIndex(Deque<int[]> positionQueue, ColumnRef originalRef,
									   ColumnRef colRef, ColumnRef queryRef,
									   PartitionIndex oldIndex,
									   boolean isPrimary) throws Exception {
		// Check if index already exists
		int nrThreads;
		switch (ParallelConfig.PARALLEL_SPEC) {
			case 18: {
				nrThreads = Math.max(1, ParallelConfig.EXE_THREADS - ParallelConfig.SEARCH_THREADS);
				break;
			}
			case 0: {
				nrThreads = Math.max(1, ParallelConfig.EXE_THREADS - 1);
				break;
			}
			case 20: {
				nrThreads = 24;
				break;
			}
			default: {
				nrThreads = ParallelConfig.EXE_THREADS;
				break;
			}
		}
		if (!BufferManager.colToIndex.containsKey(colRef)) {
			ColumnData data = BufferManager.getData(colRef);
			if (data instanceof IntData) {
				IntData intData = (IntData)data;
				IntPartitionIndex intIndex = oldIndex == null ? null : (IntPartitionIndex) oldIndex;
				int keySize = intIndex == null ? 0 : intIndex.keyToPositions.size();
				IndexPolicy policy = Indexer.indexPolicy(isPrimary, false, keySize, intData.cardinality);
				int[] positions = null;
				// Need a large array
				if (intIndex != null && (policy == IndexPolicy.Sparse && !intIndex.unique ||
						(intIndex.unique &&
								intData.cardinality >= ParallelConfig.LARGE_KEY_SIZE))) {
					// Sparse
					if (policy == IndexPolicy.Sparse && !intIndex.unique) {
						positions = positionQueue.pop();
					}
					// Large key
					else {
						Deque<int[]> largeKeys = BufferManager.aliasToPositions.get(originalRef);
						if (largeKeys.isEmpty()) {
							positions = new int[intIndex.max + 1];
						}
						else {
							positions = largeKeys.pop();
						}
					}
				}
				IntPartitionIndex index = new IntPartitionIndex(intData, nrThreads, colRef, queryRef,
						intIndex, policy, positions);
				BufferManager.colToIndex.put(colRef, index);
				return index;
			} else if (data instanceof DoubleData) {
				DoubleData doubleData = (DoubleData)data;
				DoublePartitionIndex doubleIndex = oldIndex == null ? null : (DoublePartitionIndex) oldIndex;
				int keySize = doubleIndex == null ? 0 : doubleIndex.keyToPositions.size();
				IndexPolicy policy = Indexer.indexPolicy(isPrimary, false, keySize, doubleData.cardinality);
				DoublePartitionIndex index = new DoublePartitionIndex(doubleData, nrThreads,
						colRef, queryRef, doubleIndex, policy);
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
		String[] unindexedColumns = new String[]{
				"l_comment", "p_comment", "n_comment", "r_comment", "ps_comment",
				"o_clerk", "p_retailprice", "l_linenumber", "o_comment"
		};
//		String[] indexedColumns = new String[] {
//				"p_partkey", "l_partkey",
//				"s_suppkey", "l_suppkey",
//				"l_orderkey", "o_orderkey",
//				"o_custkey", "c_custkey",
//				"c_nationkey",  "n_nationkey",
//				"n_regionkey", "r_regionkey",
//				"r_name",
//				"s_nationkey",
//				"o_orderdate",
//				"p_type"
//		};
		long startMillis = System.currentTimeMillis();
		CatalogManager.currentDB.nameToTable.values().parallelStream().forEach(
			tableInfo -> {
				tableInfo.nameToCol.values().parallelStream().forEach(
					columnInfo -> {
						try {
							if (!Arrays.asList(unindexedColumns).contains(columnInfo.name)
									&& (mode.equals(IndexingMode.ALL) ||
								(mode.equals(IndexingMode.ONLY_KEYS) &&
							(columnInfo.isPrimary || columnInfo.isForeign)))) {
								String table = tableInfo.name;
								String column = columnInfo.name;
								ColumnRef colRef = new ColumnRef(table, column);
								System.out.println("Indexing " + colRef + " ...");
								boolean sorted = columnInfo.type == SQLtype.DATE;
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
		else if (cardinality <= ParallelConfig.PARALLEL_SIZE || isSeq || keySize == 0) {
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
