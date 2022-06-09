package operators.parallel;

import buffer.BufferManager;
import catalog.CatalogManager;
import catalog.info.ColumnInfo;
import catalog.info.TableInfo;
import com.google.common.primitives.Ints;
import config.GeneralConfig;
import config.NamingConfig;
import config.ParallelConfig;
import data.ColumnData;
import data.DoubleData;
import data.IntData;
import expressions.ExpressionInfo;
import indexing.Index;
import indexing.Indexer;
import joining.parallel.indexing.DoublePartitionIndex;
import joining.parallel.indexing.IndexPolicy;
import joining.parallel.indexing.IntPartitionIndex;
import joining.result.ResultTuple;
import joining.result.UniqueJoinResult;
import operators.RowRange;
import preprocessing.Context;
import print.RelationPrinter;
import query.ColumnRef;
import query.QueryInfo;
import types.JavaType;
import types.TypeUtil;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.stream.Stream;

/**
 * Materializes parts of a table, defined by
 * a subset of columns and a subset of rows.
 * 
 * @author Anonymous
 *
 */
public class Materialize {
	/**
	 * Creates a temporary table with given name and copies into it
	 * values at given row indices and for given columns in source
	 * table.
	 * 
	 * @param sourceRelName	name of source table to copy from
	 * @param columnNames	names of columns to be copied
	 * @param rowList		list of row indices to copy, can be null
	 * @param targetRelName	name of target table
	 * @param tempResult	whether to create temporary result relation
	 * @throws Exception
	 */
	public static List<List<RecursiveAction>> execute(String sourceRelName, List<String> columnNames,
												Set<ColumnRef> requiredIndexedCols,
												List<Integer> rowList, String targetRelName,
												boolean tempResult) throws Exception {
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
		List<List<RecursiveAction>> actionTasks = new ArrayList<>();
		// Generate references to source columns
		List<ColumnRef> sourceColRefs = new ArrayList<>();
		for (String columnName : columnNames) {
			sourceColRefs.add(new ColumnRef(sourceRelName, columnName));
		}
		// Update catalog, inserting materialized table
		TableInfo resultTable = new TableInfo(targetRelName, tempResult);
		CatalogManager.currentDB.addTable(resultTable);
		for (ColumnRef sourceColRef : sourceColRefs) {
			// Add result column to result table, using type of source column
			ColumnInfo sourceCol = CatalogManager.getColumn(sourceColRef);
			ColumnInfo resultCol = new ColumnInfo(sourceColRef.columnName, 
					sourceCol.type, sourceCol.isPrimary, 
					sourceCol.isUnique, sourceCol.isNotNull, 
					sourceCol.isForeign);
			resultTable.addColumn(resultCol);
		}		
		// Load source data if necessary
		if (!GeneralConfig.inMemory) {
			for (ColumnRef sourceColRef : sourceColRefs) {
				BufferManager.loadColumn(sourceColRef);
			}
		}
		int cardinality = rowList.size();
		List<RowRange> batches = split(cardinality);
		// Generate column data
		int nrBatches = batches.size();
		List<RecursiveAction> subTasks = new ArrayList<>(nrBatches);
		for (ColumnRef sourceColRef: sourceColRefs) {
			// Copy relevant rows into result column
			ColumnData srcData = BufferManager.colToData.get(sourceColRef);
			String columnName = sourceColRef.columnName;
			ColumnRef resultColRef = new ColumnRef(targetRelName, columnName);
			ColumnInfo sourceCol = CatalogManager.getColumn(sourceColRef);
			JavaType jType = TypeUtil.toJavaType(sourceCol.type);
			switch (jType) {
				case INT: {
					IntPartitionIndex index = null;
					int[] newPositions = null;
					int keySize = 0;
					if (requiredIndexedCols.contains(sourceColRef)) {
						index = (IntPartitionIndex) BufferManager.colToIndex.get(sourceColRef);
						keySize = index.sorted ? 0 : index.keyToPositions.size();
					}
					if (keySize >= ParallelConfig.SPARSE_KEY_SIZE) {
						newPositions = new int[index.positions.length];
					}
					IntData intTargetData = new IntData(cardinality);
					IntData intSourceData = (IntData) srcData;
					BufferManager.colToData.put(resultColRef, intTargetData);
					int[] target = intTargetData.data;
					int[] source = intSourceData.data;
					if (newPositions == null) {
						for (RowRange batch: batches) {
							subTasks.add(new RecursiveAction() {
								@Override
								protected void compute() {
									int batchFirst = batch.firstTuple;
									int batchLast = batch.lastTuple;
									for (int rowCtr = batchFirst; rowCtr <= batchLast; rowCtr++) {
										int row = rowList.get(rowCtr);
										// Treat special case: insertion of null values
										if (!intSourceData.isNull.get(row)) {
											target[rowCtr] = source[row];
										}
										else {
											target[rowCtr] = Integer.MIN_VALUE;
										}
									}
								}
							});
						}
					}
					else {
						IntPartitionIndex finalIndex = index;
						int[] finalNewPositions = newPositions;
						for (RowRange batch: batches) {
							subTasks.add(new RecursiveAction() {
								@Override
								protected void compute() {
									int batchFirst = batch.firstTuple;
									int batchLast = batch.lastTuple;
									for (int rowCtr = batchFirst; rowCtr <= batchLast; rowCtr++) {
										int row = rowList.get(rowCtr);
										int posCtr = finalIndex.rowToPositions[row];
										// Treat special case: insertion of null values
										if (!intSourceData.isNull.get(row)) {
											target[rowCtr] = source[row];
											// Set index masks
											finalNewPositions[posCtr] = rowCtr;
										}
										else {
											target[rowCtr] = Integer.MIN_VALUE;
										}
									}
								}
							});
						}
					}
					actionTasks.add(subTasks);
					// Early indexes creation for sparse ones
					if (newPositions != null) {
						IntPartitionIndex intPartitionIndex = new IntPartitionIndex(intTargetData, nrThreads,
								null, sourceColRef, index, newPositions);
						// Divide tuples into batches
						List<RowRange> indexBatches = intPartitionIndex.splitPositions(index);
						int nrIndexBatches = indexBatches.size();
						List<RecursiveAction> indexSubtasks = new ArrayList<>(nrIndexBatches);
						for (RowRange batch : batches) {
							int first = batch.firstTuple;
							int last = batch.lastTuple;
							indexSubtasks.add(new RecursiveAction() {
								@Override
								protected void compute() {
									int prevPos = -1;
									// Evaluate predicate for each table row
									for (int posCtr = first; posCtr <= last; ++posCtr) {
										int rowCtr = intPartitionIndex.positions[posCtr];
										if (intPartitionIndex.isKeys[posCtr]) {
											prevPos = posCtr;
										} else if (rowCtr != 0 &&
												!intTargetData.isNull.get(rowCtr)
												&& target[rowCtr] != Integer.MIN_VALUE) {
											int nr = intPartitionIndex.positions[prevPos];
											int pos = prevPos + 1 + nr;
											intPartitionIndex.positions[pos] = rowCtr;
											int startThread = nr % nrThreads;
											intPartitionIndex.scopes[rowCtr] = startThread;
											intPartitionIndex.positions[prevPos]++;
										}
									}
								}
							});
						}
						actionTasks.add(indexSubtasks);
					}
					break;
				}
				case DOUBLE: {
					DoublePartitionIndex index = null;
					int[] newPositions = null;
					int keySize = 0;
					if (requiredIndexedCols.contains(sourceColRef)) {
						index = (DoublePartitionIndex) BufferManager.colToIndex.get(sourceColRef);
						keySize = index.sorted ? 0 : index.keyToPositions.size();
					}
					if (keySize >= ParallelConfig.SPARSE_KEY_SIZE) {
						newPositions = new int[index.positions.length];
					}
					DoubleData doubleTargetData = new DoubleData(cardinality);
					DoubleData doubleSourceData = (DoubleData) srcData;
					BufferManager.colToData.put(resultColRef, doubleTargetData);
					double[] target = doubleTargetData.data;
					double[] source = doubleSourceData.data;
					if (newPositions == null) {
						for (RowRange batch: batches) {
							subTasks.add(new RecursiveAction() {
								@Override
								protected void compute() {
									int batchFirst = batch.firstTuple;
									int batchLast = batch.lastTuple;
									for (int rowCtr = batchFirst; rowCtr <= batchLast; rowCtr++) {
										int row = rowList.get(rowCtr);
										// Treat special case: insertion of null values
										if (!doubleSourceData.isNull.get(row)) {
											target[rowCtr] = source[row];
										}
										else {
											target[rowCtr] = Integer.MIN_VALUE;
										}
									}
								}
							});
						}
					}
					else {
						DoublePartitionIndex finalIndex = index;
						int[] finalNewPositions = newPositions;
						for (RowRange batch: batches) {
							subTasks.add(new RecursiveAction() {
								@Override
								protected void compute() {
									int batchFirst = batch.firstTuple;
									int batchLast = batch.lastTuple;
									for (int rowCtr = batchFirst; rowCtr <= batchLast; rowCtr++) {
										int row = rowList.get(rowCtr);
										int posCtr = finalIndex.rowToPositions[row];
										// Treat special case: insertion of null values
										if (!doubleSourceData.isNull.get(row)) {
											target[rowCtr] = source[row];
											// Set index masks
											finalNewPositions[posCtr] = rowCtr;
										}
										else {
											target[rowCtr] = Integer.MIN_VALUE;
										}
									}
								}
							});
						}
					}
					actionTasks.add(subTasks);
					break;
				}
			}


		}
		return actionTasks;
	}

	public static List<List<RecursiveAction>> executeRange(String sourceRelName, List<String> columnNames,
									Set<ColumnRef> requiredIndexedCols, int[] indexSortedRow,
									List<Integer> rowList, String targetRelName,
									boolean tempResult) throws Exception {
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
		List<List<RecursiveAction>> actionTasks = new ArrayList<>();
		// Generate references to source columns
		List<ColumnRef> sourceColRefs = new ArrayList<>();
		for (String columnName : columnNames) {
			sourceColRefs.add(new ColumnRef(sourceRelName, columnName));
		}
		int firstIdx = rowList.get(0);
		int lastIdx = rowList.get(1);
		int cardinality = lastIdx - firstIdx;
		// Update sort arrays
		int[] sortedRow = Arrays.copyOfRange(indexSortedRow, firstIdx, lastIdx);
		Arrays.sort(sortedRow);
		// Update catalog, inserting materialized table
		TableInfo resultTable = new TableInfo(targetRelName, tempResult);
		CatalogManager.currentDB.addTable(resultTable);
		// Split batches
		List<RowRange> batches = split(cardinality);
		int nrBatches = batches.size();
		List<RecursiveAction> subTasks = new ArrayList<>(nrBatches);
		for (ColumnRef sourceColRef : sourceColRefs) {
			// Add result column to result table, using type of source column
			ColumnInfo sourceCol = CatalogManager.getColumn(sourceColRef);
			ColumnInfo resultCol = new ColumnInfo(sourceColRef.columnName,
					sourceCol.type, sourceCol.isPrimary,
					sourceCol.isUnique, sourceCol.isNotNull,
					sourceCol.isForeign);
			resultTable.addColumn(resultCol);
		}
		// Load source data if necessary
		if (!GeneralConfig.inMemory) {
			for (ColumnRef sourceColRef : sourceColRefs) {
				BufferManager.loadColumn(sourceColRef);
			}
		}
		for (ColumnRef sourceColRef: sourceColRefs) {
			ColumnInfo sourceCol = CatalogManager.getColumn(sourceColRef);
			ColumnData srcData = BufferManager.colToData.get(sourceColRef);
			ColumnRef resultColRef = new ColumnRef(targetRelName, sourceColRef.columnName);
			JavaType jType = TypeUtil.toJavaType(sourceCol.type);
			switch (jType) {
				case INT: {
					IntPartitionIndex index = null;
					int[] newPositions = null;
					int keySize = 0;
					if (requiredIndexedCols.contains(sourceColRef)) {
						index = (IntPartitionIndex) BufferManager.colToIndex.get(sourceColRef);
						keySize = index.sorted ? 0 : index.keyToPositions.size();
					}
					if (keySize >= ParallelConfig.SPARSE_KEY_SIZE) {
						newPositions = new int[index.positions.length];
					}
					IntData intTargetData = new IntData(cardinality);
					IntData intSourceData = (IntData) srcData;
					BufferManager.colToData.put(resultColRef, intTargetData);
					int[] target = intTargetData.data;
					int[] source = intSourceData.data;
					if (newPositions == null) {
//						for (RowRange batch: batches) {
//							subTasks.add(new RecursiveAction() {
//								@Override
//								protected void compute() {
//									long batchStart = System.currentTimeMillis();
//									int batchFirst = batch.firstTuple;
//									int batchLast = batch.lastTuple;
//									for (int rowCtr = batchFirst; rowCtr <= batchLast; rowCtr++) {
//										int row = sortedRow[rowCtr];
//										// Treat special case: insertion of null values
//										target[rowCtr] = !intSourceData.isNull.get(row) ?
//												source[row] : Integer.MIN_VALUE;
//									}
//									long batchEnd = System.currentTimeMillis();
//									if (batchEnd - batchStart > 1000) {
//										System.out.println(batchFirst + ": " + (batchEnd - batchStart));
//									}
//								}
//							});
//						}
						batches.parallelStream().forEach(batch -> {
							long batchStart = System.currentTimeMillis();
							int batchFirst = batch.firstTuple;
							int batchLast = batch.lastTuple;
							for (int rowCtr = batchFirst; rowCtr <= batchLast; rowCtr++) {
								int row = sortedRow[rowCtr];
								// Treat special case: insertion of null values
								target[rowCtr] = !intSourceData.isNull.get(row) ?
										source[row] : Integer.MIN_VALUE;
							}
							long batchEnd = System.currentTimeMillis();
							if (batchEnd - batchStart > 1000) {
								System.out.println(batchFirst + ": " + (batchEnd - batchStart));
							}
						});
					}
					else {
						IntPartitionIndex finalIndex = index;
						int[] finalNewPositions = newPositions;
						for (RowRange batch: batches) {
							subTasks.add(new RecursiveAction() {
								@Override
								protected void compute() {
									int batchFirst = batch.firstTuple;
									int batchLast = batch.lastTuple;
									for (int rowCtr = batchFirst; rowCtr <= batchLast; rowCtr++) {
										int row = sortedRow[rowCtr];
										int posCtr = finalIndex.rowToPositions[row];
										// Treat special case: insertion of null values
										if (!intSourceData.isNull.get(row)) {
											target[rowCtr] = source[row];
											// Set index masks
											finalNewPositions[posCtr] = rowCtr;
										}
										else {
											target[rowCtr] = Integer.MIN_VALUE;
										}
									}
								}
							});
						}
					}
					actionTasks.add(subTasks);
					// Early indexes creation for sparse ones
					if (newPositions != null) {
						IntPartitionIndex intPartitionIndex = new IntPartitionIndex(intTargetData, nrThreads,
								null, sourceColRef, index, newPositions);
						// Divide tuples into batches
						List<RowRange> indexBatches = intPartitionIndex.splitPositions(index);
						int nrIndexBatches = indexBatches.size();
						List<RecursiveAction> indexSubtasks = new ArrayList<>(nrIndexBatches);
						for (RowRange batch : batches) {
							int first = batch.firstTuple;
							int last = batch.lastTuple;
							indexSubtasks.add(new RecursiveAction() {
								@Override
								protected void compute() {
									int prevPos = -1;
									// Evaluate predicate for each table row
									for (int posCtr = first; posCtr <= last; ++posCtr) {
										int rowCtr = intPartitionIndex.positions[posCtr];
										if (intPartitionIndex.isKeys[posCtr]) {
											prevPos = posCtr;
										} else if (rowCtr != 0 &&
												!intTargetData.isNull.get(rowCtr)
												&& target[rowCtr] != Integer.MIN_VALUE) {
											int nr = intPartitionIndex.positions[prevPos];
											int pos = prevPos + 1 + nr;
											intPartitionIndex.positions[pos] = rowCtr;
											int startThread = nr % nrThreads;
											intPartitionIndex.scopes[rowCtr] = startThread;
											intPartitionIndex.positions[prevPos]++;
										}
									}
								}
							});
						}
						actionTasks.add(indexSubtasks);
					}
					break;
				}
				case DOUBLE: {
					DoublePartitionIndex index = null;
					int[] newPositions = null;
					int keySize = 0;
					if (requiredIndexedCols.contains(sourceColRef)) {
						index = (DoublePartitionIndex) BufferManager.colToIndex.get(sourceColRef);
						keySize = index.sorted ? 0 : index.keyToPositions.size();
					}
					if (keySize >= ParallelConfig.SPARSE_KEY_SIZE) {
						newPositions = new int[index.positions.length];
					}
					DoubleData doubleTargetData = new DoubleData(cardinality);
					DoubleData doubleSourceData = (DoubleData) srcData;
					BufferManager.colToData.put(resultColRef, doubleTargetData);
					double[] target = doubleTargetData.data;
					double[] source = doubleSourceData.data;
					if (newPositions == null) {
						for (RowRange batch: batches) {
							subTasks.add(new RecursiveAction() {
								@Override
								protected void compute() {
									int batchFirst = batch.firstTuple;
									int batchLast = batch.lastTuple;
									for (int rowCtr = batchFirst; rowCtr <= batchLast; rowCtr++) {
										int row = sortedRow[rowCtr];
										// Treat special case: insertion of null values
										if (!doubleSourceData.isNull.get(row)) {
											target[rowCtr] = source[row];
										}
										else {
											target[rowCtr] = Integer.MIN_VALUE;
										}
									}
								}
							});
						}
					}
					else {
						DoublePartitionIndex finalIndex = index;
						int[] finalNewPositions = newPositions;
						for (RowRange batch: batches) {
							subTasks.add(new RecursiveAction() {
								@Override
								protected void compute() {
									int batchFirst = batch.firstTuple;
									int batchLast = batch.lastTuple;
									for (int rowCtr = batchFirst; rowCtr <= batchLast; rowCtr++) {
										int row = sortedRow[rowCtr];
										int posCtr = finalIndex.rowToPositions[row];
										// Treat special case: insertion of null values
										if (!doubleSourceData.isNull.get(row)) {
											target[rowCtr] = source[row];
											// Set index masks
											finalNewPositions[posCtr] = rowCtr;
										}
										else {
											target[rowCtr] = Integer.MIN_VALUE;
										}
									}
								}
							});
						}
					}
					break;
				}
			}
		}
		return actionTasks;
	}

	/**
	 * Splits table with given cardinality into tuple batches
	 * according to the configuration for joining.parallel processing.
	 *
	 * @param cardinality	cardinality of table to split
	 * @return				list of row ranges (batches)
	 */
	static List<RowRange> split(int cardinality) {
		int nrBatches = (int) Math.round((cardinality + 0.0) / ParallelConfig.PRE_BATCH_SIZE);
		List<RowRange> batches = new ArrayList<>(nrBatches);
		int batchSize = cardinality / nrBatches;
		int remaining = cardinality - batchSize * nrBatches;

		for (int batchCtr = 0; batchCtr < nrBatches; ++batchCtr) {
			int startIdx = batchCtr * batchSize + Math.min(remaining, batchCtr);
			int endIdx = startIdx + batchSize + (batchCtr < remaining ? 0 : -1);
			RowRange rowRange = new RowRange(startIdx, endIdx);
			batches.add(rowRange);
		}
		return batches;
	}
}
