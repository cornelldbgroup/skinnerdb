package operators;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import buffer.BufferManager;
import catalog.CatalogManager;
import catalog.info.ColumnInfo;
import catalog.info.TableInfo;
import config.GeneralConfig;
import config.NamingConfig;
import config.ParallelConfig;
import data.*;
import expressions.ExpressionInfo;
import indexing.Index;
import joining.parallel.indexing.DoublePartitionIndex;
import joining.parallel.indexing.IntPartitionIndex;
import joining.parallel.join.DPJoin;
import joining.parallel.parallelization.lockfree.LockFreeResult;
import joining.parallel.threads.ThreadPool;
import joining.result.ResultTuple;
import joining.result.UniqueJoinResult;
import preprocessing.Context;
import print.RelationPrinter;
import query.ColumnRef;
import query.QueryInfo;
import types.JavaType;
import types.SQLtype;
import types.TypeUtil;

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
	 * @param rowBitSet		rows to copy in BitSet representation, can be null
	 * @param targetRelName	name of target table
	 * @param tempResult	whether to create temporary result relation
	 * @throws Exception
	 */
	public static void execute(String sourceRelName, List<String> columnNames,
			List<Integer> rowList, BitSet rowBitSet, String targetRelName,
			boolean tempResult) throws Exception {
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
		Stream<ColumnRef> sourceColStream = GeneralConfig.isParallel?
				sourceColRefs.parallelStream():sourceColRefs.stream();
		sourceColStream.forEach(sourceColRef -> {
			// Copy relevant rows into result column
			ColumnData srcData = BufferManager.colToData.get(sourceColRef);
			String columnName = sourceColRef.columnName;
			ColumnRef resultColRef = new ColumnRef(targetRelName, columnName);
			ColumnInfo sourceCol = CatalogManager.getColumn(sourceColRef);

			JavaType jType = TypeUtil.toJavaType(sourceCol.type);
			switch (jType) {
				case INT: {
					IntData intTargetData = new IntData(cardinality);
					IntData intSourceData = (IntData) srcData;
					BufferManager.colToData.put(resultColRef, intTargetData);
					int[] target = intTargetData.data;
					int[] source = intSourceData.data;
					batches.parallelStream().forEach(batch -> {
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
					});
					break;
				}
				case DOUBLE: {
					DoubleData doubleTargetData = new DoubleData(cardinality);
					DoubleData doubleSourceData = (DoubleData) srcData;
					BufferManager.colToData.put(resultColRef, doubleTargetData);
					double[] target = doubleTargetData.data;
					double[] source = doubleSourceData.data;
					batches.parallelStream().forEach(batch -> {
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
					});
					break;
				}
			}
		});

		// Update statistics in catalog
		CatalogManager.updateStats(targetRelName);
		// Unload source data if necessary
		if (!GeneralConfig.inMemory) {
			for (ColumnRef sourceColRef : sourceColRefs) {
				BufferManager.unloadColumn(sourceColRef);				
			}
		}
	}

	/**
	 * Creates a temporary table with given name and copies into it
	 * values at given row indices and for given columns in source
	 * table.
	 *
	 * @param sourceRelName	name of source table to copy from
	 * @param columnNames	names of columns to be copied
	 * @param rowList		list of row indices to copy, can be null
	 * @param rowBitSet		rows to copy in BitSet representation, can be null
	 * @param targetRelName	name of target table
	 * @param tempResult	whether to create temporary result relation
	 * @throws Exception
	 */
	public static void execute(String sourceRelName, List<String> columnNames,
							   int[] rowList, BitSet rowBitSet, String targetRelName,
							   boolean tempResult) throws Exception {
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
		int cardinality = rowList.length;
		List<RowRange> batches = split(cardinality);
		// Generate column data
		Stream<ColumnRef> sourceColStream = GeneralConfig.isParallel?
				sourceColRefs.parallelStream():sourceColRefs.stream();
		sourceColStream.forEach(sourceColRef -> {
			// Copy relevant rows into result column
			ColumnData srcData = BufferManager.colToData.get(sourceColRef);
			String columnName = sourceColRef.columnName;
			ColumnRef resultColRef = new ColumnRef(targetRelName, columnName);
			ColumnInfo sourceCol = CatalogManager.getColumn(sourceColRef);

			JavaType jType = TypeUtil.toJavaType(sourceCol.type);
			switch (jType) {
				case INT: {
					IntData intTargetData = new IntData(cardinality);
					IntData intSourceData = (IntData) srcData;
					BufferManager.colToData.put(resultColRef, intTargetData);
					int[] target = intTargetData.data;
					int[] source = intSourceData.data;
					batches.parallelStream().forEach(batch -> {
						int batchFirst = batch.firstTuple;
						int batchLast = batch.lastTuple;
						for (int rowCtr = batchFirst; rowCtr <= batchLast; rowCtr++) {
							int row = rowList[rowCtr];
							// Treat special case: insertion of null values
							if (!intSourceData.isNull.get(row)) {
								target[rowCtr] = source[row];
							}
							else {
								target[rowCtr] = Integer.MIN_VALUE;
							}
						}
					});
					break;
				}
				case DOUBLE: {
					DoubleData doubleTargetData = new DoubleData(cardinality);
					DoubleData doubleSourceData = (DoubleData) srcData;
					BufferManager.colToData.put(resultColRef, doubleTargetData);
					double[] target = doubleTargetData.data;
					double[] source = doubleSourceData.data;
					batches.parallelStream().forEach(batch -> {
						int batchFirst = batch.firstTuple;
						int batchLast = batch.lastTuple;
						for (int rowCtr = batchFirst; rowCtr <= batchLast; rowCtr++) {
							int row = rowList[rowCtr];
							// Treat special case: insertion of null values
							if (!doubleSourceData.isNull.get(row)) {
								target[rowCtr] = source[row];
							}
							else {
								target[rowCtr] = Integer.MIN_VALUE;
							}
						}
					});
					break;
				}
			}
		});

		// Update statistics in catalog
		CatalogManager.updateStats(targetRelName);
		// Unload source data if necessary
		if (!GeneralConfig.inMemory) {
			for (ColumnRef sourceColRef : sourceColRefs) {
				BufferManager.unloadColumn(sourceColRef);
			}
		}
	}

	public static void executeRange(String sourceRelName, List<String> columnNames,
									List<Integer> rowList, int[] sortedRow, String targetRelName,
									boolean tempResult) throws Exception {
		// Generate references to source columns
		List<ColumnRef> sourceColRefs = new ArrayList<>();
		for (String columnName : columnNames) {
			sourceColRefs.add(new ColumnRef(sourceRelName, columnName));
		}
		int first = rowList.get(0);
		int last = rowList.get(1);
		int cardinality = last - first;
		// Update catalog, inserting materialized table
		TableInfo resultTable = new TableInfo(targetRelName, tempResult);
		CatalogManager.currentDB.addTable(resultTable);
		// Split batches
		List<RowRange> batches = split(cardinality);

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

		sourceColRefs.parallelStream().forEach(sourceColRef -> {
			ColumnInfo sourceCol = CatalogManager.getColumn(sourceColRef);
			ColumnData srcData = BufferManager.colToData.get(sourceColRef);
			ColumnRef resultColRef = new ColumnRef(targetRelName, sourceColRef.columnName);
			JavaType jType = TypeUtil.toJavaType(sourceCol.type);
			switch (jType) {
				case INT: {
					IntData intTargetData = new IntData(cardinality);
					IntData intSourceData = (IntData) srcData;
					BufferManager.colToData.put(resultColRef, intTargetData);
					int[] target = intTargetData.data;
					int[] source = intSourceData.data;
					batches.parallelStream().forEach(batch -> {
						int batchFirst = batch.firstTuple + first;
						int batchLast = batch.lastTuple + first;
						for (int rowCtr = batchFirst; rowCtr <= batchLast; rowCtr++) {
							int offset = rowCtr - first;
							int row = sortedRow[rowCtr];
							// Treat special case: insertion of null values
							if (!intSourceData.isNull.get(row)) {
								target[offset] = source[row];
							}
						}
					});
					break;
				}
				case DOUBLE: {
					DoubleData doubleTargetData = new DoubleData(cardinality);
					DoubleData doubleSourceData = (DoubleData) srcData;
					BufferManager.colToData.put(resultColRef, doubleTargetData);
					double[] target = doubleTargetData.data;
					double[] source = doubleSourceData.data;
					batches.parallelStream().forEach(batch -> {
						int batchFirst = batch.firstTuple + first;
						int batchLast = batch.lastTuple + first;
						for (int rowCtr = batchFirst; rowCtr <= batchLast; rowCtr++) {
							int offset = rowCtr - first;
							int row = sortedRow[rowCtr];
							// Treat special case: insertion of null values
							if (!doubleSourceData.isNull.get(row)) {
								target[offset] = source[row];
							}
						}
					});
					break;
				}
			}
		});
		// Update statistics in catalog
		CatalogManager.updateStats(targetRelName);
		// Unload source data if necessary
		if (!GeneralConfig.inMemory) {
			for (ColumnRef sourceColRef : sourceColRefs) {
				BufferManager.unloadColumn(sourceColRef);
			}
		}
	}

	public static void executeEqualPos(String sourceRelName, List<String> columnNames,
									List<Integer> rowList, Index index, String targetRelName,
									boolean tempResult) throws Exception {
		// Generate references to source columns
		List<ColumnRef> sourceColRefs = new ArrayList<>();
		for (String columnName : columnNames) {
			sourceColRefs.add(new ColumnRef(sourceRelName, columnName));
		}
		int pos = rowList.get(0);
		int cardinality = index.positions[pos];
		// Update catalog, inserting materialized table
		TableInfo resultTable = new TableInfo(targetRelName, tempResult);
		CatalogManager.currentDB.addTable(resultTable);
//		List<IntData> intSource = new ArrayList<>();
//		List<IntData> intTarget = new ArrayList<>();
//		List<DoubleData> doubleSource = new ArrayList<>();
//		List<DoubleData> doubleTarget = new ArrayList<>();
//		for (ColumnRef sourceColRef : sourceColRefs) {
//			// Add result column to result table, using type of source column
//			ColumnInfo sourceCol = CatalogManager.getColumn(sourceColRef);
//			ColumnData srcData = BufferManager.colToData.get(sourceColRef);
//			ColumnInfo resultCol = new ColumnInfo(sourceColRef.columnName,
//					sourceCol.type, sourceCol.isPrimary,
//					sourceCol.isUnique, sourceCol.isNotNull,
//					sourceCol.isForeign);
//			resultTable.addColumn(resultCol);
//			ColumnRef resultColRef = new ColumnRef(targetRelName, sourceColRef.columnName);
//			JavaType jType = TypeUtil.toJavaType(sourceCol.type);
//			switch (jType) {
//				case INT:
//					IntData intData = new IntData(cardinality);
//					intSource.add((IntData) srcData);
//					intTarget.add(intData);
//					BufferManager.colToData.put(resultColRef, intData);
//					break;
//				case DOUBLE:
//					DoubleData doubleData = new DoubleData(cardinality);
//					doubleSource.add((DoubleData) srcData);
//					doubleTarget.add(doubleData);
//					BufferManager.colToData.put(resultColRef, doubleData);
//					break;
//			}
//		}
//		// Load source data if necessary
//		if (!GeneralConfig.inMemory) {
//			for (ColumnRef sourceColRef : sourceColRefs) {
//				BufferManager.loadColumn(sourceColRef);
//			}
//		}
//		ExecutorService executorService = ThreadPool.preprocessingService;
//		int nrBatches = GeneralConfig.isParallel ? ParallelConfig.PRE_THREADS : 1;
//		List<RowRange> batches = OperatorUtils.split(cardinality, nrBatches);
//		List<Future<Integer>> futures = new ArrayList<>();
//		for (RowRange batch: batches) {
//			int batchFirst = batch.firstTuple + pos + 1;
//			int batchLast = batch.lastTuple + pos + 1;
//			futures.add(executorService.submit(() -> {
//				for (int i = 0; i < intSource.size(); i++) {
//					IntData intDataSource = intSource.get(i);
//					IntData intDataTarget = intTarget.get(i);
//					int[] source = intDataSource.data;
//					int[] target = intDataTarget.data;
//					for (int rid = batchFirst; rid <= batchLast; rid++) {
//						int offset = rid - pos - 1;
//						int row = index.positions[rid];
//						// Treat special case: insertion of null values
//						if (!intDataSource.isNull.get(row)) {
//							target[offset] = source[row];
//						}
//						else {
//							target[offset] = Integer.MIN_VALUE;
//						}
//					}
//				}
//				for (int i = 0; i < doubleSource.size(); i++) {
//					DoubleData doubleDataSource = doubleSource.get(i);
//					DoubleData doubleDataTarget = doubleTarget.get(i);
//					double[] source = doubleDataSource.data;
//					double[] target = doubleDataTarget.data;
//					for (int rid = batchFirst; rid <= batchLast; rid++) {
//						int offset = rid - pos - 1;
//						int row = index.positions[rid];
//						// Treat special case: insertion of null values
//						if (!doubleDataSource.isNull.get(row)) {
//							target[offset] = source[row];
//						}
//						else {
//							target[offset] = Integer.MIN_VALUE;
//						}
//					}
//				}
//				return 1;
//			}));
//		}
//
//
//		for (Future<Integer> futureResult : futures) {
//			try {
//				int result = futureResult.get();
//			} catch (InterruptedException | ExecutionException e) {
//				e.printStackTrace();
//			}
//		}
		for (ColumnRef sourceColRef : sourceColRefs) {
			// Add result column to result table, using type of source column
			ColumnInfo sourceCol = CatalogManager.getColumn(sourceColRef);
			ColumnInfo resultCol = new ColumnInfo(sourceColRef.columnName,
					sourceCol.type, sourceCol.isPrimary,
					sourceCol.isUnique, sourceCol.isNotNull,
					sourceCol.isForeign);
			resultTable.addColumn(resultCol);
		}

		if (GeneralConfig.isParallel) {
			//Generate column data
			sourceColRefs.parallelStream().forEach(sourceColRef -> {
				// Copy relevant rows into result column
				ColumnData srcData = BufferManager.colToData.get(sourceColRef);
				if (srcData instanceof IntData) {
					IntData resultData = new IntData(cardinality);
					IntData intData = (IntData) srcData;
					for (int i = 0; i < cardinality; i++) {
						int row = index.positions[pos + 1 + i];
						if (!srcData.isNull.get(row)) {
							resultData.data[i] = intData.data[row];
						}
						else {
							resultData.isNull.set(i);
						}
					}
					String columnName = sourceColRef.columnName;
					ColumnRef resultColRef = new ColumnRef(targetRelName, columnName);
					BufferManager.colToData.put(resultColRef, resultData);
				}
				else if (srcData instanceof DoubleData) {
					DoubleData resultData = new DoubleData(cardinality);
					DoubleData doubleData = (DoubleData) srcData;
					for (int i = 0; i < cardinality; i++) {
						int row = index.positions[pos + 1 + i];
						if (!srcData.isNull.get(row)) {
							resultData.data[i] = doubleData.data[row];
						}
						else {
							resultData.isNull.set(i);
						}
					}
					String columnName = sourceColRef.columnName;
					ColumnRef resultColRef = new ColumnRef(targetRelName, columnName);
					BufferManager.colToData.put(resultColRef, resultData);
				}
			});
		}
		else {
			sourceColRefs.forEach(sourceColRef -> {
				// Copy relevant rows into result column
				ColumnData srcData = BufferManager.colToData.get(sourceColRef);
				if (srcData instanceof IntData) {
					IntData resultData = new IntData(cardinality);
					IntData intData = (IntData) srcData;
					for (int i = 0; i < cardinality; i++) {
						int row = index.positions[pos + 1 + i];
						if (!srcData.isNull.get(row)) {
							resultData.data[i] = intData.data[row];
						}
						else {
							resultData.isNull.set(i);
						}
					}
					String columnName = sourceColRef.columnName;
					ColumnRef resultColRef = new ColumnRef(targetRelName, columnName);
					BufferManager.colToData.put(resultColRef, resultData);
				}
				else if (srcData instanceof DoubleData) {
					DoubleData resultData = new DoubleData(cardinality);
					DoubleData doubleData = (DoubleData) srcData;
					for (int i = 0; i < cardinality; i++) {
						int row = index.positions[pos + 1 + i];
						if (!srcData.isNull.get(row)) {
							resultData.data[i] = doubleData.data[row];
						}
						else {
							resultData.isNull.set(i);
						}
					}
					String columnName = sourceColRef.columnName;
					ColumnRef resultColRef = new ColumnRef(targetRelName, columnName);
					BufferManager.colToData.put(resultColRef, resultData);
				}
			});
		}

		// Update statistics in catalog
		CatalogManager.updateStats(targetRelName);
		// Unload source data if necessary
		if (!GeneralConfig.inMemory) {
			for (ColumnRef sourceColRef : sourceColRefs) {
				BufferManager.unloadColumn(sourceColRef);
			}
		}
	}

	/**
	 * Materializes a join relation from given indices
	 * for a set of base tables.
	 * 
	 * @param tuples			base table indices representing result tuples
	 * @param tableToIdx		maps table names to base table indices
	 * @param sourceCols		set of columns to copy
	 * @param columnMappings	maps source columns, as in query, to DB columns
	 * @param targetRelName		name of materialized result relation
	 * @throws Exception
	 */
	public static void execute(Collection<ResultTuple> tuples, 
			Map<String, Integer> tableToIdx, 
			Collection<ColumnRef> sourceCols, 
			Map<ColumnRef, ColumnRef> columnMappings, 
			String targetRelName) throws Exception {
		// Update catalog, insert result table
		TableInfo resultInfo = new TableInfo(targetRelName, true);
		CatalogManager.currentDB.addTable(resultInfo);
		// Add result columns to catalog
		for (ColumnRef srcQueryRef : sourceCols) {
			// Map query column to DB column
			ColumnRef srcDBref = columnMappings.get(srcQueryRef);
			// Extract information on source column
			String srcAlias = srcQueryRef.aliasName;
			String srcColName = srcQueryRef.columnName;
			ColumnInfo srcInfo = CatalogManager.getColumn(srcDBref);
			// Generate target column
			String targetColName = srcAlias + "." + srcColName;
			ColumnInfo targetInfo = new ColumnInfo(targetColName, 
					srcInfo.type, false, false, false, false);
			resultInfo.addColumn(targetInfo);
		}

		// Materialize result columns
		sourceCols.parallelStream().forEach(srcQueryRef -> {
			// Generate target column reference
			String targetCol = srcQueryRef.aliasName + "." + srcQueryRef.columnName;
			ColumnRef targetRef = new ColumnRef(targetRelName, targetCol);
			// Generate target column
			int tableIdx = tableToIdx.get(srcQueryRef.aliasName);
			ColumnRef srcDBref = columnMappings.get(srcQueryRef);
			ColumnData srcData = BufferManager.colToData.get(srcDBref);
			ColumnData targetData = srcData.copyRows(tuples, tableIdx);
			// Insert into buffer pool
			BufferManager.colToData.put(targetRef, targetData);
		});
		// Update statistics in catalog
		CatalogManager.updateStats(targetRelName);
	}

	/**
	 * Materializes a join relation from given indices
	 * for a set of base tables.
	 *
	 * @param tuples			base table indices representing result tuples
	 * @param tableToIdx		maps table names to base table indices
	 * @param sourceCols		set of columns to copy
	 * @param context	maps source columns, as in query, to DB columns
	 * @param targetRelName		name of materialized result relation
	 * @throws Exception
	 */
	public static void execute(Collection<ResultTuple> tuples,
							   Map<String, Integer> tableToIdx,
							   Collection<ColumnRef> sourceCols,
							   Context context,
							   String targetRelName) throws Exception {
		// Update catalog, insert result table
		TableInfo resultInfo = new TableInfo(targetRelName, true);
		CatalogManager.currentDB.addTable(resultInfo);
		// Add result columns to catalog
		for (ColumnRef srcQueryRef : sourceCols) {
			// Map query column to DB column
			ColumnRef srcDBref = context.columnMapping.get(srcQueryRef);
			// Extract information on source column
			String srcAlias = srcQueryRef.aliasName;
			String srcColName = srcQueryRef.columnName;
			ColumnInfo srcInfo = CatalogManager.getColumn(srcDBref);
			// Generate target column
			String targetColName = srcAlias + "." + srcColName;
			ColumnInfo targetInfo = new ColumnInfo(targetColName,
					srcInfo.type, false, false, false, false);
			resultInfo.addColumn(targetInfo);
		}

		List<Collection<ResultTuple>> resultsPerThread = context.resultTuplesList;
		int maxSize = context.maxSize;
		ResultTuple[] resultArray = new ResultTuple[maxSize];
		ConcurrentHashMap<ResultTuple, Integer> resultTupleSet = new ConcurrentHashMap<>(maxSize);

		AtomicInteger index = new AtomicInteger(0);
		resultsPerThread.parallelStream().forEach(resultTuples ->
				resultTuples.forEach(resultTuple -> {
					if (resultTupleSet.putIfAbsent(resultTuple, 0) == null) {
						resultArray[index.getAndIncrement()] = resultTuple;
					}
				})
		);
		int cardinality = index.get();
		List<RowRange> batches = split(cardinality);
		// Materialize result columns
		sourceCols.parallelStream().forEach(srcQueryRef -> {
			// Generate target column reference
			String targetCol = srcQueryRef.aliasName + "." + srcQueryRef.columnName;
			ColumnRef targetRef = new ColumnRef(targetRelName, targetCol);
			// Generate target column
			int tableIdx = tableToIdx.get(srcQueryRef.aliasName);
			ColumnRef srcDBref = context.columnMapping.get(srcQueryRef);
			ColumnInfo srcInfo = CatalogManager.getColumn(srcDBref);
			ColumnData srcData = BufferManager.colToData.get(srcDBref);
			JavaType jType = TypeUtil.toJavaType(srcInfo.type);
			switch (jType) {
				case INT: {
					IntData intTargetData = new IntData(cardinality);
					IntData intSourceData = (IntData) srcData;
					BufferManager.colToData.put(targetRef, intTargetData);
					int[] target = intTargetData.data;
					int[] source = intSourceData.data;
					batches.parallelStream().forEach(batch -> {
						int batchFirst = batch.firstTuple;
						int batchLast = batch.lastTuple;
						for (int rowCtr = batchFirst; rowCtr <= batchLast; rowCtr++) {
							int row = resultArray[rowCtr].baseIndices[tableIdx];
							// Treat special case: insertion of null values
							if (!intSourceData.isNull.get(row)) {
								target[rowCtr] = source[row];
							}
							else {
								target[rowCtr] = Integer.MIN_VALUE;
							}
						}
					});
					break;
				}
				case DOUBLE: {
					DoubleData doubleTargetData = new DoubleData(cardinality);
					DoubleData doubleSourceData = (DoubleData) srcData;
					BufferManager.colToData.put(targetRef, doubleTargetData);
					double[] target = doubleTargetData.data;
					double[] source = doubleSourceData.data;
					batches.parallelStream().forEach(batch -> {
						int batchFirst = batch.firstTuple;
						int batchLast = batch.lastTuple;
						for (int rowCtr = batchFirst; rowCtr <= batchLast; rowCtr++) {
							int row = resultArray[rowCtr].baseIndices[tableIdx];
							// Treat special case: insertion of null values
							if (!doubleSourceData.isNull.get(row)) {
								target[rowCtr] = source[row];
							}
							else {
								target[rowCtr] = Integer.MIN_VALUE;
							}
						}
					});
					break;
				}
			}
		});
		// Update statistics in catalog
		CatalogManager.updateStats(targetRelName);
	}



	/**
	 * Materializes a join relation from given an existing relation
	 *
	 * @param sourceCols		set of columns to copy
	 * @param columnMappings	maps source columns, as in query, to DB columns
	 * @param targetRelName		name of materialized result relation
	 * @throws Exception
	 */
	public static void executeFromExistingTable(Collection<ColumnRef> sourceCols,
							   Map<ColumnRef, ColumnRef> columnMappings,
							   String targetRelName) throws Exception {
		// Update catalog, insert result table
		TableInfo resultInfo = new TableInfo(targetRelName, true);
		CatalogManager.currentDB.addTable(resultInfo);
		// Add result columns to catalog
		for (ColumnRef srcQueryRef : sourceCols) {
			// Map query column to DB column
			ColumnRef srcDBref = columnMappings.get(srcQueryRef);
			// Extract information on source column
			String srcAlias = srcQueryRef.aliasName;
			String srcColName = srcQueryRef.columnName;
			ColumnInfo srcInfo = CatalogManager.getColumn(srcDBref);
			// Generate target column
			String targetColName = srcAlias + "." + srcColName;
			ColumnInfo targetInfo = new ColumnInfo(targetColName,
					srcInfo.type, false, false, false, false);
			resultInfo.addColumn(targetInfo);
		}

		// Materialize result columns
		sourceCols.parallelStream().forEach(srcQueryRef -> {
			// Generate target column reference
			String targetCol = srcQueryRef.aliasName + "." + srcQueryRef.columnName;
			ColumnRef targetRef = new ColumnRef(targetRelName, targetCol);
			// Generate target column
			ColumnRef srcDBref = columnMappings.get(srcQueryRef);
			ColumnData srcData = BufferManager.colToData.get(srcDBref);
			// Insert into buffer pool
			BufferManager.colToData.put(targetRef, srcData);
		});

		// Update statistics in catalog
		CatalogManager.updateStats(targetRelName);

	}

	/**
	 * Materializes a temporary relation from given tuple indices.
	 *
	 * @param columnMappings	maps source columns, as in query, to DB columns.
	 * @param tupleIndices		tuple indices combination.
	 * @param queryInfo			query information
	 * @throws Exception
	 */
	public static void materializeTupleIndices(Map<ColumnRef, ColumnRef> columnMappings, int[] tupleIndices,
											   QueryInfo queryInfo) throws Exception {
		String targetRelName = NamingConfig.TEST_RESULT_NAME;
		TableInfo resultInfo = new TableInfo(targetRelName, true);
		CatalogManager.currentDB.addTable(resultInfo);
		// Add result columns to catalog
		for (ColumnRef srcQueryRef : columnMappings.keySet()) {
			// Map query column to DB column
			ColumnRef srcDBref = columnMappings.get(srcQueryRef);
			// Extract information on source column
			String srcAlias = srcQueryRef.aliasName;
			String srcColName = srcQueryRef.columnName;
			ColumnInfo srcInfo = CatalogManager.getColumn(srcDBref);
			// Generate target column
			String targetColName = srcColName;
			ColumnInfo targetInfo = new ColumnInfo(targetColName,
					srcInfo.type, false, false, false, false);
			resultInfo.addColumn(targetInfo);
		}
		columnMappings.keySet().forEach(srcQueryRef -> {
			// Copy relevant rows into result column
			String targetCol = srcQueryRef.columnName;
			ColumnRef sourceColRef = columnMappings.get(srcQueryRef);
			ColumnData srcData = BufferManager.colToData.get(sourceColRef);
			int tableIdx = queryInfo.aliasToIndex.get(srcQueryRef.aliasName);
			int row = tupleIndices[tableIdx];
			List<Integer> rowList = new ArrayList<>();
			rowList.add(row);
			ColumnData resultData = srcData.copyRows(rowList);
			ColumnRef resultColRef = new ColumnRef(targetRelName, targetCol);
			BufferManager.colToData.put(resultColRef, resultData);
		});
		// Update statistics in catalog
		CatalogManager.updateStats(targetRelName);
		RelationPrinter.print(targetRelName);
	}


	public static void materializeRow(UniqueJoinResult uniqueJoinResult,
									  QueryInfo query,
									  Context context,
									  String resultRel) throws Exception {

		TableInfo resultInfo = new TableInfo(resultRel, true);
		CatalogManager.currentDB.addTable(resultInfo);
		// Add result columns to catalog
		int columnCtr = 0;
		for (ExpressionInfo expr : query.selectExpressions) {
			ColumnRef srcQueryRef = expr.columnsMentioned.iterator().next();
			ColumnRef srcDBref = context.columnMapping.get(srcQueryRef);
			ColumnInfo srcInfo = CatalogManager.getColumn(srcDBref);
			// Update catalog by adding result column
			String targetColName = query.selectToAlias.getOrDefault(expr,
					srcQueryRef.aliasName + "." + srcQueryRef.columnName);
			ColumnRef resultRef = new ColumnRef(resultRel, targetColName);
			ColumnInfo targetInfo = new ColumnInfo(targetColName,
					srcInfo.type, false, false, false, false);
			resultInfo.addColumn(targetInfo);
			// insert row
			List<Integer> rowList = new ArrayList<>();
			int row = uniqueJoinResult.tuples[columnCtr];
			rowList.add(row);
			ColumnData resultData = uniqueJoinResult.uniqueColumns[columnCtr].copyRows(rowList);
			BufferManager.colToData.put(resultRef, resultData);
			columnCtr++;
		}
		// Update statistics in catalog
		CatalogManager.updateStats(resultRel);
	}

	/**
	 * Splits table with given cardinality into tuple batches
	 * according to the configuration for joining.parallel processing.
	 *
	 * @param cardinality	cardinality of table to split
	 * @return				list of row ranges (batches)
	 */
	static List<RowRange> split(int cardinality) {
		List<RowRange> batches = new ArrayList<RowRange>();
		for (int batchCtr=0; batchCtr*ParallelConfig.PRE_BATCH_SIZE<cardinality;
			 ++batchCtr) {
			int startIdx = batchCtr * ParallelConfig.PRE_BATCH_SIZE;
			int tentativeEndIdx = startIdx + ParallelConfig.PRE_BATCH_SIZE-1;
			int endIdx = Math.min(cardinality - 1, tentativeEndIdx);
			RowRange rowRange = new RowRange(startIdx, endIdx);
			batches.add(rowRange);
		}
		return batches;
	}
}
