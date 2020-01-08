package operators;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import buffer.BufferManager;
import catalog.CatalogManager;
import catalog.info.ColumnInfo;
import catalog.info.TableInfo;
import config.GeneralConfig;
import config.NamingConfig;
import config.ParallelConfig;
import data.*;
import indexing.Index;
import joining.parallel.join.DPJoin;
import joining.parallel.parallelization.lockfree.LockFreeResult;
import joining.parallel.threads.ThreadPool;
import joining.result.ResultTuple;
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
 * @author immanueltrummer
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
		List<ColumnRef> sourceColRefs = new ArrayList<ColumnRef>();
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
		if (GeneralConfig.isParallel) {
			// Generate column data
			sourceColRefs.parallelStream().forEach(sourceColRef -> {
				// Copy relevant rows into result column
				ColumnData srcData = BufferManager.colToData.get(sourceColRef);
				ColumnData resultData = rowList==null?
						srcData.copyRows(rowBitSet):srcData.copyRows(rowList);
				String columnName = sourceColRef.columnName;
				ColumnRef resultColRef = new ColumnRef(targetRelName, columnName);
				BufferManager.colToData.put(resultColRef, resultData);
			});
		}
		else {
			// Generate column data
			for (ColumnRef sourceColRef: sourceColRefs) {
				// Copy relevant rows into result column
				ColumnData srcData = BufferManager.colToData.get(sourceColRef);
				ColumnData resultData = rowList==null?
						srcData.copyRows(rowBitSet):srcData.copyRows(rowList);
				String columnName = sourceColRef.columnName;
				ColumnRef resultColRef = new ColumnRef(targetRelName, columnName);
				BufferManager.colToData.put(resultColRef, resultData);
			}
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

	public static void executeRange(String sourceRelName, List<String> columnNames,
									List<Integer> rowList, Index index, String targetRelName,
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
		List<IntData> intSource = new ArrayList<>();
		List<IntData> intTarget = new ArrayList<>();
		List<LongData> longSource = new ArrayList<>();
		List<LongData> longTarget = new ArrayList<>();
		List<DoubleData> doubleSource = new ArrayList<>();
		List<DoubleData> doubleTarget = new ArrayList<>();
		List<StringData> stringSource = new ArrayList<>();
		List<StringData> stringTarget = new ArrayList<>();
		for (ColumnRef sourceColRef : sourceColRefs) {
			// Add result column to result table, using type of source column
			ColumnInfo sourceCol = CatalogManager.getColumn(sourceColRef);
			ColumnData srcData = BufferManager.colToData.get(sourceColRef);
			ColumnInfo resultCol = new ColumnInfo(sourceColRef.columnName,
					sourceCol.type, sourceCol.isPrimary,
					sourceCol.isUnique, sourceCol.isNotNull,
					sourceCol.isForeign);
			resultTable.addColumn(resultCol);
			ColumnRef resultColRef = new ColumnRef(targetRelName, sourceColRef.columnName);
			JavaType jType = TypeUtil.toJavaType(sourceCol.type);
			switch (jType) {
				case INT:
					IntData intData = new IntData(cardinality);
					intSource.add((IntData) srcData);
					intTarget.add(intData);
					BufferManager.colToData.put(resultColRef, intData);
					break;
				case LONG:
					LongData longData = new LongData(cardinality);
					longSource.add((LongData) srcData);
					longTarget.add(longData);
					BufferManager.colToData.put(resultColRef, longData);
					break;
				case DOUBLE:
					DoubleData doubleData = new DoubleData(cardinality);
					doubleSource.add((DoubleData) srcData);
					doubleTarget.add(doubleData);
					BufferManager.colToData.put(resultColRef, doubleData);
					break;
				case STRING:
					StringData stringData = new StringData(cardinality);
					stringSource.add((StringData) srcData);
					stringTarget.add(stringData);
					BufferManager.colToData.put(resultColRef, stringData);
					break;
			}
		}
		// Load source data if necessary
		if (!GeneralConfig.inMemory) {
			for (ColumnRef sourceColRef : sourceColRefs) {
				BufferManager.loadColumn(sourceColRef);
			}
		}
		ExecutorService executorService = ThreadPool.preprocessingService;
		int nrBatches = GeneralConfig.isParallel ? ParallelConfig.PRE_THREADS : 1;
		List<RowRange> batches = OperatorUtils.split(cardinality, nrBatches);
		List<Future<Integer>> futures = new ArrayList<>();
		int[] sortedRow = index.sortedRow;
		for (RowRange batch: batches) {
			int batchFirst = batch.firstTuple + first;
			int batchLast = batch.lastTuple + first;
			futures.add(executorService.submit(() -> {
				for (int i = 0; i < intSource.size(); i++) {
					IntData intDataSource = intSource.get(i);
					IntData intDataTarget = intTarget.get(i);
					int[] source = intDataSource.data;
					int[] target = intDataTarget.data;
//					int[] fake = new int[batchLast - batchFirst + 1];
					for (int rid = batchFirst; rid <= batchLast; rid++) {
						int offset = rid - first;
						int row = sortedRow[rid];
						// Treat special case: insertion of null values
						target[offset] = source[row];
//						intDataTarget.isNull.set(offset, intDataSource.isNull.get(row));
					}
				}
				for (int i = 0; i < doubleSource.size(); i++) {
					DoubleData doubleDataSource = doubleSource.get(i);
					DoubleData doubleDataTarget = doubleTarget.get(i);
					double[] source = doubleDataSource.data;
					double[] target = doubleDataTarget.data;
//					double[] fake = new double[batchLast - batchFirst + 1];
					for (int rid = batchFirst; rid <= batchLast; rid++) {
						int offset = rid - first;
						int row = sortedRow[rid];
						// Treat special case: insertion of null values
//						fake[rid - batchFirst] = 1;
						target[offset] = source[row];
//						doubleDataTarget.isNull.set(offset, doubleDataSource.isNull.get(row));
					}
				}
				return 1;
			}));
		}


		for (Future<Integer> futureResult : futures) {
			try {
				int result = futureResult.get();
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
		}

		// Generate column data
//		sourceColRefs.parallelStream().forEach(sourceColRef -> {
//			// Copy relevant rows into result column
//			ColumnData srcData = BufferManager.colToData.get(sourceColRef);
//			ColumnData resultData = srcData.copyRangeRows(first, last, index);
//			String columnName = sourceColRef.columnName;
//			ColumnRef resultColRef = new ColumnRef(targetRelName, columnName);
//			BufferManager.colToData.put(resultColRef, resultData);
//		});
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
		List<IntData> intSource = new ArrayList<>();
		List<IntData> intTarget = new ArrayList<>();
		List<LongData> longSource = new ArrayList<>();
		List<LongData> longTarget = new ArrayList<>();
		List<DoubleData> doubleSource = new ArrayList<>();
		List<DoubleData> doubleTarget = new ArrayList<>();
		List<StringData> stringSource = new ArrayList<>();
		List<StringData> stringTarget = new ArrayList<>();
		for (ColumnRef sourceColRef : sourceColRefs) {
			// Add result column to result table, using type of source column
			ColumnInfo sourceCol = CatalogManager.getColumn(sourceColRef);
			ColumnData srcData = BufferManager.colToData.get(sourceColRef);
			ColumnInfo resultCol = new ColumnInfo(sourceColRef.columnName,
					sourceCol.type, sourceCol.isPrimary,
					sourceCol.isUnique, sourceCol.isNotNull,
					sourceCol.isForeign);
			resultTable.addColumn(resultCol);
			ColumnRef resultColRef = new ColumnRef(targetRelName, sourceColRef.columnName);
			JavaType jType = TypeUtil.toJavaType(sourceCol.type);
			switch (jType) {
				case INT:
					IntData intData = new IntData(cardinality);
					intSource.add((IntData) srcData);
					intTarget.add(intData);
					BufferManager.colToData.put(resultColRef, intData);
					break;
				case LONG:
					LongData longData = new LongData(cardinality);
					longSource.add((LongData) srcData);
					longTarget.add(longData);
					BufferManager.colToData.put(resultColRef, longData);
					break;
				case DOUBLE:
					DoubleData doubleData = new DoubleData(cardinality);
					doubleSource.add((DoubleData) srcData);
					doubleTarget.add(doubleData);
					BufferManager.colToData.put(resultColRef, doubleData);
					break;
				case STRING:
					StringData stringData = new StringData(cardinality);
					stringSource.add((StringData) srcData);
					stringTarget.add(stringData);
					BufferManager.colToData.put(resultColRef, stringData);
					break;
			}
		}
		// Load source data if necessary
		if (!GeneralConfig.inMemory) {
			for (ColumnRef sourceColRef : sourceColRefs) {
				BufferManager.loadColumn(sourceColRef);
			}
		}
		ExecutorService executorService = ThreadPool.preprocessingService;
		int nrBatches = GeneralConfig.isParallel ? ParallelConfig.PRE_THREADS : 1;
		List<RowRange> batches = OperatorUtils.split(cardinality, nrBatches);
		List<Future<Integer>> futures = new ArrayList<>();
		int[] sortedRow = index.sortedRow;
		for (RowRange batch: batches) {
			int batchFirst = batch.firstTuple + pos + 1;
			int batchLast = batch.lastTuple + pos + 1;
			futures.add(executorService.submit(() -> {
				for (int i = 0; i < intSource.size(); i++) {
					IntData intDataSource = intSource.get(i);
					IntData intDataTarget = intTarget.get(i);
					int[] source = intDataSource.data;
					int[] target = intDataTarget.data;
					for (int rid = batchFirst; rid <= batchLast; rid++) {
						int offset = rid - pos - 1;
						int row = index.positions[rid];
						// Treat special case: insertion of null values
						target[offset] = source[row];
					}
				}
				for (int i = 0; i < doubleSource.size(); i++) {
					DoubleData doubleDataSource = doubleSource.get(i);
					DoubleData doubleDataTarget = doubleTarget.get(i);
					double[] source = doubleDataSource.data;
					double[] target = doubleDataTarget.data;
					for (int rid = batchFirst; rid <= batchLast; rid++) {
						int offset = rid - pos - 1;
						int row = index.positions[rid];
						// Treat special case: insertion of null values
						target[offset] = source[row];
					}
				}
				return 1;
			}));
		}


		for (Future<Integer> futureResult : futures) {
			try {
				int result = futureResult.get();
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
		}

		// Generate column data
//		sourceColRefs.parallelStream().forEach(sourceColRef -> {
//			// Copy relevant rows into result column
//			ColumnData srcData = BufferManager.colToData.get(sourceColRef);
//			ColumnData resultData = srcData.copyRangeRows(first, last, index);
//			String columnName = sourceColRef.columnName;
//			ColumnRef resultColRef = new ColumnRef(targetRelName, columnName);
//			BufferManager.colToData.put(resultColRef, resultData);
//		});
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
}
