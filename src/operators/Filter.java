package operators;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import buffer.BufferManager;
import catalog.CatalogManager;
import catalog.info.ColumnInfo;
import catalog.info.TableInfo;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import com.koloboke.collect.IntCollection;
import config.GeneralConfig;
import config.NamingConfig;
import config.ParallelConfig;
import config.PreConfig;
import data.ColumnData;
import data.Dictionary;
import data.DoubleData;
import data.IntData;
import expressions.ExpressionInfo;
import expressions.compilation.EvaluatorType;
import expressions.compilation.ExpressionCompiler;
import expressions.compilation.UnaryBoolEval;
import indexing.Index;
import jni.JNIFilter;
import joining.parallel.indexing.DoublePartitionIndex;
import joining.parallel.indexing.IntPartitionIndex;
import joining.parallel.indexing.PartitionIndex;
import joining.parallel.parallelization.search.SearchResult;
import joining.parallel.threads.ThreadPool;
import operators.parallel.JNIParser;
import predicate.NonEquiNode;
import predicate.NonEquiNodesTest;
import preprocessing.Context;
import query.ColumnRef;
import query.QueryInfo;
import types.JavaType;

import javax.swing.text.html.StyleSheet;

/**
 * Filters a table by applying a unary predicate.
 * 
 * @author Anonymous
 *
 */
public class Filter {
	static {
		try {
			System.load(GeneralConfig.JNI_PATH);
		} catch (UnsatisfiedLinkError e) {
			System.err.println("Native code library failed to load.\n" + e);
			System.exit(1);
		}
	}
	/**
	 * Load required columns for predicate evaluations into main memory.
	 * 
	 * @param unaryPred			unary predicate
	 * @param columnMapping		maps query to database columns
	 * @throws Exception
	 */
	static void loadPredCols(ExpressionInfo unaryPred, 
			Map<ColumnRef, ColumnRef> columnMapping) throws Exception {
		// Load required data into memory
		if (!GeneralConfig.inMemory) {
			for (ColumnRef queryRef : unaryPred.columnsMentioned) {
				ColumnRef dbRef = columnMapping.get(queryRef);
				BufferManager.loadColumn(dbRef);
			}
		}
	}
	/**
	 * Compiles evaluator for unary predicate.
	 * 
	 * @param unaryPred			predicate to compile
	 * @param columnMapping		maps query to database columns
	 * @return					compiled predicate evaluator
	 * @throws Exception
	 */
	static UnaryBoolEval compilePred(ExpressionInfo unaryPred, 
			Map<ColumnRef, ColumnRef> columnMapping) throws Exception {
		ExpressionCompiler unaryCompiler = new ExpressionCompiler(
				unaryPred, columnMapping, null, null, 
				EvaluatorType.UNARY_BOOLEAN);
		unaryPred.finalExpression.accept(unaryCompiler);
		return (UnaryBoolEval)unaryCompiler.getBoolEval();
	}
	/**
	 * Returns bit set indicating which rows satisfy a given unary predicate.
	 * 
	 * @param unaryPred			unary predicate
	 * @param tableName			name of DB table to which predicate refers
	 * @param columnMapping		maps query to database columns
	 * @return					bit set indicating satisfying rows
	 * @throws Exception
	 */
	public static BitSet executeToBitSet(ExpressionInfo unaryPred,
			String tableName, Map<ColumnRef, ColumnRef> columnMapping) 
					throws Exception {
		// Load required columns for predicate evaluation
		loadPredCols(unaryPred, columnMapping);
		// Compile unary predicate for fast evaluation
		UnaryBoolEval predEval = compilePred(unaryPred, columnMapping);
		// Get cardinality of table referenced in predicate
		int cardinality = CatalogManager.getCardinality(tableName);
		// Generate result set
		BitSet result = new BitSet(cardinality);
		for (int i=0; i<cardinality; ++i) {
			if (predEval.evaluate(i)>0) {
				result.set(i);
			}
		}
		return result;
	}
	/**
	 * Returns list of indices of rows satisfying given
	 * unary predicate.s
	 *
	 * @param unaryPred		unary predicate
	 * @param tableName		name of DB table to which predicate applies
	 * @param columnMapping	maps query columns to buffered columns -
	 * 						assume identity mapping if null is specified.
	 * @return				list of satisfying row indices
	 */
	public static List<Integer> executeToList(ExpressionInfo unaryPred,
											  String tableName, Map<ColumnRef, ColumnRef> columnMapping)
			throws Exception {
		// Load required columns for predicate evaluation
		loadPredCols(unaryPred, columnMapping);
		// Compile unary predicate for fast evaluation
		UnaryBoolEval unaryBoolEval = compilePred(unaryPred, columnMapping);
		// Get cardinality of table referenced in predicate
		int cardinality = CatalogManager.getCardinality(tableName);
		// Initialize filter result
		List<Integer> result = null;
		// Choose between sequential and parallel processing
		if (cardinality <= ParallelConfig.PRE_BATCH_SIZE) {
			RowRange allTuples = new RowRange(0, cardinality - 1);
			result = filterBatch(unaryBoolEval, allTuples);
		} else {
			// Divide tuples into batches
			List<RowRange> batches = split(cardinality);
			// Process batches in parallel
			result = batches.parallelStream().flatMap(batch ->
					filterBatch(unaryBoolEval, batch).stream()).collect(
					Collectors.toList());
		}
		// Clean up columns loaded for this operation
		/*
		if (!GeneralConfig.inMemory) {
			for (ColumnRef colRef : unaryPred.columnsMentioned) {
				BufferManager.unloadColumn(colRef);
			}
		}
		*/
		return result;
	}
	/**
	 * Returns list of indices of rows satisfying given
	 * unary predicate.s
	 *
	 * @param tableName		name of DB table to which predicate applies
	 * @param columnMapping	maps query columns to buffered columns -
	 * 						assume identity mapping if null is specified.
	 * @return				list of satisfying row indices
	 */
	public static List<Integer> executeToList(IndexFilter filter,
											  String tableName, Map<ColumnRef, ColumnRef> columnMapping,
											  QueryInfo query, List<ColumnRef> requiredCols)
					throws Exception {
		ExpressionInfo unaryPred = filter.remainingInfo;
		// Compile unary predicate for fast evaluation
		UnaryBoolEval unaryBoolEval = compilePred(unaryPred, columnMapping);
		// Get cardinality of table referenced in predicate
		int cardinality = CatalogManager.getCardinality(tableName);
		// Initialize filter result
		List<Integer> result;
//		long s3 = System.currentTimeMillis();
		// Choose between sequential and joining.parallel processing
		if (cardinality <= ParallelConfig.PRE_BATCH_SIZE || !GeneralConfig.isParallel) {
			RowRange allTuples = new RowRange(0, cardinality - 1);
			result = filterBatch(unaryBoolEval, allTuples);
		} else {
			// Divide tuples into batches
			List<RowRange> batches = split(cardinality);
			int nrBatches = batches.size();
			ColumnRef columnRef = columnMapping.get(unaryPred.columnsMentioned.iterator().next());
			ColumnData data = BufferManager.getData(columnRef);
			result = new ArrayList<>(cardinality);
			List<Integer>[] resultsArray = new ArrayList[nrBatches];
			IntStream.range(0, nrBatches).parallel().forEach(bid -> {
				RowRange batch = batches.get(bid);
				int first = batch.firstTuple;
				int end = batch.lastTuple;
				List<Integer> subResult = new ArrayList<>(end - first + 1);
				// Evaluate predicate for each table row
				for (int rowCtr = first; rowCtr <= end; ++rowCtr) {
					if (data.longForRow(rowCtr) != Integer.MIN_VALUE
							&& unaryBoolEval.evaluate(rowCtr) > 0) {
						subResult.add(rowCtr);
					}
				}
				resultsArray[bid] = subResult;
			});
			for (List<Integer> subResult: resultsArray) {
				result.addAll(subResult);
			}
		}
		return result;
	}
	/**
	 * Returns list of indices of rows satisfying given
	 * unary predicate by calling C++.
	 *
	 * @param tableName		name of DB table to which predicate applies
	 * @param preSummary	maps query columns to buffered columns -
	 * 						assume identity mapping if null is specified.
	 * @return				list of satisfying row indices
	 */
	public static List<Integer> executeToListJNI(IndexFilter filter,
											  String tableName, Context preSummary,
											  QueryInfo query, List<ColumnRef> requiredCols, String alias)
			throws Exception {
		ExpressionInfo unaryPred = filter.remainingInfo;
		int[] indexFilteredRows = filter.rows == null ? new int[0] : filter.rows;
		// Get cardinality of table referenced in predicate
		int cardinality = indexFilteredRows.length == 0 ?
				CatalogManager.getCardinality(tableName) : indexFilteredRows.length;
		// Initialize filter result
		List<Integer> result;
//		long s3 = System.currentTimeMillis();
		// Choose between sequential and joining.parallel processing
		if (cardinality <= ParallelConfig.PRE_BATCH_SIZE || !GeneralConfig.isParallel) {
			// Compile unary predicate for fast evaluation
			UnaryBoolEval unaryBoolEval = compilePred(unaryPred, preSummary.columnMapping);
			result = new ArrayList<>(cardinality);
			for (int rowCtr = 0; rowCtr < cardinality; ++rowCtr) {
				if (unaryBoolEval.evaluate(rowCtr) > 0) {
					result.add(rowCtr);
				}
			}
		} else {
			JNIParser jniParser = new JNIParser(query, preSummary.columnMapping);
			String filteredName = NamingConfig.FILTERED_PRE + alias;
			unaryPred.finalExpression.accept(jniParser);
			long time1 = System.currentTimeMillis();
			// All columns to be materialized
			List<int[]> intSrcCols = new ArrayList<>();
			List<double[]> doubleSrcCols = new ArrayList<>();
			List<ColumnRef> sourceColRefs = new ArrayList<>();
			List<ColumnRef> intColRefs = new ArrayList<>();
			List<ColumnRef> doubleColRefs = new ArrayList<>();
			for (ColumnRef columnRef: requiredCols) {
				ColumnRef mapRef = preSummary.columnMapping.get(columnRef);
				ColumnRef filteredRef = new ColumnRef(filteredName, columnRef.columnName);
				sourceColRefs.add(mapRef);
				ColumnData columnData = BufferManager.getData(mapRef);
				if (columnData instanceof IntData) {
					intSrcCols.add(((IntData) columnData).data);
					intColRefs.add(filteredRef);
				}
				else {
					doubleSrcCols.add(((DoubleData) columnData).data);
					doubleColRefs.add(filteredRef);
				}
			}

			int[][] intTargetCols = new int[intSrcCols.size()][];
			double[][] doubleTargetCols = new double[doubleSrcCols.size()][];
			int nrRows = JNIFilter.filter(jniParser.jniAST.toArray(new String[0]),
					jniParser.intColumns.toArray(new int[0][0]),
					jniParser.doubleColumns.toArray(new double[0][0]),
					indexFilteredRows, cardinality,
					intSrcCols.toArray(new int[0][0]), doubleSrcCols.toArray(new double[0][0]),
					intTargetCols, doubleTargetCols,
					ParallelConfig.EXE_THREADS);
			// Materialize relevant rows and columns
			// Update catalog, inserting materialized table
			TableInfo resultTable = new TableInfo(filteredName, true);
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
//			// Divide tuples into batches
//			List<RowRange> batches = split(cardinality);
//			int nrBatches = batches.size();
//			ColumnRef columnRef = preSummary.columnMapping.get(unaryPred.columnsMentioned.iterator().next());
//			ColumnData data = BufferManager.getData(columnRef);
//			result = new ArrayList<>(cardinality);
//			List<Integer>[] resultsArray = new ArrayList[nrBatches];
//			UnaryBoolEval unaryBoolEval = compilePred(unaryPred, preSummary.columnMapping);
//			IntStream.range(0, nrBatches).parallel().forEach(bid -> {
//				RowRange batch = batches.get(bid);
//				int first = batch.firstTuple;
//				int end = batch.lastTuple;
//				List<Integer> subResult = new ArrayList<>(end - first + 1);
//				// Evaluate predicate for each table row
//				for (int rowCtr = first; rowCtr <= end; ++rowCtr) {
//					if (data.longForRow(rowCtr) != Integer.MIN_VALUE
//							&& unaryBoolEval.evaluate(rowCtr) > 0) {
//						subResult.add(rowCtr);
//					}
//				}
//				resultsArray[bid] = subResult;
//			});
//			for (List<Integer> subResult: resultsArray) {
//				result.addAll(subResult);
//			}
			long time2 = System.currentTimeMillis();
//			int newCardinality = intTargetCols.length > 0 ? intTargetCols[0].length : doubleTargetCols[0].length;
			for (int intColCtr = 0; intColCtr < intSrcCols.size(); intColCtr++) {
				int[] target = intTargetCols[intColCtr] == null ?
						intSrcCols.get(intColCtr) : intTargetCols[intColCtr];
				IntData intData = new IntData(target);
				BufferManager.colToData.put(intColRefs.get(intColCtr), intData);
			}
			for (int doubleColCtr = 0; doubleColCtr < doubleSrcCols.size(); doubleColCtr++) {
				double[] target = doubleTargetCols[doubleColCtr] == null ?
						doubleSrcCols.get(doubleColCtr) : doubleTargetCols[doubleColCtr];
				DoubleData doubleData = new DoubleData(target);
				BufferManager.colToData.put(doubleColRefs.get(doubleColCtr), doubleData);
			}
			// Update pre-processing summary
			for (ColumnRef srcRef : requiredCols) {
				String columnName = srcRef.columnName;
				ColumnRef resRef = new ColumnRef(filteredName, columnName);
				preSummary.columnMapping.put(srcRef, resRef);
			}
			// Update statistics in catalog
			CatalogManager.updateStats(filteredName);
			preSummary.aliasToFiltered.put(alias, filteredName);

			result = new ArrayList<>();
			long time3 = System.currentTimeMillis();
			System.out.println("JNI time: " + (time2 - time1) + " " + (time3 - time2));

		}
		return result;
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
		int batchSize = Math.max(ParallelConfig.PRE_BATCH_SIZE, cardinality / 500);
		for (int batchCtr=0; batchCtr * batchSize < cardinality;
				++batchCtr) {
			int startIdx = batchCtr * batchSize;
			int tentativeEndIdx = startIdx + batchSize - 1;
			int endIdx = Math.min(cardinality - 1, tentativeEndIdx);
			RowRange rowRange = new RowRange(startIdx, endIdx);
			batches.add(rowRange);
		}
		return batches;
	}
	/**
	 * Filters given tuple batch using specified predicate evaluator,
	 * return indices of rows within the batch that satisfy the 
	 * predicate.
	 * 
	 * @param unaryBoolEval	unary predicate evaluator
	 * @param rowRange		range of tuple indices of batch
	 * @return				list of indices satisfying the predicate
	 */
	static List<Integer> filterBatch(UnaryBoolEval unaryBoolEval, 
			RowRange rowRange) {
		List<Integer> result = new ArrayList<>(rowRange.lastTuple - rowRange.firstTuple);
		// Evaluate predicate for each table row
		for (int rowCtr=rowRange.firstTuple;
				rowCtr<=rowRange.lastTuple; ++rowCtr) {
			if (unaryBoolEval.evaluate(rowCtr) > 0) {
				result.add(rowCtr);
			}
		}
		return result;
	}
	/**
	 * Filters given tuple batch using specified predicate evaluator,
	 * return indices of rows within the batch that satisfy the
	 * predicate.
	 *
	 * @param unaryBoolEval	unary predicate evaluator
	 * @param rowRange		range of tuple indices of batch
	 * @return				list of indices satisfying the predicate
	 */
	static List<Integer> filterBatchRows(UnaryBoolEval unaryBoolEval,
									 RowRange rowRange, List<Integer> rows) {
		List<Integer> result = new ArrayList<>(rowRange.lastTuple - rowRange.firstTuple);
		// Evaluate predicate for each table row
		for (int rowCtr=rowRange.firstTuple;
			 rowCtr<=rowRange.lastTuple; ++rowCtr) {
			int row = rows.get(rowCtr);
			if (unaryBoolEval.evaluate(row) > 0) {
				result.add(row);
			}
		}
		return result;
	}

}
