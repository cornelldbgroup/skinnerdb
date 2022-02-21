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
import com.koloboke.collect.IntCollection;
import config.GeneralConfig;
import config.NamingConfig;
import config.ParallelConfig;
import config.PreConfig;
import data.ColumnData;
import expressions.ExpressionInfo;
import expressions.compilation.EvaluatorType;
import expressions.compilation.ExpressionCompiler;
import expressions.compilation.UnaryBoolEval;
import indexing.Index;
import joining.parallel.indexing.DoublePartitionIndex;
import joining.parallel.indexing.IntPartitionIndex;
import joining.parallel.indexing.PartitionIndex;
import joining.parallel.parallelization.search.SearchResult;
import joining.parallel.threads.ThreadPool;
import predicate.NonEquiNode;
import predicate.NonEquiNodesTest;
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
		// Load required columns for predicate evaluation
		loadPredCols(unaryPred, columnMapping);
		// Compile unary predicate for fast evaluation
		UnaryBoolEval unaryBoolEval = compilePred(unaryPred, columnMapping);
		// Get cardinality of table referenced in predicate
		int cardinality = CatalogManager.getCardinality(tableName);
		// Initialize filter result
		List<Integer> result = null;
//		long s3 = System.currentTimeMillis();
		// Choose between sequential and joining.parallel processing
		if (cardinality <= ParallelConfig.PRE_BATCH_SIZE || !GeneralConfig.isParallel) {
			RowRange allTuples = new RowRange(0, cardinality - 1);
			result = filterBatch(unaryBoolEval, allTuples);
		} else {
			if (unaryPred.columnsMentioned.size() == 1 && PreConfig.PROCESS_KEYS) {
				// Divide tuples into batches
				ColumnRef col = unaryPred.columnsMentioned.iterator().next();
				ColumnRef columnRef = columnMapping.get(col);
				Index index = BufferManager.colToIndex.get(columnRef);
				NonEquiNodesTest nonEquiNodesTest = new NonEquiNodesTest(query, columnMapping);
				unaryPred.finalExpression.accept(nonEquiNodesTest);
				long timer1 = System.currentTimeMillis();
				if (index != null && index.posSet().size() < ParallelConfig.SPARSE_FILTER_SIZE) {
					IntCollection posSet = index.posSet();
					int size = posSet.size();
					NonEquiNode evaluator = nonEquiNodesTest.nonEquiNodes.pop();
					int[] positions = index.positions;
					List<Integer> posList = new ArrayList<>(posSet);
					int[] posResults = new int[size];
					int[] valueResults = new int[size];
					IntStream.range(0, size).parallel().forEach(pid -> {
						int pos = posList.get(pid);
						int nrValues = positions[pos];
						int rowCtr = positions[pos + 1];
						if (evaluator.evaluateUnary(rowCtr)) {
							posResults[pid] = pos;
							valueResults[pid] = nrValues;
						} else {
							posResults[pid] = -1;
							valueResults[pid] = 0;
						}
					});
					int filteredSize = 0;
					for (int i = 0; i < size; i++) {
						filteredSize += valueResults[i];
					}
					String filteredName = NamingConfig.FILTERED_PRE;
					List<String> columnNames = new ArrayList<>();
					for (ColumnRef colRef : requiredCols) {
						columnNames.add(colRef.columnName);
						filteredName = NamingConfig.FILTERED_PRE + colRef.aliasName;
					}
					if (filteredSize == index.cardinality) {
						List<ColumnRef> sourceColRefs = new ArrayList<>();
						for (String columnName : columnNames) {
							sourceColRefs.add(new ColumnRef(tableName, columnName));
						}
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
						// Generate column data
						String finalFilteredName = filteredName;
						sourceColRefs.parallelStream().forEach(sourceColRef -> {
							// Copy relevant rows into result column
							ColumnData srcData = BufferManager.colToData.get(sourceColRef);
							String columnName = sourceColRef.columnName;
							ColumnRef resultColRef = new ColumnRef(finalFilteredName, columnName);
							BufferManager.colToData.put(resultColRef, srcData);
						});
						CatalogManager.updateStats(finalFilteredName);
					}
					else {
						throw new RuntimeException("Not implemented");
					}
					long timer2 = System.currentTimeMillis();
					System.out.println("Time: " + (timer2 - timer1));
					return null;
				}
				else {
					List<RowRange> batches = split(cardinality);
					// Process batches in joining.parallel
					result = batches.parallelStream().flatMap(batch ->
							filterBatch(unaryBoolEval, batch).stream()).collect(
							Collectors.toList());
				}
			}
			else {
				// Divide tuples into batches
				List<RowRange> batches = split(cardinality);
				int nrBatches = batches.size();
//				result = batches.parallelStream().flatMap(batch ->
//						filterBatch(unaryBoolEval, batch).stream()).collect(
//						Collectors.toList());
//				return result;
				ColumnRef columnRef = columnMapping.get(unaryPred.columnsMentioned.iterator().next());
				ColumnData data = BufferManager.getData(columnRef);
				result = new ArrayList<>(cardinality);
				List<Integer>[] resultsArray = new ArrayList[batches.size()];
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
		}
		return result;
	}
	/**
	 * Returns list of indices of rows satisfying given
	 * unary predicate.s
	 *
	 * @param tableName		name of DB table to which predicate applies
	 * @param columnMapping	maps query columns to buffered columns -
	 * 						assume identity mapping if null is specified.
	 * @return				array of satisfying row indices
	 */
	public static int[] executeToArray(IndexFilter filter,
											  String tableName, Map<ColumnRef, ColumnRef> columnMapping,
											   QueryInfo query, List<ColumnRef> requiredCols)
			throws Exception {
		ExpressionInfo unaryPred = filter.remainingInfo;
		// Load required columns for predicate evaluation
		loadPredCols(unaryPred, columnMapping);
		// Compile unary predicate for fast evaluation
		UnaryBoolEval unaryBoolEval = compilePred(unaryPred, columnMapping);
		// Get cardinality of table referenced in predicate
		int cardinality = CatalogManager.getCardinality(tableName);
		// Initialize filter result
		int[] result;
//		long s3 = System.currentTimeMillis();
		// Choose between sequential and joining.parallel processing
		if (cardinality <= ParallelConfig.PRE_BATCH_SIZE || !GeneralConfig.isParallel) {
			RowRange allTuples = new RowRange(0, cardinality - 1);
			result = filterBatch(unaryBoolEval, allTuples).stream().mapToInt(Integer::intValue).toArray();
		} else {
			// Divide tuples into batches
			int nrThreads = 200;
			ExecutorService executorService = ThreadPool.executorService;
			ColumnRef columnRef = columnMapping.get(unaryPred.columnsMentioned.iterator().next());
			ColumnData data = BufferManager.getData(columnRef);
			List<Callable<int[]>> tasks = new ArrayList<>(nrThreads);
			int base = cardinality / nrThreads;
			int remainingTuples = cardinality - base * nrThreads;
			for (int threadCtr = 0; threadCtr < nrThreads; threadCtr++) {
				int prevRemaining = Math.min(remainingTuples, threadCtr);
				int first = threadCtr * base + prevRemaining;
				int end = first + base + (threadCtr < remainingTuples ? 1 : 0);
				int size = end - first;
				tasks.add(() -> {
					int[] subResult = new int[size+1];
					// Evaluate predicate for each table row
//					long start = System.currentTimeMillis();
					int count = 0;
					for (int rowCtr = first; rowCtr < end; ++rowCtr) {
						if (data.longForRow(rowCtr) != Integer.MIN_VALUE
								&& unaryBoolEval.evaluate(rowCtr) > 0) {
							subResult[count+1] = rowCtr;
							count++;
						}
					}
					subResult[0] = count;
//					long terminate = System.currentTimeMillis();
//					System.out.println(Thread.currentThread() + ":" + (terminate - start) + " " + size);
					return subResult;
				});
			}
			long executionStart = System.currentTimeMillis();
			List<Future<int[]>> futures = executorService.invokeAll(tasks);
			long executionEnd = System.currentTimeMillis();
			System.out.println("Parallel Execution: " + (executionEnd - executionStart));
			int totalSize = 0;
			List<int[]> threadResults = new ArrayList<>(nrThreads);
			for (int threadCtr = 0; threadCtr < nrThreads; threadCtr++) {
				int[] threadResult = futures.get(threadCtr).get();
				totalSize += threadResult[0];
				threadResults.add(threadResult);
			}
			result = new int[totalSize];
			int prefix = 0;
			for (int[] threadResult: threadResults) {
				int nrTuples = threadResult[0];
				System.arraycopy(threadResult, 1, result, prefix, nrTuples);
				prefix += nrTuples;
			}

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
	 * Splits table with given cardinality into tuple batches
	 * according to the configuration for joining.parallel processing.
	 *
	 * @param index			column index
	 * @return				list of row ranges (batches)
	 */
	static int[] filterValues(Index index, NonEquiNode unaryBoolEval) {
		if (index == null) {
			return null;
		}
		IntCollection posSet = index.posSet();
		int cardinality = posSet.size();
		if (cardinality < ParallelConfig.SPARSE_FILTER_SIZE) {
			int[] positions = index.positions;
			List<Integer> posList = new ArrayList<>(posSet);
			int[] posResults = new int[cardinality];
			int[] valueResults = new int[cardinality];
			IntStream.range(0, cardinality).parallel().forEach(pid -> {
				int pos = posList.get(pid);
				int nrValues = positions[pos];
				int rowCtr = positions[pos + 1];
				if (unaryBoolEval.evaluateUnary(rowCtr)) {
//					for (int i = pos + 1; i <= pos + nrValues; i++) {
//						result.add(positions[i]);
//					}
					posResults[pid] = pos;
					valueResults[pid] = nrValues;
				}
				else {
					posResults[pid] = -1;
					valueResults[pid] = 0;
				}
			});
			return posResults;
		}
		else {
			return null;
		}
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

}
