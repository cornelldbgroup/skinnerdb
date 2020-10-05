package preprocessing;

import buffer.BufferManager;
import catalog.CatalogManager;
import catalog.info.ColumnInfo;
import catalog.info.TableInfo;
import com.koloboke.collect.IntCollection;
import com.koloboke.collect.map.DoubleIntCursor;
import com.koloboke.collect.map.IntIntCursor;
import com.koloboke.collect.map.hash.HashDoubleIntMaps;
import com.koloboke.collect.map.hash.HashIntIntMaps;
import config.*;
import data.*;
import expressions.ExpressionInfo;
import expressions.compilation.EvaluatorType;
import expressions.compilation.ExpressionCompiler;
import expressions.compilation.UnaryBoolEval;
import indexing.Index;
import joining.parallel.indexing.DoubleIndexRange;
import joining.parallel.indexing.DoublePartitionIndex;
import joining.parallel.indexing.IntIndexRange;
import joining.parallel.indexing.IntPartitionIndex;
import joining.parallel.threads.ThreadPool;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import operators.*;
import predicate.NonEquiNode;
import predicate.NonEquiNodesTest;
import print.RelationPrinter;
import query.ColumnRef;
import query.QueryInfo;
import statistics.PreStats;
import types.JavaType;
import types.TypeUtil;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Filters query tables via unary predicates. Try to avoid
 * materializing filtered rows into new tables. Creates hash tables
 * for columns with binary equality join predicates.
 *
 * @author Anonymous
 *
 */
public class NewPreprocessor {
//    /**
//     * Whether an error occurred during last invocation.
//     * This flag is used in cases where an error occurs
//     * without an exception being thrown.
//     */
//    public static boolean hadError = false;
//    /**
//     * Whether to calculate performance.
//     */
//    public static boolean performance = false;
//    /**
//     * Whether to calculate performance.
//     */
//    public static boolean terminated = false;
//    /**
//     * Translates a column reference using a table
//     * alias into one using the original table.
//     *
//     * @param query		meta-data about query
//     * @param queryRef	reference to alias column
//     * @return 			resolved column reference
//     */
//    static ColumnRef DBref(QueryInfo query, ColumnRef queryRef) {
//        String alias = queryRef.aliasName;
//        String table = query.aliasToTable.get(alias);
//        String colName = queryRef.columnName;
//        return new ColumnRef(table, colName);
//    }
//
//    /**
//     * Executes pre-processing.
//     *
//     * @param query			the query to pre-process
//     * @return 				summary of pre-processing steps
//     */
//    public static Context process(QueryInfo query) throws Exception {
//        // Start counter
//        long startMillis = System.currentTimeMillis();
//        // Reset error flag
//        hadError = false;
//        terminated = false;
//        // Collect columns required for joins and post-processing
//        Set<ColumnRef> requiredCols = new HashSet<>();
//        requiredCols.addAll(query.colsForJoins);
//        requiredCols.addAll(query.colsForPostProcessing);
//        // Initialize pre-processing summary
//        Context preSummary = new Context();
//        // Initialize mapping for join and post-processing columns
//        for (ColumnRef queryRef : requiredCols) {
//            preSummary.columnMapping.put(queryRef,
//                    DBref(query, queryRef));
//        }
//        // Initialize column mapping for unary predicate columns
//        for (ExpressionInfo unaryPred : query.unaryPredicates) {
//            for (ColumnRef queryRef : unaryPred.columnsMentioned) {
//                preSummary.columnMapping.put(queryRef,
//                        DBref(query, queryRef));
//            }
//        }
//        // Initialize mapping from query alias to DB tables
//        preSummary.aliasToFiltered.putAll(query.aliasToTable);
//
//        // Iterate over query aliases
//        query.aliasToTable.keySet().parallelStream().forEach(alias -> {
//            long s1 = System.currentTimeMillis();
//            // Collect required columns (for joins and post-processing) for this table
//            List<ColumnRef> curRequiredCols = new ArrayList<>();
//            for (ColumnRef requiredCol : requiredCols) {
//                if (requiredCol.aliasName.equals(alias)) {
//                    curRequiredCols.add(requiredCol);
//                }
//            }
//
//            // Get applicable unary predicates
//            ExpressionInfo curUnaryPred = null;
//            for (ExpressionInfo exprInfo : query.unaryPredicates) {
//                if (exprInfo.aliasesMentioned.contains(alias)) {
//                    curUnaryPred = exprInfo;
//                }
//            }
//
//            // Filter and project if enabled
//            if (curUnaryPred != null && PreConfig.FILTER) {
//                //check if the predicate is in the cache
//                List<Integer> inCacheRows = null;
//                try {
//                    if (inCacheRows == null) {
//                        // Apply index to prune rows if possible
//                        IndexFilter remainingPred = applyIndex(query, curUnaryPred);
//                        //ExpressionInfo remainingPred = curUnaryPred;
//                        // Filter remaining rows by remaining predicate
//                        filterProject(query, alias, remainingPred,
//                                curRequiredCols, preSummary);
//                        String filteredName = NamingConfig.FILTERED_PRE + alias;
//                        int cardinality = CatalogManager.getCardinality(filteredName);
//                        if (cardinality == 0) {
//                            terminated = true;
//                        }
//                    }
//                    else {
//                        System.out.println("In Cache!");
//                    }
//                } catch (Exception e) {
//                    System.err.println("Error filtering " + alias);
//                    e.printStackTrace();
//                    hadError = true;
//                }
//            }
//            else {
//                String table = query.aliasToTable.get(alias);
//                preSummary.aliasToFiltered.put(alias, table);
//            }
//
//            long s2 = System.currentTimeMillis();
//            if (curUnaryPred != null) {
//                System.out.println("Predicate: " + curUnaryPred.toString() + "\tTime: " + (s2 - s1));
//            }
//
//        });
//
//        // Measure processing time
//        if (performance) {
//            PreStats.preMillis = System.currentTimeMillis() - startMillis;
//            PreStats.subPreMillis.add(PreStats.preMillis);
//        }
//
//        // construct mapping from join tables to index for each join predicate
//        query.equiJoinPreds.forEach(expressionInfo -> {
//            expressionInfo.extractIndex(preSummary);
//            expressionInfo.setColumnType();
//        });
//        return preSummary;
//    }
//
//    /**
//     * Forms a conjunction between given conjuncts.
//     *
//     * @param conjuncts	list of conjuncts
//     * @return	conjunction between all conjuncts or null
//     * 			(iff the input list of conjuncts is empty)
//     */
//    static Expression conjunction(List<Expression> conjuncts) {
//        Expression result = null;
//        for (Expression conjunct : conjuncts) {
//            if (result == null) {
//                result = conjunct;
//            } else {
//                result = new AndExpression(
//                        result, conjunct);
//            }
//        }
//        return result;
//    }
//
//    /**
//     * Search for applicable index and use it to prune rows. Redirect
//     * column mappings to index-filtered table if possible.
//     *
//     * @param query			query to pre-process
//     * @param unaryPred		unary predicate on that table
//     * @return	remaining unary predicate to apply afterwards
//     */
//    static IndexFilter applyIndex(QueryInfo query, ExpressionInfo unaryPred) throws Exception {
//        // Divide predicate conjuncts depending on whether they can
//        // be evaluated using indices alone.
//        IndexTest indexTest = new IndexTest(query);
//        List<Expression> indexedConjuncts = new ArrayList<>();
//        List<Expression> nonIndexedConjuncts = new ArrayList<>();
//        List<Expression> sortedConjuncts = new ArrayList<>();
//        List<Expression> unsortedConjuncts = new ArrayList<>();
//        for (Expression conjunct : unaryPred.conjuncts) {
//            // Re-initialize index test
//            indexTest.canUseIndex = true;
//            indexTest.constantQueue.clear();
//            indexTest.sorted = true;
//            // Compare predicate against indexes
//            conjunct.accept(indexTest);
//            // Can conjunct be evaluated only from indices?
//            if (indexTest.canUseIndex && PreConfig.CONSIDER_INDICES) {
//                if (indexTest.sorted) {
//                    sortedConjuncts.add(conjunct);
//                } else {
//                    unsortedConjuncts.add(conjunct);
//                }
//            } else {
//                nonIndexedConjuncts.add(conjunct);
//            }
//        }
//
//        // Create remaining predicate expression
//        if (unsortedConjuncts.size() > 0) {
//            indexedConjuncts.addAll(unsortedConjuncts);
//            nonIndexedConjuncts.addAll(sortedConjuncts);
//        } else {
//            indexedConjuncts.addAll(sortedConjuncts);
//        }
//        Expression remainingExpr = conjunction(nonIndexedConjuncts);
//        // Evaluate indexed predicate part
//        if (!indexedConjuncts.isEmpty()) {
//            IndexFilter indexFilter = new IndexFilter(query);
//            Expression indexedExpr = conjunction(indexedConjuncts);
//            indexedExpr.accept(indexFilter);
//            // Need to keep columns for evaluating remaining predicates, if any
//            if (remainingExpr != null) {
//                indexFilter.remainingInfo = new ExpressionInfo(query, remainingExpr);
//            }
//            return indexFilter;
//        } else {
//            IndexFilter indexFilter = new IndexFilter(query);
//            indexFilter.remainingInfo = unaryPred;
//            return indexFilter;
//        }
//    }
//
//
//    /**
//     * Creates a new temporary table containing remaining tuples
//     * after applying unary predicates, project on columns that
//     * are required for following steps.
//     *
//     * @param query			query to pre-process
//     * @param alias			alias of table to filter
//     * @param indexFilter	unary predicate and index filter on that table
//     * @param requiredCols	project on those columns
//     * @param preSummary	summary of pre-processing steps
//     */
//    static void filterProject(QueryInfo query, String alias, IndexFilter indexFilter,
//                                       List<ColumnRef> requiredCols, Context preSummary) throws Exception {
//        // Determine rows satisfying unary predicate
//        // Compile unary predicate for fast evaluation
//        Map<ColumnRef, ColumnRef> columnMapping = preSummary.columnMapping;
//        ExpressionInfo unaryPred = indexFilter.remainingInfo;
//        UnaryBoolEval unaryBoolEval = unaryPred == null ? null : compilePred(unaryPred, columnMapping);
//        String tableName = preSummary.aliasToFiltered.get(alias);
//        // Get cardinality of table referenced in predicate
//        int cardinality = indexCardinality(tableName, indexFilter);
//        // target relation name
//        String targetRelName = NamingConfig.FILTERED_PRE + alias;
//        // columns that need to be indexed
//        List<ColumnRef> indexedCols = new ArrayList<>();
//        List<ColumnRef> nonIndexedCols = new ArrayList<>();
//        requiredCols.forEach(columnRef -> {
//            if (query.indexCols.contains(columnRef)) {
//                indexedCols.add(columnRef);
//            }
//            else {
//                nonIndexedCols.add(columnRef);
//            }
//        });
//
//        // Choose between sequential and joining.parallel processing
//        if (cardinality <= ParallelConfig.PARALLEL_SIZE || !GeneralConfig.isParallel) {
//            RowRange allTuples = new RowRange(0, cardinality - 1);
//            List<RowRange> batches = new ArrayList<>();
//            batches.add(allTuples);
////            filterBatch(unaryBoolEval, batches, indexFilter, indexedCols, nonIndexedCols);
//        } else {
//            if (unaryPred == null) {
//                // Divide tuples into batches
//                List<RowRange> batches = split(cardinality);
//                if (indexedCols.size() == 0) {
//                    materializeBatch(batches, nonIndexedCols,
//                            indexFilter, targetRelName, cardinality);
//                }
//                else {
//                    filterBatch(batches, indexFilter,
//                            indexedCols, nonIndexedCols, targetRelName, cardinality);
//                }
//            }
//            else if (PreConfig.PROCESS_KEYS && unaryPred.columnsMentioned.size() == 1) {
//                throw new RuntimeException("Not Implemented!");
//            }
//            else {
//                // Divide tuples into batches
//                List<RowRange> batches = split(cardinality);
//                // Process batches when there is no need to generate index.
//            }
//        }
////
//        // Update pre-processing summary
//        for (ColumnRef srcRef : requiredCols) {
//            String columnName = srcRef.columnName;
//            ColumnRef resRef = new ColumnRef(targetRelName, columnName);
//            preSummary.columnMapping.put(srcRef, resRef);
//        }
//        preSummary.aliasToFiltered.put(alias, targetRelName);
//        // Print out intermediate result table if logging is enabled
//        if (LoggingConfig.PRINT_INTERMEDIATES) {
//            RelationPrinter.print(targetRelName);
//        }
//    }
//
//
//
//    private static void materializeBatch(List<RowRange> batches,
//                                         List<ColumnRef> nonIndexedCols,
//                                         IndexFilter indexFilter,
//                                         String targetRelName,
//                                         int cardinality) throws Exception {
//        // Generate references to source columns
//        if (indexFilter.qualifyingRows.size() > 0) {
//            // Update catalog, inserting materialized table
//            TableInfo resultTable = new TableInfo(targetRelName, true);
//            CatalogManager.currentDB.addTable(resultTable);
//
//            List<IntData> intSource = new ArrayList<>();
//            List<IntData> intTarget = new ArrayList<>();
//            List<DoubleData> doubleSource = new ArrayList<>();
//            List<DoubleData> doubleTarget = new ArrayList<>();
//
//            initializeData(nonIndexedCols, resultTable, cardinality, intSource, intTarget, doubleSource, doubleTarget);
//            // materialize tuples
//            List<Integer> rows = indexFilter.qualifyingRows.pop();
//            Index index = indexFilter.lastIndex;
//            batches.parallelStream().forEach(rowRange -> {
//                // Materialize predicate for each table row
//                copyColumnData(indexFilter, rowRange, rows, index,
//                        intSource, intTarget, doubleSource, doubleTarget);
//            });
//        }
//        else {
//            throw new Exception("Not Implemented!");
//        }
//        // Update statistics in catalog
//        CatalogManager.updateStats(targetRelName);
//    }
//
//    static void copyColumnData(IndexFilter indexFilter, RowRange rowRange,
//                               List<Integer> rows, Index index,
//                               List<IntData> intSource, List<IntData> intTarget,
//                               List<DoubleData> doubleSource, List<DoubleData> doubleTarget) {
//        if (indexFilter.resultType == 0) {
//            for (int i = 0; i < intSource.size(); i++) {
//                IntData intDataSource = intSource.get(i);
//                IntData intDataTarget = intTarget.get(i);
//                int[] source = intDataSource.data;
//                int[] target = intDataTarget.data;
//                for (int rowCtr = rowRange.firstTuple;
//                     rowCtr <= rowRange.lastTuple; ++rowCtr) {
//                    int row = rows.get(rowCtr);
//                    if (!intDataSource.isNull.get(row)) {
//                        target[rowCtr] = source[row];
//                    }
//                    else {
//                        target[rowCtr] = Integer.MIN_VALUE;
//                    }
//                }
//            }
//            for (int i = 0; i < doubleSource.size(); i++) {
//                DoubleData doubleDataSource = doubleSource.get(i);
//                DoubleData doubleDataTarget = doubleTarget.get(i);
//                double[] source = doubleDataSource.data;
//                double[] target = doubleDataTarget.data;
//                for (int rowCtr = rowRange.firstTuple;
//                     rowCtr <= rowRange.lastTuple; ++rowCtr) {
//                    int row = rows.get(rowCtr);
//                    if (!doubleDataSource.isNull.get(row)) {
//                        target[rowCtr] = source[row];
//                    }
//                    else {
//                        target[rowCtr] = Integer.MIN_VALUE;
//                    }
//                }
//            }
//        }
//        else if (indexFilter.resultType == 1) {
//            int first = rows.get(0);
//            int[] sortedRow = index.sortedRow;
//            for (int i = 0; i < intSource.size(); i++) {
//                IntData intDataSource = intSource.get(i);
//                IntData intDataTarget = intTarget.get(i);
//                int[] source = intDataSource.data;
//                int[] target = intDataTarget.data;
//                for (int rowCtr = rowRange.firstTuple;
//                     rowCtr <= rowRange.lastTuple; ++rowCtr) {
//                    int row = sortedRow[rowCtr + first];
//                    if (!intDataSource.isNull.get(row)) {
//                        target[rowCtr] = source[row];
//                    }
//                    else {
//                        target[rowCtr] = Integer.MIN_VALUE;
//                    }
//                }
//            }
//            for (int i = 0; i < doubleSource.size(); i++) {
//                DoubleData doubleDataSource = doubleSource.get(i);
//                DoubleData doubleDataTarget = doubleTarget.get(i);
//                double[] source = doubleDataSource.data;
//                double[] target = doubleDataTarget.data;
//                for (int rowCtr = rowRange.firstTuple;
//                     rowCtr <= rowRange.lastTuple; ++rowCtr) {
//                    int row = sortedRow[rowCtr + first];
//                    if (!doubleDataSource.isNull.get(row)) {
//                        target[rowCtr] = source[row];
//                    }
//                    else {
//                        target[rowCtr] = Integer.MIN_VALUE;
//                    }
//                }
//            }
//        }
//        else {
//            int pos = rows.get(0);
//            int[] positions = index.positions;
//            for (int i = 0; i < intSource.size(); i++) {
//                IntData intDataSource = intSource.get(i);
//                IntData intDataTarget = intTarget.get(i);
//                int[] source = intDataSource.data;
//                int[] target = intDataTarget.data;
//                for (int rowCtr = rowRange.firstTuple;
//                     rowCtr <= rowRange.lastTuple; ++rowCtr) {
//                    int row = positions[pos + 1 + rowCtr];
//                    if (!intDataSource.isNull.get(row)) {
//                        target[rowCtr] = source[row];
//                    }
//                    else {
//                        target[rowCtr] = Integer.MIN_VALUE;
//                    }
//                }
//            }
//            for (int i = 0; i < doubleSource.size(); i++) {
//                DoubleData doubleDataSource = doubleSource.get(i);
//                DoubleData doubleDataTarget = doubleTarget.get(i);
//                double[] source = doubleDataSource.data;
//                double[] target = doubleDataTarget.data;
//                for (int rowCtr = rowRange.firstTuple;
//                     rowCtr <= rowRange.lastTuple; ++rowCtr) {
//                    int row = positions[pos + 1 + rowCtr];
//                    if (!doubleDataSource.isNull.get(row)) {
//                        target[rowCtr] = source[row];
//                    }
//                    else {
//                        target[rowCtr] = Integer.MIN_VALUE;
//                    }
//                }
//            }
//        }
//    }
//
//    /**
//     * Filters given tuple batch using specified predicate evaluator,
//     * return indices of rows within the batch that satisfy the
//     * predicate.
//     *
//     * @param indexFilter	unary predicate and index filter on that table
//     */
//    static void filterBatch(List<RowRange> batches,
//                            IndexFilter indexFilter,
//                            List<ColumnRef> indexedCols,
//                            List<ColumnRef> nonIndexedCols,
//                            String targetRelName,
//                            int cardinality) throws Exception {
//        if (indexFilter.qualifyingRows.size() > 0) {
//            TableInfo resultTable = new TableInfo(targetRelName, true);
//            CatalogManager.currentDB.addTable(resultTable);
//            // initialize column data
//            List<IntData> intSource = new ArrayList<>();
//            List<IntData> intTarget = new ArrayList<>();
//            List<DoubleData> doubleSource = new ArrayList<>();
//            List<DoubleData> doubleTarget = new ArrayList<>();
//            initializeData(nonIndexedCols, resultTable, cardinality, intSource, intTarget, doubleSource, doubleTarget);
//            // initialize index
//            List<IntPartitionIndex> intIndexes = new ArrayList<>();
//            List<IntPartitionIndex> intSourceIndex = new ArrayList<>();
//            List<DoublePartitionIndex> doubleIndexes = new ArrayList<>();
//            List<DoublePartitionIndex> doubleSourceIndex = new ArrayList<>();
//            initializeIndexes(indexedCols, resultTable, cardinality, intIndexes,
//                    intSourceIndex, doubleIndexes, doubleSourceIndex);
//            int nrBatches = batches.size();
//            IntIndexRange[][] intIndexRanges = intIndexes.size() > 0 ?
//                    new IntIndexRange[intIndexes.size()][nrBatches] : null;
//            DoubleIndexRange[][] doubleIndexRanges = doubleIndexes.size() > 0 ?
//                    new DoubleIndexRange[doubleIndexes.size()][nrBatches] : null;
//            // materialize tuples
//            List<Integer> rows = indexFilter.qualifyingRows.pop();
//            Index index = indexFilter.lastIndex;
//            IntStream.range(0, nrBatches).parallel().forEach(bid -> {
//                RowRange rowRange = batches.get(bid);
//                if (indexFilter.resultType == 0) {
//                    for (int i = 0; i < intIndexes.size(); i++) {
//                        IntPartitionIndex intIndex = intIndexes.get(i);
//                        IntPartitionIndex source = intSourceIndex.get(i);
//                        IntIndexRange intIndexRange = new IntIndexRange(rowRange.firstTuple, rowRange.lastTuple, bid);
//                        intIndexRanges[i][bid] = intIndexRange;
//                        for (int rowCtr = rowRange.firstTuple;
//                             rowCtr <= rowRange.lastTuple; ++rowCtr) {
//                            int row = rows.get(rowCtr);
//                            if (!source.intData.isNull.get(row)) {
//                                int value = source.intData.data[row];
//                                intIndexRange.add(value);
//                                intIndex.intData.data[rowCtr] = value;
//                            }
//                        }
//                    }
//                    for (int i = 0; i < doubleIndexes.size(); i++) {
//                        DoublePartitionIndex doubleIndex = doubleIndexes.get(i);
//                        DoublePartitionIndex source = doubleSourceIndex.get(i);
//                        DoubleIndexRange doubleIndexRange = new DoubleIndexRange(rowRange.firstTuple, rowRange.lastTuple, bid);
//                        doubleIndexRanges[i][bid] = doubleIndexRange;
//                        for (int rowCtr = rowRange.firstTuple;
//                             rowCtr <= rowRange.lastTuple; ++rowCtr) {
//                            int row = rows.get(rowCtr);
//                            if (!source.doubleData.isNull.get(row)) {
//                                double value = source.doubleData.data[row];
//                                doubleIndexRange.add(value);
//                                doubleIndex.doubleData.data[rowCtr] = value;
//                            }
//                        }
//                    }
//                }
//                else if (indexFilter.resultType == 1) {
//                    int first = rows.get(0);
//                    int[] sortedRow = index.sortedRow;
//                    for (int i = 0; i < intIndexes.size(); i++) {
//                        IntPartitionIndex intIndex = intIndexes.get(i);
//                        IntPartitionIndex source = intSourceIndex.get(i);
//                        IntIndexRange intIndexRange = new IntIndexRange(rowRange.firstTuple, rowRange.lastTuple, bid);
//                        intIndexRanges[i][bid] = intIndexRange;
//                        for (int rowCtr = rowRange.firstTuple;
//                             rowCtr <= rowRange.lastTuple; ++rowCtr) {
//                            int row = sortedRow[rowCtr + first];
//                            if (!source.intData.isNull.get(row)) {
//                                int value = source.intData.data[row];
//                                intIndexRange.add(value);
//                                intIndex.intData.data[rowCtr] = value;
//                            }
//                        }
//                    }
//                    for (int i = 0; i < doubleIndexes.size(); i++) {
//                        DoublePartitionIndex doubleIndex = doubleIndexes.get(i);
//                        DoublePartitionIndex source = doubleSourceIndex.get(i);
//                        DoubleIndexRange doubleIndexRange = new DoubleIndexRange(rowRange.firstTuple, rowRange.lastTuple, bid);
//                        doubleIndexRanges[i][bid] = doubleIndexRange;
//                        for (int rowCtr = rowRange.firstTuple;
//                             rowCtr <= rowRange.lastTuple; ++rowCtr) {
//                            int row = sortedRow[rowCtr + first];
//                            if (!source.doubleData.isNull.get(row)) {
//                                double value = source.doubleData.data[row];
//                                doubleIndexRange.add(value);
//                                doubleIndex.doubleData.data[rowCtr] = value;
//
//                            }
//                        }
//                    }
//                }
//                else {
//                    int pos = rows.get(0);
//                    int[] positions = index.positions;
//                    for (int i = 0; i < intIndexes.size(); i++) {
//                        IntPartitionIndex intIndex = intIndexes.get(i);
//                        IntPartitionIndex source = intSourceIndex.get(i);
//                        IntIndexRange intIndexRange = new IntIndexRange(rowRange.firstTuple, rowRange.lastTuple, bid);
//                        intIndexRanges[i][bid] = intIndexRange;
//                        for (int rowCtr = rowRange.firstTuple;
//                             rowCtr <= rowRange.lastTuple; ++rowCtr) {
//                            int row = positions[pos + 1 + rowCtr];
//                            if (!source.intData.isNull.get(row)) {
//                                int value = source.intData.data[row];
//                                intIndexRange.add(value);
//                                intIndex.intData.data[rowCtr] = value;
//                            }
//                        }
//                    }
//                    for (int i = 0; i < doubleIndexes.size(); i++) {
//                        DoublePartitionIndex doubleIndex = doubleIndexes.get(i);
//                        DoublePartitionIndex source = doubleSourceIndex.get(i);
//                        DoubleIndexRange doubleIndexRange = new DoubleIndexRange(rowRange.firstTuple, rowRange.lastTuple, bid);
//                        doubleIndexRanges[i][bid] = doubleIndexRange;
//                        for (int rowCtr = rowRange.firstTuple;
//                             rowCtr <= rowRange.lastTuple; ++rowCtr) {
//                            int row = positions[pos + 1 + rowCtr];
//                            if (!source.doubleData.isNull.get(row)) {
//                                double value = source.doubleData.data[row];
//                                doubleIndexRange.add(value);
//                                doubleIndex.doubleData.data[rowCtr] = value;
//                            }
//                        }
//                    }
//                }
//                copyColumnData(indexFilter, rowRange, rows, index,
//                        intSource, intTarget, doubleSource, doubleTarget);
//            });
//
//            IntStream.range(0, nrBatches).parallel().forEach(bid -> {
//                for (int i = 0; i < intIndexes.size(); i++) {
//                    IntPartitionIndex intIndex = intIndexes.get(i);
//                    IntPartitionIndex source = intSourceIndex.get(i);
//                    IntIndexRange intIndexRange = intIndexRanges[i][bid];
//                    IntIntCursor batchCursor = intIndexRange.valuesMap.cursor();
//                    intIndexRange.prefixMap = HashIntIntMaps.newMutableMap(intIndexRange.valuesMap.size());
//                    intIndex.keyToPositions = source.keyToPositions;
//                    intIndex.positions = new int[source.positions.length];
//                    int[] positions = intIndex.positions;
//                    while (batchCursor.moveNext()) {
//                        int key = batchCursor.key();
//                        int prefix = 1;
//                        int localNr = batchCursor.value();
//                        int startPos = intIndex.keyToPositions.getOrDefault(key, -1);
//                        for (int b = 0; b < bid; b++) {
//                            prefix += intIndexRanges[i][b].valuesMap.getOrDefault(key, 0);
//                        }
//                        int nrValue = positions[startPos];
//                        if (nrValue == 0) {
//                            int nr = prefix - 1 + localNr;
//                            for (int b = bid + 1; b < nrBatches; b++) {
//                                nr += intIndexRanges[i][b].valuesMap.getOrDefault(key, 0);
//                            }
//                            positions[startPos] = nr;
//                            nrValue = nr;
//                        }
//                        if (nrValue > 1) {
//                            intIndexRange.prefixMap.put(key, prefix + startPos);
//                        }
//                    }
//                    // Evaluate predicate for each table row
//                    for (int rowCtr = intIndexRange.firstTuple;
//                         rowCtr <= intIndexRange.lastTuple; ++rowCtr) {
//                        if (!source.intData.isNull.get(rowCtr)) {
//                            int value = intIndex.intData.data[rowCtr];
//                            int firstPos = intIndex.keyToPositions.getOrDefault(value, -1);
//                            int nr = positions[firstPos];
//                            if (nr == 1) {
//                                positions[firstPos + 1] = rowCtr;
//                                intIndex.scopes[rowCtr] = 0;
//                            } else {
//                                int pos = intIndexRange.prefixMap.computeIfPresent(value, (k, v) -> v + 1) - 1;
//                                positions[pos] = rowCtr;
//                                int startThread = (pos - firstPos - 1) % ParallelConfig.EXE_THREADS;
//                                intIndex.scopes[rowCtr] = (byte) startThread;
//                            }
//                        }
//                    }
//                }
//
//                for (int i = 0; i < doubleIndexes.size(); i++) {
//                    DoublePartitionIndex doubleIndex = doubleIndexes.get(i);
//                    DoublePartitionIndex source = doubleSourceIndex.get(i);
//                    DoubleIndexRange doubleIndexRange = doubleIndexRanges[i][bid];
//                    DoubleIntCursor batchCursor = doubleIndexRange.valuesMap.cursor();
//                    doubleIndexRange.prefixMap = HashDoubleIntMaps.newMutableMap(doubleIndexRange.valuesMap.size());
//                    doubleIndex.keyToPositions = source.keyToPositions;
//                    doubleIndex.positions = new int[source.positions.length];
//                    int[] positions = doubleIndex.positions;
//                    while (batchCursor.moveNext()) {
//                        double key = batchCursor.key();
//                        int prefix = 1;
//                        int localNr = batchCursor.value();
//                        int startPos = doubleIndex.keyToPositions.getOrDefault(key, -1);
//                        for (int b = 0; b < bid; b++) {
//                            prefix += doubleIndexRanges[i][b].valuesMap.getOrDefault(key, 0);
//                        }
//                        int nrValue = positions[startPos];
//                        if (nrValue == 0) {
//                            int nr = prefix - 1 + localNr;
//                            for (int b = bid + 1; b < nrBatches; b++) {
//                                nr += doubleIndexRanges[i][b].valuesMap.getOrDefault(key, 0);
//                            }
//                            positions[startPos] = nr;
//                            nrValue = nr;
//                        }
//                        if (nrValue > 1) {
//                            doubleIndexRange.prefixMap.put(key, prefix + startPos);
//                        }
//                    }
//                    // Evaluate predicate for each table row
//                    for (int rowCtr = doubleIndexRange.firstTuple;
//                         rowCtr <= doubleIndexRange.lastTuple; ++rowCtr) {
//                        if (!source.doubleData.isNull.get(rowCtr)) {
//                            double value = doubleIndex.doubleData.data[rowCtr];
//                            int firstPos = doubleIndex.keyToPositions.getOrDefault(value, -1);
//                            int nr = positions[firstPos];
//                            if (nr == 1) {
//                                positions[firstPos + 1] = rowCtr;
//                                doubleIndex.scopes[rowCtr] = 0;
//                            } else {
//                                int pos = doubleIndexRange.prefixMap.computeIfPresent(value, (k, v) -> v + 1) - 1;
//                                positions[pos] = rowCtr;
//                                int startThread = (pos - firstPos - 1) % ParallelConfig.EXE_THREADS;
//                                doubleIndex.scopes[rowCtr] = (byte) startThread;
//                            }
//                        }
//                    }
//                }
//            });
//            // Update statistics in catalog
//            CatalogManager.updateStats(targetRelName);
//        }
//        else {
//
//        }
//    }
//
//    static void initializeData(List<ColumnRef> nonIndexedCols, TableInfo resultTable, int cardinality,
//                               List<IntData> intSource, List<IntData> intTarget,
//                               List<DoubleData> doubleSource, List<DoubleData> doubleTarget) throws Exception {
//        for (ColumnRef sourceColRef : nonIndexedCols) {
//            // Add result column to result table, using type of source column
//            ColumnInfo sourceCol = CatalogManager.getColumn(sourceColRef);
//            ColumnData srcData = BufferManager.colToData.get(sourceColRef);
//            ColumnInfo resultCol = new ColumnInfo(sourceColRef.columnName,
//                    sourceCol.type, sourceCol.isPrimary,
//                    sourceCol.isUnique, sourceCol.isNotNull,
//                    sourceCol.isForeign);
//            resultTable.addColumn(resultCol);
//            ColumnRef resultColRef = new ColumnRef(resultTable.name, sourceColRef.columnName);
//            JavaType jType = TypeUtil.toJavaType(sourceCol.type);
//            switch (jType) {
//                case INT:
//                    IntData intData = new IntData(cardinality);
//                    intSource.add((IntData) srcData);
//                    intTarget.add(intData);
//                    BufferManager.colToData.put(resultColRef, intData);
//                    break;
//                case DOUBLE:
//                    DoubleData doubleData = new DoubleData(cardinality);
//                    doubleSource.add((DoubleData) srcData);
//                    doubleTarget.add(doubleData);
//                    BufferManager.colToData.put(resultColRef, doubleData);
//                    break;
//            }
//        }
//    }
//
//    static void initializeIndexes(List<ColumnRef> indexedCols,
//                                  TableInfo resultTable,
//                                  int cardinality,
//                                  List<IntPartitionIndex> intIndexes,
//                                  List<IntPartitionIndex> intSource,
//                                  List<DoublePartitionIndex> doubleIndexes,
//                                  List<DoublePartitionIndex> doubleSource) throws Exception {
//        for (ColumnRef sourceColRef : indexedCols) {
//            // Add result column to result table, using type of source column
//            ColumnInfo sourceCol = CatalogManager.getColumn(sourceColRef);
//            ColumnInfo resultCol = new ColumnInfo(sourceColRef.columnName,
//                    sourceCol.type, sourceCol.isPrimary,
//                    sourceCol.isUnique, sourceCol.isNotNull,
//                    sourceCol.isForeign);
//            resultTable.addColumn(resultCol);
//            JavaType jType = TypeUtil.toJavaType(sourceCol.type);
//            switch (jType) {
//                case INT: {
//                    IntPartitionIndex intPartitionIndex = new IntPartitionIndex(cardinality, sourceColRef);
//                    IntPartitionIndex source = (IntPartitionIndex) BufferManager.colToIndex.get(sourceColRef);
//                    intIndexes.add(intPartitionIndex);
//                    intSource.add(source);
//                    ColumnRef targetColRef = new ColumnRef(resultTable.name, source.queryRef.columnName);
//                    BufferManager.colToData.put(targetColRef, intPartitionIndex.intData);
//                    BufferManager.colToIndex.put(targetColRef, intPartitionIndex);
//                    break;
//                }
//                case DOUBLE: {
//                    DoublePartitionIndex doublePartitionIndex = new DoublePartitionIndex(cardinality, sourceColRef);
//                    DoublePartitionIndex source = (DoublePartitionIndex) BufferManager.colToIndex.get(sourceColRef);
//                    doubleIndexes.add(doublePartitionIndex);
//                    doubleSource.add(source);
//                    ColumnRef targetColRef = new ColumnRef(resultTable.name, source.queryRef.columnName);
//                    BufferManager.colToData.put(targetColRef, doublePartitionIndex.doubleData);
//                    BufferManager.colToIndex.put(targetColRef, doublePartitionIndex);
//                    break;
//                }
//            }
//        }
//    }
//
//    /**
//     * Get the cardinality of filtered table
//     * after applying index.
//     *
//     * @param tableName		name of DB table to which predicate applies
//     * @param indexFilter	unary predicate and index filter on that table
//     */
//    static int indexCardinality(String tableName, IndexFilter indexFilter) {
//        if (indexFilter.qualifyingRows.size() > 0) {
//            List<Integer> rows = indexFilter.qualifyingRows.peek();
//            // full results
//            if (indexFilter.resultType == 0) {
//                return rows.size();
//            }
//            // range results
//            else if (indexFilter.resultType == 1) {
//                return rows.get(1) - rows.get(0);
//            }
//            // equal position
//            else {
//                int position = rows.get(0);
//                return indexFilter.lastIndex.positions[position];
//            }
//        }
//        else {
//            return CatalogManager.getCardinality(tableName);
//        }
//    }
//
//    /**
//     * Compiles evaluator for unary predicate.
//     *
//     * @param unaryPred			predicate to compile
//     * @param columnMapping		maps query to database columns
//     * @return					compiled predicate evaluator
//     * @throws Exception
//     */
//    static UnaryBoolEval compilePred(ExpressionInfo unaryPred,
//                                     Map<ColumnRef, ColumnRef> columnMapping) throws Exception {
//        ExpressionCompiler unaryCompiler = new ExpressionCompiler(
//                unaryPred, columnMapping, null, null,
//                EvaluatorType.UNARY_BOOLEAN);
//        unaryPred.finalExpression.accept(unaryCompiler);
//        return (UnaryBoolEval)unaryCompiler.getBoolEval();
//    }
//
//    /**
//     * Splits table with given cardinality into tuple batches
//     * according to the configuration for joining.parallel processing.
//     *
//     * @param cardinality	cardinality of table to split
//     * @return				list of row ranges (batches)
//     */
//    static List<RowRange> split(int cardinality) {
//        List<RowRange> batches = new ArrayList<>();
//        for (int batchCtr=0; batchCtr*ParallelConfig.PARALLEL_SIZE < cardinality;
//             ++batchCtr) {
//            int startIdx = batchCtr * ParallelConfig.PARALLEL_SIZE;
//            int tentativeEndIdx = startIdx + ParallelConfig.PARALLEL_SIZE - 1;
//            int endIdx = Math.min(cardinality - 1, tentativeEndIdx);
//            RowRange rowRange = new RowRange(startIdx, endIdx);
//            batches.add(rowRange);
//        }
//        return batches;
//    }
}