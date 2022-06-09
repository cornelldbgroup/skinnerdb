package preprocessing;

import buffer.BufferManager;
import catalog.CatalogManager;
import catalog.info.ColumnInfo;
import com.google.common.primitives.Ints;
import config.*;
import data.ColumnData;
import data.DoubleData;
import data.IntData;
import expressions.ExpressionInfo;
import expressions.compilation.EvaluatorType;
import expressions.compilation.ExpressionCompiler;
import expressions.compilation.UnaryBoolEval;
import indexing.Index;
import indexing.Indexer;
import joining.parallel.indexing.DoublePartitionIndex;
import joining.parallel.indexing.IndexPolicy;
import joining.parallel.indexing.IntPartitionIndex;
import joining.parallel.indexing.PartitionIndex;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import operators.*;
import operators.parallel.Materialize;
import print.RelationPrinter;
import query.ColumnRef;
import query.QueryInfo;

import java.util.*;
import java.util.concurrent.RecursiveAction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TableTask extends RecursiveAction {
    public final QueryInfo query;
    public final Context preSummary;
    public final String alias;
    public final List<ColumnRef> curRequiredCols;
    public boolean terminated = false;

    public TableTask(QueryInfo query, Context preSummary, String alias,
                     List<ColumnRef> curRequiredCols) {
        this.query = query;
        this.preSummary = preSummary;
        this.alias = alias;
        this.curRequiredCols = curRequiredCols;
    }


    @Override
    protected void compute() {
        // Phase 1: Index filtering
        // Get applicable unary predicates
        ExpressionInfo curUnaryPred = null;
        System.out.println("Start " + alias);
        for (ExpressionInfo exprInfo : query.unaryPredicates) {
            if (exprInfo.aliasesMentioned.contains(alias)) {
                curUnaryPred = exprInfo;
            }
        }
        int size;
        // Filter and project
        List<Integer> rows = null;
        IndexFilter filter = null;
        // Result type: 0 - full result, 1 - range [start, end], 2 - pos
        int resultType = 0;
        if (curUnaryPred != null) {
            // Apply index to prune rows if possible
            long indexFilterTimer = System.currentTimeMillis();
            try {
                filter = applyIndex(
                        query, curUnaryPred, preSummary);
                rows = filter.qualifyingRows.peek();
                resultType = filter.resultType;
                // Early materialization for filtering
                if (resultType == 1 && filter.remainingInfo != null && rows != null) {
                    long startTimer = System.currentTimeMillis();
                    int start = rows.get(0);
                    int end = rows.get(1);
                    rows = Ints.asList(Arrays.copyOfRange(filter.lastIndex.sortedRow, start, end));
                    Collections.sort(rows);
                    long endTimer = System.currentTimeMillis();
                    System.out.println("Range to list: " + (endTimer - startTimer));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            ExpressionInfo remainingPred = filter.remainingInfo;
            long filterTimer = System.currentTimeMillis();
            System.out.println(alias + " index filter " + (filterTimer - indexFilterTimer));
            // Filter remaining rows by remaining predicate
            if (remainingPred != null) {
                try {
                    rows = filterProject(filter, rows, preSummary);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                size = rows.size();
                resultType = 0;
                if (size == 0) {
                    terminated = true;
                }
            }
            long f2Timer = System.currentTimeMillis();
            System.out.println(alias + " f2 index filter " + (f2Timer - filterTimer));
            String alias = curUnaryPred.aliasesMentioned.iterator().next();
            String table = query.aliasToTable.get(alias);
            Set<ColumnRef> colSuperset = new HashSet<>();
            colSuperset.addAll(query.colsForJoins);
            colSuperset.addAll(query.colsForPostProcessing);
            List<String> requiredCols = colSuperset.stream().
                    filter(c -> c.aliasName.equals(alias)).
                    map(c -> c.columnName).collect(Collectors.toList());
            String filteredName = NamingConfig.FILTERED_PRE + alias;
            Set<ColumnRef> indexSuperset = new HashSet<>(query.indexCols);

            Set<ColumnRef> requiredIndexedCols = indexSuperset.stream().
                    filter(c -> c.aliasName.equals(alias)).collect(Collectors.toSet());
            // Materialize the satisfied rows
            try {
                // Case 1: full results
                if (resultType == 0) {
                    List<List<RecursiveAction>> tasks = Materialize.execute(table, requiredCols,
                            requiredIndexedCols, rows,
                            filteredName, true);
                    for (List<RecursiveAction> subTasks: tasks) {
                        invokeAll(subTasks);
                    }
                    // Update statistics in catalog
                    CatalogManager.updateStats(filteredName);
                    preSummary.aliasToFiltered.put(alias, filteredName);
                    int cardinality = CatalogManager.getCardinality(filteredName);
                    if (cardinality == 0) {
                        terminated = true;
                    }
                }
                // Case 2: range
                else if (resultType == 1) {
                    List<List<RecursiveAction>> tasks = Materialize.executeRange(table, requiredCols,
                            requiredIndexedCols,
                            filter.lastIndex.sortedRow, rows,
                            filteredName, true);
                    int idx = 0;
                    for (List<RecursiveAction> subTasks: tasks) {
                        long earlyStart = System.currentTimeMillis();
                        invokeAll(subTasks);
                        long earlyEnd = System.currentTimeMillis();
                        System.out.println(alias + " Early " + idx + " " + (earlyEnd - earlyStart));
                        idx++;
                    }
                    long mTimer = System.currentTimeMillis();
                    System.out.println(alias + " material index filter " + (mTimer - f2Timer));
                    // Update statistics in catalog
                    CatalogManager.updateStats(filteredName);
                    preSummary.aliasToFiltered.put(alias, filteredName);
                    int cardinality = CatalogManager.getCardinality(filteredName);
                    if (cardinality == 0) {
                        terminated = true;
                    }
                }
                // Case 3: pos
                else {

                }
            }
            catch (Exception e) {
                e.printStackTrace();
            }
            // Create indexes
            for (ColumnRef queryRef: requiredIndexedCols) {
                // Resolve query-specific column reference
                ColumnRef dbRef = preSummary.columnMapping.get(queryRef);
                long timer1 = System.currentTimeMillis();
                ColumnInfo columnInfo = query.colRefToInfo.get(queryRef);
                String tableName = query.aliasToTable.get(queryRef.aliasName);
                String columnName = queryRef.columnName;
                ColumnRef columnRef = new ColumnRef(tableName, columnName);
                Index index = BufferManager.colToIndex.getOrDefault(columnRef, null);
                PartitionIndex partitionIndex = index == null ? null : (PartitionIndex) index;
                // Get index generation policy according to statistics.
                // Create index (unless it exists already)
                try {
                    partitionIndex(dbRef, queryRef, partitionIndex, columnInfo.isPrimary,
                            false, null);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                long timer2 = System.currentTimeMillis();
                System.out.println("Indexing " + queryRef + " " + (timer2 - timer1));
            }
            long indexTimer = System.currentTimeMillis();
            System.out.println("Finish " + alias);
        }
    }

    /**
     * Search for applicable index and use it to prune rows. Redirect
     * column mappings to index-filtered table if possible.
     *
     * @param query			query to pre-process
     * @param unaryPred		unary predicate on that table
     * @param preSummary	summary of pre-processing steps
     * @return	remaining unary predicate to apply afterwards
     */
    private IndexFilter applyIndex(QueryInfo query, ExpressionInfo unaryPred,
                                  Context preSummary) throws Exception {
        // Divide predicate conjuncts depending on whether they can
        // be evaluated using indices alone.
        IndexTest indexTest = new IndexTest(query);
        List<Expression> indexedConjuncts = new ArrayList<>();
        List<Expression> nonIndexedConjuncts = new ArrayList<>();
        List<Expression> sortedConjuncts = new ArrayList<>();
        List<Expression> unsortedConjuncts = new ArrayList<>();
        Set<String> unsortedColumns = new HashSet<>();
        Set<String> sortedColumns = new HashSet<>();
        Set<String> indexedColumns = new HashSet<>();
        for (Expression conjunct : unaryPred.conjuncts) {
            // Re-initialize index test
            indexTest.canUseIndex = true;
            indexTest.constantQueue.clear();
            indexTest.columnNames.clear();
            indexTest.sorted = true;
            // Compare predicate against indexes
            conjunct.accept(indexTest);
            // Can conjunct be evaluated only from indices?
            if (indexTest.canUseIndex && PreConfig.CONSIDER_INDICES) {
                if (indexTest.sorted) {
                    sortedConjuncts.add(conjunct);
                    sortedColumns.addAll(indexTest.columnNames);
                }
                else {
                    unsortedConjuncts.add(conjunct);
                    unsortedColumns.addAll(indexTest.columnNames);
                }
            } else {
                nonIndexedConjuncts.add(conjunct);
            }
        }
        // Create remaining predicate expression
        if (unsortedConjuncts.size() > 0) {
            indexedConjuncts.addAll(unsortedConjuncts);
            indexedColumns.addAll(unsortedColumns);
            nonIndexedConjuncts.addAll(sortedConjuncts);
        }
        else {
            indexedColumns.addAll(sortedColumns);
            indexedConjuncts.addAll(sortedConjuncts);
        }
        Expression remainingExpr = conjunction(nonIndexedConjuncts);
        // Evaluate indexed predicate part
        if (!indexedConjuncts.isEmpty()) {
            IndexFilter indexFilter = new IndexFilter(query);
            indexFilter.isSameColumn = indexedColumns.size() == 1;
            Expression indexedExpr = conjunction(indexedConjuncts);
            indexedExpr.accept(indexFilter);
            ExpressionInfo remainingInfo;
            if (remainingExpr != null) {
                remainingInfo = new ExpressionInfo(query, remainingExpr);
                indexFilter.remainingInfo = remainingInfo;
            }
            return indexFilter;
        } else {
            IndexFilter indexFilter = new IndexFilter(query);
            indexFilter.lastIndex = null;
            indexFilter.remainingInfo = unaryPred;
            return indexFilter;
        }
    }
    /**
     * Creates a new temporary table containing remaining tuples
     * after applying unary predicates, project on columns that
     * are required for following steps.
     *
     * @param filter		unary predicate filter
     * @param preSummary	summary of pre-processing steps
     */
    private List<Integer> filterProject(IndexFilter filter, List<Integer> rows,
                                        Context preSummary) throws Exception {
        ExpressionInfo unaryPred = filter.remainingInfo;
        // Determine rows satisfying unary predicate
        // Compile unary predicate for fast evaluation
        UnaryBoolEval unaryBoolEval = compilePred(unaryPred, preSummary.columnMapping);
        // Get cardinality of table referenced in predicate
        int cardinality = rows.size();
        // Initialize filter result
        List<Integer> result;
        // Choose between sequential and joining.parallel processing
        if (cardinality <= ParallelConfig.PRE_BATCH_SIZE || !GeneralConfig.isParallel) {
            RowRange allTuples = new RowRange(0, cardinality - 1);
            result = filterBatch(unaryBoolEval, allTuples, rows);
        }
        else {
            // Divide tuples into batches
            List<RowRange> batches = split(cardinality);
            int nrBatches = batches.size();
            result = new ArrayList<>(cardinality);
            List<Integer>[] resultsArray = new ArrayList[nrBatches];
            List<RecursiveAction> subtasks = new ArrayList<>(nrBatches);
            for (int bid = 0; bid < nrBatches; bid++) {
                RowRange batch = batches.get(bid);
                int first = batch.firstTuple;
                int end = batch.lastTuple;
                List<Integer> subResult = new ArrayList<>(end - first + 1);
                int finalBid = bid;
                subtasks.add(new RecursiveAction() {
                    @Override
                    protected void compute() {
                        // Evaluate predicate for each table row
                        for (int rowCtr = first; rowCtr <= end; ++rowCtr) {
                            int rowPos = rows.get(rowCtr);
                            if (unaryBoolEval.evaluate(rowPos) > 0) {
                                subResult.add(rowPos);
                            }
                        }
                        resultsArray[finalBid] = subResult;
                    }
                });
            }
            invokeAll(subtasks);
            for (List<Integer> subResult: resultsArray) {
                result.addAll(subResult);
            }
        }

        return result;
    }
    /**
     * Forms a conjunction between given conjuncts.
     *
     * @param conjuncts	list of conjuncts
     * @return	conjunction between all conjuncts or null
     * 			(iff the input list of conjuncts is empty)
     */
    static Expression conjunction(List<Expression> conjuncts) {
        Expression result = null;
        for (Expression conjunct : conjuncts) {
            if (result == null) {
                result = conjunct;
            } else {
                result = new AndExpression(
                        result, conjunct);
            }
        }
        return result;
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
     * Filters given tuple batch using specified predicate evaluator,
     * return indices of rows within the batch that satisfy the
     * predicate.
     *
     * @param unaryBoolEval	unary predicate evaluator
     * @param rowRange		range of tuple indices of batch
     * @return				list of indices satisfying the predicate
     */
    private List<Integer> filterBatch(UnaryBoolEval unaryBoolEval,
                                     RowRange rowRange, List<Integer> rows) {
        List<Integer> result = new ArrayList<>(rowRange.lastTuple - rowRange.firstTuple);
        // Evaluate predicate for each table row
        for (int rowCtr = rowRange.firstTuple;
             rowCtr <= rowRange.lastTuple; ++rowCtr) {
            int rowPos = rows.get(rowCtr);
            if (unaryBoolEval.evaluate(rowPos) > 0) {
                result.add(rowPos);
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
    private List<RowRange> split(int cardinality) {
        int nrBatches = (int) Math.round((cardinality + 0.0) / ParallelConfig.PRE_INDEX_SIZE);
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
    /**
     * Create an index on the specified column.
     *
     * @param colRef	create index on this column
     */
    public Index partitionIndex(ColumnRef colRef, ColumnRef queryRef, PartitionIndex oldIndex,
                                       boolean isPrimary, boolean isSeq, int[] newPositions) throws Exception {
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
                BufferManager.colToIndex.put(colRef, index);
                return index;
            } else if (data instanceof DoubleData) {
                DoubleData doubleData = (DoubleData)data;
                DoublePartitionIndex doubleIndex = oldIndex == null ? null : (DoublePartitionIndex) oldIndex;
                int keySize = doubleIndex == null ? 0 : doubleIndex.keyToPositions.size();
                IndexPolicy policy = Indexer.indexPolicy(isPrimary, isSeq, keySize, doubleData.cardinality);
                DoublePartitionIndex index = new DoublePartitionIndex(doubleData, nrThreads,
                        colRef, queryRef, doubleIndex, policy);
                BufferManager.colToIndex.put(colRef, index);
                return index;
            }
        }
        return null;
    }
}
