package preprocessing.search;

import catalog.CatalogManager;
import config.LoggingConfig;
import config.NamingConfig;
import config.PreConfig;
import expressions.ExpressionInfo;
import expressions.compilation.UnaryBoolEval;
import indexing.HashIndex;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import operators.Materialize;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.eclipse.collections.impl.factory.primitive.IntLists;
import org.eclipse.collections.impl.parallel.ParallelIterate;
import parallel.ParallelService;
import preprocessing.Context;
import preprocessing.Preprocessor;
import print.RelationPrinter;
import query.ColumnRef;
import query.QueryInfo;
import statistics.PreStats;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import static operators.Filter.*;
import static preprocessing.PreprocessorUtil.*;
import static preprocessing.search.FilterSearchConfig.*;

public class SearchPreprocessor implements Preprocessor {
    /**
     * Whether an error occurred during last invocation.
     * This flag is used in cases where an error occurs
     * without an exception being thrown.
     */
    private boolean hadError = false;

    /**
     * Executes pre-processing.
     *
     * @param query the query to pre-process
     * @return summary of pre-processing steps
     */
    @Override
    public Context process(QueryInfo query) throws Exception {
        // Start counter
        long startMillis = System.currentTimeMillis();
        // Reset error flag
        hadError = false;
        // Collect columns required for joins and post-processing
        Set<ColumnRef> requiredCols = new HashSet<>();
        requiredCols.addAll(query.colsForJoins);
        requiredCols.addAll(query.colsForPostProcessing);
        log("Required columns: " + requiredCols);
        // Initialize pre-processing summary
        Context preSummary = new Context();
        // Initialize mapping for join and post-processing columns
        for (ColumnRef queryRef : requiredCols) {
            preSummary.columnMapping.put(queryRef,
                    DBref(query, queryRef));
        }
        // Initialize column mapping for unary predicate columns
        for (ExpressionInfo unaryPred : query.unaryPredicates) {
            for (ColumnRef queryRef : unaryPred.columnsMentioned) {
                preSummary.columnMapping.put(queryRef,
                        DBref(query, queryRef));
            }
        }
        // Initialize mapping from query alias to DB tables
        preSummary.aliasToFiltered.putAll(query.aliasToTable);
        log("Column mapping:\t" + preSummary.columnMapping.toString());

        final boolean shouldFilter = shouldFilter(query, preSummary);

        for (String alias : query.aliasToTable.keySet()) {
            // Collect required columns (for joins and
            // post-processing) for
            // this table
            List<ColumnRef> curRequiredCols = new ArrayList<>();
            for (ColumnRef requiredCol : requiredCols) {
                if (requiredCol.aliasName.equals(alias)) {
                    curRequiredCols.add(requiredCol);
                }
            }
            // Get applicable unary predicates
            ExpressionInfo curUnaryPred = null;
            for (ExpressionInfo exprInfo : query.unaryPredicates) {
                if (exprInfo.aliasesMentioned.contains(alias)) {
                    curUnaryPred = exprInfo;
                }
            }
            // Filter and project if enabled
            if (curUnaryPred != null && PreConfig.PRE_FILTER) {
                try {
                    //curUnaryPred = new ExpressionInfo(query,
                    //        curUnaryPred.finalExpression, false);
                    ImmutableList<Expression> conjuncts = Lists.immutable.ofAll(
                            curUnaryPred.conjuncts);
                    filterProject(query, curUnaryPred, conjuncts, alias,
                            curRequiredCols, preSummary, shouldFilter);
                } catch (Exception e) {
                    System.err.println("Error filtering " + alias);
                    e.printStackTrace();
                    hadError = true;
                }
            } else {
                String table = query.aliasToTable.get(alias);
                preSummary.aliasToFiltered.put(alias, table);
            }
        }


        // Abort pre-processing if filtering error occurred
        if (hadError) {
            throw new Exception("Error in pre-processor.");
        }

        // Create missing indices for columns involved in equi-joins.
        log("Creating indices ...");

        createJoinIndices(query, preSummary);
        // Measure processing time
        PreStats.preMillis = System.currentTimeMillis() - startMillis;
        return preSummary;
    }

    private boolean shouldFilter(QueryInfo query, Context preSummary)
            throws Exception {
        boolean alwaysFalseExpression = false;
        for (ExpressionInfo exprInfo : query.unaryPredicates) {
            if (exprInfo.aliasesMentioned.isEmpty()) {
                UnaryBoolEval eval = interpretPred(exprInfo,
                        exprInfo.finalExpression, preSummary.columnMapping);
                if (eval.evaluate(0) <= 0) {
                    alwaysFalseExpression = true;
                    break;
                }
            }
        }
        return !alwaysFalseExpression;
    }

    /**
     * Creates a new temporary table containing remaining tuples
     * after applying unary predicates, project on columns that
     * are required for following steps.
     *
     * @param predicates   predicates to filter
     * @param alias        alias of table to filter
     * @param requiredCols project on those columns
     * @param preSummary   summary of pre-processing steps
     */
    private void filterProject(QueryInfo queryInfo, ExpressionInfo unaryPred,
                               ImmutableList<Expression> predicates,
                               String alias,
                               List<ColumnRef> requiredCols,
                               Context preSummary,
                               boolean shouldFilter) throws Exception {
        log("Filtering and projection for " + alias + " ...");
        String tableName = preSummary.aliasToFiltered.get(alias);
        log("Table name for " + alias + " is " + tableName);

        // Determine rows satisfying unary predicate
        loadPredCols(unaryPred, preSummary.columnMapping);

        MutableList<UnaryBoolEval> compiled =
                Lists.mutable.ofInitialCapacity(predicates.size());
        long start = System.nanoTime();
        if (predicates.size() >= 5) {
            // parallel compile them bc thread overheads are bad
            ParallelIterate.collect(predicates, expression -> {
                try {
                    return compilePred(unaryPred,
                            expression,
                            preSummary.columnMapping);
                } catch (Exception e) {
                    return null;
                }
            }, compiled, 1, ParallelService.POOL, false);
        } else {
            for (Expression expression : predicates) {
                compiled.add(compilePred(unaryPred,
                        expression, preSummary.columnMapping));
            }
        }

        long end = System.nanoTime();
        PreStats.compileNanos = end - start;


        List<HashIndex> indices = new ArrayList<>(predicates.size());
        List<Integer> values = new ArrayList<>(predicates.size());
        for (Expression expression : predicates) {
            SinglePredicateIndexTest test =
                    new SinglePredicateIndexTest(queryInfo);
            expression.accept(test);
            if (test.canUseIndex) {
                indices.add(test.index);
                @SuppressWarnings("unchecked")
                int dataLoc = test.index.getDataLocation(test.constant);
                values.add(dataLoc);
            } else {
                indices.add(null);
                values.add(null);
            }
        }
        Collection<MutableIntList> satisfyingRows =
                shouldFilter ?
                        filterUCTNaiveParallel(tableName, predicates,
                                compiled.toImmutable(), indices,
                                values, unaryPred, preSummary.columnMapping) :
                        Arrays.asList();

        // Materialize relevant rows and columns
        String filteredName = NamingConfig.FILTERED_PRE + alias;
        List<String> columnNames = new ArrayList<>();
        for (ColumnRef colRef : requiredCols) {
            columnNames.add(colRef.columnName);
        }
        Materialize.execute(tableName, columnNames,
                satisfyingRows, null, filteredName, true);

        // Update pre-processing summary
        for (ColumnRef srcRef : requiredCols) {
            String columnName = srcRef.columnName;
            ColumnRef resRef = new ColumnRef(filteredName, columnName);
            preSummary.columnMapping.put(srcRef, resRef);
        }
        preSummary.aliasToFiltered.put(alias, filteredName);
        if (LoggingConfig.PRINT_INTERMEDIATES) {
            RelationPrinter.print(filteredName);
        }
    }

    private Collection<MutableIntList> filterUCTSingleThreaded(
            String tableName,
            ImmutableList<Expression> predicates,
            ImmutableList<UnaryBoolEval> compiled,
            List<HashIndex> indices,
            List<Integer> dataLocations,
            ExpressionInfo unaryPred,
            Map<ColumnRef, ColumnRef> colMap) throws Exception {
        ConcurrentHashMap<List<Integer>, UnaryBoolEval> cache =
                new ConcurrentHashMap<>();
        int nrCompiled = compiled.size();
        long nextCompile = 75;
        final int CARDINALITY = CatalogManager.getCardinality(tableName);
        long roundCtr = 0;
        IndexFilter indexFilter = new IndexFilter(indices, dataLocations);
        FilterUCTNode root = new FilterUCTNode(roundCtr,
                nrCompiled, indices);
        int lastCompletedRow = 0;
        List<MutableIntList> resultList = new ArrayList<>();
        BudgetedFilter filterOp = new BudgetedFilter(compiled, indexFilter,
                CARDINALITY, resultList);

        while (lastCompletedRow < CARDINALITY) {
            ++roundCtr;

            final FilterState state = new FilterState(nrCompiled);
            Pair<FilterUCTNode, Boolean> sample = root.sample(roundCtr,
                    state, cache);
            final FilterUCTNode selected = sample.getLeft();
            boolean playedOut = sample.getRight();

            final int start = lastCompletedRow;
            final int end;
            if (playedOut) {
                state.batchSize = ROWS_PER_TIMESTEP;
                end = Math.min(start + ROWS_PER_TIMESTEP, CARDINALITY);
            } else {
                state.batchSize = LEAF_ROWS_PER_TIMESTEP;
                end = Math.min(start + state.batches * LEAF_ROWS_PER_TIMESTEP,
                        CARDINALITY);
            }
            lastCompletedRow = end;
            FilterUCTNode.initialUpdateStatistics(selected, state);
            List<Integer> outputId = initializeEpoch(resultList,
                    state.batches);
            double reward = filterOp.execute(start, outputId, state);
            FilterUCTNode.finalUpdateStatistics(selected, state, reward);

            if (ENABLE_COMPILATION && roundCtr == nextCompile) {
                nextCompile *= 2;

                int compileSetSize = predicates.size();
                HashMap<FilterUCTNode, Integer> savedCalls = new HashMap<>();
                root.initializeUtility(savedCalls, cache);
                for (int j = 0; j < Math.min(compileSetSize,
                        savedCalls.size()); j++) {

                    FilterUCTNode node = null;
                    int maxSavedCalls = -1;
                    for (Map.Entry<FilterUCTNode, Integer> entry :
                            savedCalls.entrySet()) {
                        if (entry.getValue() > maxSavedCalls) {
                            maxSavedCalls = entry.getValue();
                            node = entry.getKey();
                        }
                    }
                    node.updateUtility(savedCalls, cache);

                    final List<Integer> preds = node.getChosenPreds();
                    ParallelService.POOL.submit(() -> {
                        Expression expr = null;
                        for (int i = preds.size() - 1; i >= 0; i--) {
                            if (expr == null) {
                                expr = predicates.get(preds.get(i));
                            } else {
                                expr = new AndExpression(expr,
                                        predicates.get(preds.get(i)));
                            }
                        }

                        try {
                            cache.put(preds, compilePred(unaryPred, expr,
                                    colMap));
                        } catch (Exception e) {}
                    });
                }
            }
        }

        return resultList;
    }

    private Collection<MutableIntList> filterUCTNaiveParallel(
            String tableName,
            ImmutableList<Expression> predicates,
            ImmutableList<UnaryBoolEval> compiled,
            List<HashIndex> indices,
            List<Integer> dataLocations,
            ExpressionInfo unaryPred,
            Map<ColumnRef, ColumnRef> colMap) throws Exception {
        int nrCompiled = compiled.size();
        final int CARDINALITY = CatalogManager.getCardinality(tableName);
        List<MutableIntList> finalResult = new ArrayList<>();
        List<Future<List<MutableIntList>>> futures = new ArrayList<>();

        int parallelBatchSize = (int) Math.ceil(CARDINALITY /
                (double) ParallelService.POOL_THREADS);

        for (int batch = 0; batch < ParallelService.POOL_THREADS; batch++) {
            final int index = batch;
            final int nextStart = 0 + parallelBatchSize * index;
            final int nextEnd = Math.min(nextStart + parallelBatchSize,
                    CARDINALITY);
            if (nextStart >= CARDINALITY) break;

            futures.add(ParallelService.POOL.submit(() -> {
                Map<List<Integer>, UnaryBoolEval> cache =
                        Collections.synchronizedMap(new HashMap<>());
                int startRow = nextStart;

                long nextCompile = 75;
                long roundCtr = 0;
                IndexFilter indexFilter = new IndexFilter(indices,
                        dataLocations);
                FilterUCTNode root = new FilterUCTNode(roundCtr,
                        nrCompiled, indices);
                List<MutableIntList> resultList = new ArrayList<>();
                BudgetedFilter filterOp = new BudgetedFilter(compiled,
                        indexFilter,
                        CARDINALITY, resultList);

                while (startRow < nextEnd) {
                    ++roundCtr;

                    final FilterState state = new FilterState(nrCompiled);
                    Pair<FilterUCTNode, Boolean> sample = root.sample(roundCtr,
                            state, cache);
                    final FilterUCTNode selected = sample.getLeft();
                    boolean playedOut = sample.getRight();

                    final int start = startRow;
                    final int end;
                    if (playedOut) {
                        state.batchSize = ROWS_PER_TIMESTEP;
                        end = Math.min(start + ROWS_PER_TIMESTEP, CARDINALITY);
                    } else {
                        state.batchSize = LEAF_ROWS_PER_TIMESTEP;
                        end = Math.min(start + state.batches *
                                LEAF_ROWS_PER_TIMESTEP, CARDINALITY);
                    }
                    startRow = end;
                    FilterUCTNode.initialUpdateStatistics(selected, state);
                    List<Integer> outputId = initializeEpoch(resultList,
                            state.batches);
                    double reward = filterOp.execute(start, outputId, state);
                    FilterUCTNode.finalUpdateStatistics(selected, state,
                            reward);

                    if (ENABLE_COMPILATION && roundCtr == nextCompile) {
                        nextCompile *= 2;

                        int compileSetSize = predicates.size();
                        HashMap<FilterUCTNode, Integer> savedCalls =
                                new HashMap<>();
                        root.initializeUtility(savedCalls, cache);
                        for (int j = 0; j < Math.min(compileSetSize,
                                savedCalls.size()); j++) {

                            FilterUCTNode node = null;
                            int maxSavedCalls = -1;
                            for (Map.Entry<FilterUCTNode, Integer> entry :
                                    savedCalls.entrySet()) {
                                if (entry.getValue() > maxSavedCalls) {
                                    maxSavedCalls = entry.getValue();
                                    node = entry.getKey();
                                }
                            }
                            savedCalls.remove(node);
                            node.updateUtility(savedCalls, cache);

                            final List<Integer> preds = node.getChosenPreds();
                            ParallelService.POOL.submit(() -> {
                                Expression expr = null;
                                for (int i = preds.size() - 1; i >= 0; i--) {
                                    if (expr == null) {
                                        expr = predicates.get(preds.get(i));
                                    } else {
                                        expr = new AndExpression(expr,
                                                predicates.get(preds.get(i)));
                                    }
                                }

                                try {
                                    cache.put(preds, compilePred(unaryPred,
                                            expr,
                                            colMap));
                                } catch (Exception e) {}
                            });
                        }
                    }
                }
                return resultList;
            }));
        }

        for (Future<List<MutableIntList>> future : futures) {
            finalResult.addAll(future.get());
        }

        return finalResult;
    }

    public List<Integer> initializeEpoch(List<MutableIntList> resultList,
                                         int numEpochs) {
        List<Integer> outputIds = new ArrayList<>(numEpochs);

        for (int i = 0; i < numEpochs; i++) {
            resultList.add(IntLists.mutable.empty());
            outputIds.add(resultList.size() - 1);
        }

        return outputIds;
    }
}
