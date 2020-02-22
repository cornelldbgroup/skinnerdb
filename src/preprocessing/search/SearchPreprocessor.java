package preprocessing.search;

import config.LoggingConfig;
import config.NamingConfig;
import config.PreConfig;
import expressions.ExpressionInfo;
import expressions.compilation.UnaryBoolEval;
import indexing.HashIndex;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import operators.Materialize;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.list.primitive.IntList;
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

import static operators.Filter.*;
import static preprocessing.PreprocessorUtil.*;
import static preprocessing.search.FilterSearchConfig.FORGET;
import static preprocessing.search.FilterSearchConfig.ROWS_PER_TIMESTEP;

public class SearchPreprocessor implements Preprocessor {
    /**
     * Whether an error occurred during last invocation.
     * This flag is used in cases where an error occurs
     * without an exception being thrown.
     */
    private boolean hadError = false;

    Map<List<Integer>, UnaryBoolEval> compileCache = null;

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

        ParallelIterate.forEach(query.aliasToTable.keySet(), alias -> {
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
                    ImmutableList<Expression> conjuncts =
                            Lists.immutable.ofAll(curUnaryPred.conjuncts);
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
        }, ParallelService.HIGH_POOL);


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
                } catch (Exception e) {}
                return null;
            }, compiled, 1, ParallelService.HIGH_POOL, false);
        } else {
            for (Expression expression : predicates) {
                compiled.add(compilePred(unaryPred,
                        expression, preSummary.columnMapping));
            }
        }

        long end = System.nanoTime();
        PreStats.compileNanos = end - start;


        List<HashIndex> indices = new ArrayList<>(predicates.size());
        List<Number> values = new ArrayList<>(predicates.size());
        for (Expression expression : predicates) {
            SinglePredicateIndexTest test =
                    new SinglePredicateIndexTest(queryInfo);
            expression.accept(test);
            if (test.canUseIndex) {
                indices.add(test.index);
                values.add(test.constant);
            } else {
                indices.add(null);
                values.add(null);
            }
        }
        IntList satisfyingRows =
                shouldFilter ?
                        filterUCT(tableName, predicates,
                                compiled.toImmutable(), indices,
                                values, unaryPred, preSummary.columnMapping) :
                        IntLists.immutable.empty();

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

    private MutableIntList filterUCT(String tableName,
                                     ImmutableList<Expression> predicates,
                                     ImmutableList<UnaryBoolEval> compiled,
                                     List<HashIndex> indices,
                                     List<Number> values,
                                     ExpressionInfo unaryPred,
                                     Map<ColumnRef, ColumnRef> colMap)
            throws Exception {
        long roundCtr = 0;
        int nrCompiled = compiled.size();
        BudgetedFilter filterOp = new BudgetedFilter(tableName, predicates,
                compiled, indices, values);

        FilterState state = new FilterState(nrCompiled);
        FilterUCTNode root = new FilterUCTNode(filterOp, roundCtr, nrCompiled,
                indices);
        long nextForget = 1;
        long nextCompile = 15;

        while (!filterOp.isFinished()) {
            ++roundCtr;
            state.reset();
            root.sample(roundCtr, state, ROWS_PER_TIMESTEP);

            if (FORGET && roundCtr == nextForget) {
                root = new FilterUCTNode(filterOp, roundCtr, nrCompiled,
                        indices);
                nextForget *= 10;
            }

            if (roundCtr == nextCompile) {
                nextCompile += 100;

                int compileSetSize = predicates.size();
                PriorityQueue<FilterUCTNode> compile =
                        new PriorityQueue<>(compileSetSize,
                                Comparator.comparingInt
                                        (FilterUCTNode::getAddedSavedCalls));
                root.addChildrenToCompile(compile, compileSetSize);
                for (int j = 0; j < compileSetSize; j++) {
                    if (compile.size() == 0) break;
                    FilterUCTNode node = compile.poll();

                    if (node.getCompiledEval() == null) {
                        ParallelService.LOW_POOL.submit(() -> {
                            Expression expr = null;
                            for (int i = node.getIndexPrefixLength();
                                 i < node.getPreds().size(); i++) {
                                if (expr == null) {
                                    expr = predicates.get(
                                            node.getPreds().get(i));
                                } else {
                                    expr = new AndExpression(expr,
                                            predicates.get(
                                                    node.getPreds().get(i)));
                                }
                            }

                            try {
                                node.setCompiledEval(
                                        compilePred(unaryPred, expr, colMap));
                            } catch (Exception e) {}
                        });
                    }

                    node.addChildrenToCompile(compile, compileSetSize);
                }
            }
        }

        return filterOp.getResult();
    }
}
