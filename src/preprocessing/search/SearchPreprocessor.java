package preprocessing.search;

import config.LoggingConfig;
import config.NamingConfig;
import config.PreConfig;
import expressions.ExpressionInfo;
import expressions.compilation.UnaryBoolEval;
import net.sf.jsqlparser.expression.Expression;
import operators.Materialize;
import preprocessing.Context;
import preprocessing.Preprocessor;
import print.RelationPrinter;
import query.ColumnRef;
import query.QueryInfo;
import statistics.PreStats;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static operators.Filter.compilePred;
import static operators.Filter.loadPredCols;
import static preprocessing.PreprocessorUtil.*;

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
        // Iterate over query aliases
        query.aliasToTable.keySet().parallelStream().forEach(alias -> {
            // Collect required columns (for joins and post-processing) for
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
                    List<Expression> conjuncts = curUnaryPred.conjuncts;
                    filterProject(curUnaryPred, conjuncts, alias,
                            curRequiredCols, preSummary);
                } catch (Exception e) {
                    System.err.println("Error filtering " + alias);
                    e.printStackTrace();
                    hadError = true;
                }
            } else {
                String table = query.aliasToTable.get(alias);
                preSummary.aliasToFiltered.put(alias, table);
            }
        });
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
    private void filterProject(ExpressionInfo unaryPred,
                               List<Expression> predicates, String alias,
                               List<ColumnRef> requiredCols,
                               Context preSummary) throws Exception {
        long startMillis = System.currentTimeMillis();
        log("Filtering and projection for " + alias + " ...");
        String tableName = preSummary.aliasToFiltered.get(alias);
        log("Table name for " + alias + " is " + tableName);

        // Determine rows satisfying unary predicate
        loadPredCols(unaryPred, preSummary.columnMapping);
        List<UnaryBoolEval> compiled = new ArrayList<>(predicates.size());
        for (Expression expression : predicates) {
            compiled.add(compilePred(unaryPred, expression,
                    preSummary.columnMapping));
        }
        List<Integer> satisfyingRows = filterUCT(tableName, compiled);

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
        long totalMillis = System.currentTimeMillis() - startMillis;
        if (LoggingConfig.PRINT_INTERMEDIATES) {
            RelationPrinter.print(filteredName);
        }
    }

    private List<Integer> filterUCT(String tableName,
                                    List<UnaryBoolEval> compiled) {
        long roundCtr = 0;
        int nrCompiled = compiled.size();
        BudgetedFilter filterOp = new BudgetedFilter(tableName, compiled);
        int[] order = new int[nrCompiled];
        FilterUCTNode root = new FilterUCTNode(filterOp, roundCtr, nrCompiled);
        long nextForget = 1;

        boolean FORGET = true;

        while (!filterOp.isFinished()) {
            ++roundCtr;
            int reward = root.sample(roundCtr, order);

            if (FORGET && roundCtr == nextForget) {
                root = new FilterUCTNode(filterOp, roundCtr, nrCompiled);
                nextForget *= 10;
            }
        }

        return filterOp.getResult();
    }
}