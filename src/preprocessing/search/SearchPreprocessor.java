package preprocessing.search;

import config.PreConfig;
import expressions.ExpressionInfo;
import indexing.Indexer;
import net.sf.jsqlparser.expression.Expression;
import preprocessing.Context;
import preprocessing.Preprocessor;
import query.ColumnRef;
import query.QueryInfo;
import statistics.PreStats;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static preprocessing.PreprocessorUtil.DBref;
import static preprocessing.PreprocessorUtil.log;

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
        Set<ColumnRef> requiredCols = new HashSet<ColumnRef>();
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
            List<ColumnRef> curRequiredCols = new ArrayList<ColumnRef>();
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
                    filterProject(conjuncts, alias, curRequiredCols,
                            preSummary);
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
    private void filterProject(List<Expression> predicates, String alias,
                               List<ColumnRef> requiredCols,
                               Context preSummary) throws Exception {
        long startMillis = System.currentTimeMillis();
        log("Filtering and projection for " + alias + " ...");
        String tableName = preSummary.aliasToFiltered.get(alias);
        log("Table name for " + alias + " is " + tableName);

        /*
        // Determine rows satisfying unary predicate
        List<Integer> satisfyingRows = Filter.executeToList(
                unaryPred, tableName, preSummary.columnMapping);
        // Materialize relevant rows and columns
        String filteredName = NamingConfig.FILTERED_PRE + alias;
        List<String> columnNames = new ArrayList<String>();
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
        // Print out intermediate result table if logging is enabled
        if (LoggingConfig.PRINT_INTERMEDIATES) {
            RelationPrinter.print(filteredName);
        } */
    }

    /**
     * Create indices on equality join columns if not yet available.
     *
     * @param query      query for which to create indices
     * @param preSummary summary of pre-processing steps executed so far
     * @throws Exception
     */
    private void createJoinIndices(QueryInfo query, Context preSummary)
            throws Exception {
        // Iterate over columns in equi-joins
        long startMillis = System.currentTimeMillis();
        query.equiJoinCols.parallelStream().forEach(queryRef -> {
            try {
                // Resolve query-specific column reference
                ColumnRef dbRef = preSummary.columnMapping.get(queryRef);
                log("Creating index for " + queryRef +
                        " (query) - " + dbRef + " (DB)");
                // Create index (unless it exists already)
                Indexer.index(dbRef);
            } catch (Exception e) {
                System.err.println("Error creating index for " + queryRef);
                e.printStackTrace();
            }
        });
        long totalMillis = System.currentTimeMillis() - startMillis;
        log("Created all indices in " + totalMillis + " ms.");
    }
}