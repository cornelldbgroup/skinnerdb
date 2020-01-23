package preprocessing.search;

import expressions.ExpressionInfo;
import preprocessing.Context;
import preprocessing.Preprocessor;
import query.ColumnRef;
import query.QueryInfo;
import search.Agent;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static preprocessing.PreprocessorUtil.DBref;
import static preprocessing.PreprocessorUtil.log;

public class SearchPreprocessor implements Preprocessor, Agent<TableAction> {
    /**
     * Whether an error occurred during last invocation.
     * This flag is used in cases where an error occurs
     * without an exception being thrown.
     */
    private boolean hadError = false;

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

        preSummary.aliasToFiltered.putAll(query.aliasToTable);

        System.out.println(query.unaryPredicates);
        System.exit(1);

        return preSummary;
    }

    @Override
    public double simulate(List<TableAction> action) {
        return 0;
    }
}
