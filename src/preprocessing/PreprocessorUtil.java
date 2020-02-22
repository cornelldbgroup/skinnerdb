package preprocessing;

import config.LoggingConfig;
import indexing.Indexer;
import org.eclipse.collections.impl.parallel.ParallelIterate;
import parallel.ParallelService;
import query.ColumnRef;
import query.QueryInfo;

public class PreprocessorUtil {
    /**
     * Translates a column reference using a table
     * alias into one using the original table.
     *
     * @param query    meta-data about query
     * @param queryRef reference to alias column
     * @return resolved column reference
     */
    public static ColumnRef DBref(QueryInfo query, ColumnRef queryRef) {
        String alias = queryRef.aliasName;
        String table = query.aliasToTable.get(alias);
        String colName = queryRef.columnName;
        return new ColumnRef(table, colName);
    }

    /**
     * Create indices on equality join columns if not yet available.
     *
     * @param query      query for which to create indices
     * @param preSummary summary of pre-processing steps executed so far
     * @throws Exception
     */
    public static void createJoinIndices(QueryInfo query, Context preSummary) {
        // Iterate over columns in equi-joins
        long startMillis = System.currentTimeMillis();
        ParallelIterate.forEach(query.equiJoinCols, queryRef -> {
            try {
                // Resolve query-specific column reference
                ColumnRef dbRef =
                        preSummary.columnMapping.get(queryRef);
                log("Creating index for " + queryRef +
                        " (query) - " + dbRef + " (DB)");
                // Create index (unless it exists already)
                Indexer.index(dbRef);
            } catch (Exception e) {
                System.err.println("Error creating index for " + queryRef);
                e.printStackTrace();
            }
        }, ParallelService.HIGH_POOL);
        long totalMillis = System.currentTimeMillis() - startMillis;
        log("Created all indices in " + totalMillis + " ms.");
    }

    /**
     * Output logging message if pre-processing logging activated.
     *
     * @param toLog text to display if logging is activated
     */
    public static void log(String toLog) {
        if (LoggingConfig.PREPROCESSING_VERBOSE) {
            System.out.println(toLog);
        }
    }
}
