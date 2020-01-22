package preprocessing;

import config.LoggingConfig;
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
    static ColumnRef DBref(QueryInfo query, ColumnRef queryRef) {
        String alias = queryRef.aliasName;
        String table = query.aliasToTable.get(alias);
        String colName = queryRef.columnName;
        return new ColumnRef(table, colName);
    }

    /**
     * Output logging message if pre-processing logging activated.
     *
     * @param toLog text to display if logging is activated
     */
    static void log(String toLog) {
        if (LoggingConfig.PREPROCESSING_VERBOSE) {
            System.out.println(toLog);
        }
    }
}
