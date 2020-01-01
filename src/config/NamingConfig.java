package config;

/**
 * Configuration parameter concerning naming
 * for intermediate result tables and columns.
 * 
 * @author immanueltrummer
 *
 */
public class NamingConfig {
	/**
	 * Prefix for naming anonymous sub-queries during unnesting.
	 */
	public static final String SUBQUERY_PRE = "subquery.";
	/**
	 * Prefix for naming columns that are created during unnesting.
	 */
	public static final String SUBQUERY_COL_PRE = "subquerycol.";
	/**
	 * Prefix to add to table names after initial, index-based filtering.
	 */
	public static final String IDX_FILTERED_PRE = "indexfiltered.";
	/**
	 * Prefix to add to table names after filtering them.
	 */
	public static final String FILTERED_PRE = "filtered.";
	/**
	 * Name of table containing result after join phase.
	 */
	public static final String JOINED_NAME = "joined.";
	/**
	 * Name of table containing for each join result row
	 * the associated group (and potentially columns used
	 * for calculating group IDs).
	 */
	public static final String GROUPS_TBL_NAME = "groups.results";
	/**
	 * Prefix of name of a column that contains data for group by.
	 */
	public static final String GROUPS_SRC_COL_PRE = "groups.source";
	/**
	 * Name of column containing group IDs for join result rows.
	 */
	public static final String GROUPS_COL_NAME = "groups.result";
	/**
	 * Name of table holding results of expressions over which
	 * we aggregate.
	 */
	public static final String AGG_SRC_TBL_NAME = "aggregates.sources";
	/**
	 * Prefix of name of column holding input data for aggregate.
	 */
	public static final String AGG_SRC_COL_PRE = "aggregate.source";
	/**
	 * Name of table holding the results for query aggregates.
	 */
	public static final String AGG_TBL_NAME = "aggregates.";
	/**
	 * Prefix of columns holding the result of query aggregates.
	 */
	public static final String AGG_COL_PRE = "aggregate.result";
	/**
	 * Name of table containing intermediate result: groups not
	 * satisfying HAVING clause are not yet filtered out.
	 */
	public static final String RESULT_NO_HAVING = "nohavingresult.";
	/**
	 * Name of table containing columns to order by before
	 * filtering groups via the HAVING clause.
	 */
	public static final String ORDER_NO_HAVING = "nohavingorder.";
	/**
	 * Name of table containing result of evaluating having clause.
	 */
	public static final String HAVING_TBL_NAME = "having.";
	/**
	 * Name of column containing result of evaluating having clause.
	 */
	public static final String HAVING_COL_NAME = "having.";
	/**
	 * Name of table containing columns to order by.
	 */
	public static final String ORDER_NAME = "order.";
	/**
	 * Name of table in which the result is stored before
	 * applying the LIMIT clause (if a LIMIT clause is
	 * specified).
	 */
	public static final String PRE_LIMIT_TBL = "prelimit.";
	/**
	 * Name of the table containing final query result.
	 */
	public static final String FINAL_RESULT_NAME = "result.";
	/**
	 * Name of the table containing test query result.
	 */
	public static final String TEST_RESULT_NAME = "test.";
}
