package query;

/**
 * Describes what kind of aggregation a query requires.
 * 
 * @author immanueltrummer
 *
 */
public enum AggregationType {
	ALL_ROWS,	// query specifies aggregates but no group-by	 
	GROUPS, 	// query specifies aggregates and group-by
	NONE		// query specifies neither group by nor aggregates
}
