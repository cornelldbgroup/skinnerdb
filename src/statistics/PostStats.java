package statistics;

/**
 * Statistics about post-processing phase.
 * Refers to post-processing of last query if
 * the input query is translated into a sequence
 * of queries during unnesting.
 * 
 * @author immanueltrummer
 *
 */
public class PostStats {
	/**
	 * Duration of post-processing phase in milliseconds.
	 */
	public static long postMillis = 0;
	/**
	 * Duration of groupby phase in milliseconds.
	 */
	public static long groupbyMillis = 0;
	/**
	 * Duration of aggregate phase in milliseconds.
	 */
	public static long aggregateMillis = 0;
	/**
	 * Duration of having phase in milliseconds.
	 */
	public static long havingMillis = 0;
	/**
	 * Duration of order by phase in milliseconds.
	 */
	public static long orderMillis = 0;

	/**
	 * Initialize some post-processing statistics.
	 */
	public static void initializePostStats() {
		postMillis = 0;
		groupbyMillis = 0;
		aggregateMillis = 0;
		havingMillis = 0;
		orderMillis = 0;
	}
}
