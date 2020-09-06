package statistics;

import java.util.List;

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
	 * A list of duration of group by phase in milliseconds for sub queries.
	 */
	public static List<Long> subGroupby;
	/**
	 * A list of duration of aggregation phase in milliseconds for sub queries.
	 */
	public static List<Long> subAggregation;
	/**
	 * A list of duration of having phase in milliseconds for sub queries.
	 */
	public static List<Long> subHaving;
	/**
	 * A list of duration of order phase in milliseconds for sub queries.
	 */
	public static List<Long> subOrder;
}
