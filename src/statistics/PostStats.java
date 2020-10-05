package statistics;

import java.util.List;

/**
 * Statistics about post-processing phase.
 * Refers to post-processing of last query if
 * the input query is translated into a sequence
 * of queries during unnesting.
 * 
 * @author Anonymous
 *
 */
public class PostStats {
	/**
	 * Duration of post-processing phase in milliseconds.
	 */
	public static long postMillis = 0;
	/**
	 * Duration of GroupBy phase in milliseconds.
	 */
	public static long groupByMillis = 0;
	/**
	 * Duration of aggregation phase in milliseconds.
	 */
	public static long aggMillis = 0;
	/**
	 * Duration of having phase in milliseconds.
	 */
	public static long havingMillis = 0;
	/**
	 * Duration of order phase in milliseconds.
	 */
	public static long orderMillis = 0;
}
