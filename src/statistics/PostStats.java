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
	 * A list of duration of post-processing phase in milliseconds for sub queries.
	 */
	public static List<Long> subPostMillis;
}
