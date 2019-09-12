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
}
