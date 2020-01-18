package statistics;

/**
 * Statistics about the pre-processing phase.
 * Refers to pre-processing of the last query
 * if the input query is translted into a
 * query sequence during unnesting.
 * 
 * @author immanueltrummer
 *
 */
public class PreStats {
	/**
	 * Milliseconds spent with filtering and projections
	 * (i.e., pre-processing time without overhead due to
	 * creating join indices).
	 */
	public static long filterProjectMillis = 0;
	/**
	 * Preprocessing time in milliseconds.
	 */
	public static long preMillis = 0;
}
