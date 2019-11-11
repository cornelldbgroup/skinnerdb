package statistics;

import java.util.ArrayList;
import java.util.List;

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
	 * Preprocessing time in milliseconds.
	 */
	public static long preMillis = 0;
	/**
	 * Period of filtering columns.
	 */
	public static long filterTime = 0;
	/**
	 * Period of creating indices.
	 */
	public static long indexTime = 0;
	/**
	 * A list of Preprocessing time in milliseconds for sub-queries.
	 */
	public static List<Long> subPreMillis;
}
