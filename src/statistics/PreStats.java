package statistics;

import java.util.ArrayList;
import java.util.List;

/**
 * Statistics about the pre-processing phase.
 * Refers to pre-processing of the last query
 * if the input query is translted into a
 * query sequence during unnesting.
 * 
 * @author Anonymous
 *
 */
public class PreStats {
	/**
	 * Pre-processing time in milliseconds.
	 */
	public static long preMillis = 0;
	/**
	 * Period of filtering columns.
	 */
	public static long filterMillis = 0;
	/**
	 * Period of creating indices.
	 */
	public static long indexMillis = 0;
}
