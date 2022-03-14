package config;

/**
 * Configuration parameters influencing the
 * pre-processing phase.
 * 
 * @author Anonymous
 *
 */
public class PreConfig {
	/**
	 * Whether to apply unary predicates for filtering
	 * during pre-processing step.
	 */
	public static boolean FILTER = true;
	public static final boolean PRE_FILTER = true;
	/**
	 * Whether to consider using indices for evaluating
	 * unary equality predicates.
	 */
	public static final boolean CONSIDER_INDICES = true;
	/**
	 * Whether to write filtered rows in the cache.
	 */
	public static boolean IN_CACHE = false;
	/**
	 * Whether to write filtered rows in the cache.
	 */
	public static final boolean PROCESS_KEYS = false;
	/**
	 * Whether to create index for groupby clause.
	 */
	public static boolean GROUP_INDEX = true;
}
