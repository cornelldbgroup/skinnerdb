package config;

/**
 * Configuration parameters influencing the
 * pre-processing phase.
 * 
 * @author immanueltrummer
 *
 */
public class PreConfig {
	/**
	 * Whether to apply unary predicates for filtering
	 * during pre-processing step.
	 */
	public static final boolean PRE_FILTER = true;
	/**
	 * Whether to write filtered rows in the cache.
	 */
	public static boolean IN_CACHE = true;
}
