package config;

/**
 * Configures run time checks (useful for
 * debugging).
 * 
 * @author immanueltrummer
 *
 */
public class CheckConfig {
	/**
	 * Whether to check indices after generation.
	 */
	public static boolean CHECK_INDICES = false;
	/**
	 * Whether to check iterators created for
	 * leap-frog trie join.
	 */
	public static boolean CHECK_LFTJ_ITERS = true;
}
