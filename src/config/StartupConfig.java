package config;

/**
 * Configures behavior of SkinnerMT during startup.
 * 
 * @author Anonymous
 *
 */
public class StartupConfig {
	/**
	 * How to select columns on which to create indices at startup.
	 */
	public static final IndexingMode INDEX_CRITERIA = IndexingMode.ALL;
	/**
	 * Whether to have extra run for warm up.
	 */
	public static final boolean WARMUP_RUN = true;
	/**
	 * Whether to test memory.
	 */
    public static boolean Memory = false;
}
