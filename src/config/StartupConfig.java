package config;

/**
 * Configures behavior of SkinnerDB during startup.
 * 
 * @author immanueltrummer
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
	public static final boolean WARMUP_RUN = false;
}
