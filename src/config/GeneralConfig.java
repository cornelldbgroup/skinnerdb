package config;

/**
 * Configuration parameters that influence
 * several of the three processing phases.
 * 
 * @author immanueltrummer
 *
 */
public class GeneralConfig {
	/**
	 * Whether to use in-memory data processing.
	 */
	public static boolean inMemory = true;

	public static boolean reuseUnary = true;

	public static boolean enableTracker = true;

	public static int testQuery = 180;
}
