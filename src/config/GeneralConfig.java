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
	/**
	 * Whether to apply parallel methods.
	 */
	public static boolean isParallel = false;
}
