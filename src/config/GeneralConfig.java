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
	public static boolean isParallel = true;
	/**
	 * Whether to apply parallel methods.
	 */
	public static boolean parallelPre = true;
	/**
	 * Whether to apply parallel methods.
	 */
	public static boolean parallelPost = true;
	/**
	 * Number of test cases.
	 */
	public static int TEST_CASE = 1;
	/**
	 * Whether it is the test case.
	 */
	public static boolean ISTESTCASE = false;
}
