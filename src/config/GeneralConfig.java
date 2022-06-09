package config;

/**
 * Configuration parameters that influence
 * several of the three processing phases.
 * 
 * @author Anonymous
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
	public final static boolean isParallel = true;
	/**
	 * Number of test cases.
	 */
	public static int TEST_CASE = 5;
	/**
	 * Number of test cases.
	 */
	public static int NR_WARMUP = 1;
	/**
	 * Whether to write query results to file
	 */
	public static boolean WRITE_RESULTS = false;
	/**
	 * Whether it is the test case.
	 */
	public static boolean ISTESTCASE = false;
	/**
	 * Whether to test cache.
	 */
	public static boolean TESTCACHE = false;
	/**
	 * Path of JNI library.
	 */
	public static String JNI_PATH = "";
}
