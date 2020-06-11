package config;

/**
 * Configuration parameters related to
 * the way in which processing is
 * parallelized.
 * 
 * @author immanueltrummer
 *
 */
public class ParallelConfig {
	/**
	 * Whether to parallelize.
	 */
	public static final boolean PARALLEL = true;
	/**
	 * Maximal number of tuples per batch during pre-processing.
	 */
	public final static int PRE_BATCH_SIZE = 1000;
	/**
	 * Number of executed threads during execution.
	 */
	public static int EXE_THREADS = 1;
}
