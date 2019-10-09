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
	 * Maximal number of tuples per batch during pre-processing.
	 */
	public final static int PRE_BATCH_SIZE = 100000;
	/**
	 * Maximal number of tuples per batch during pre-processing.
	 */
	public static int EXE_THREADS = 20;
	/**
	 * Minimal size of post-processing batch.
	 */
	public static final int POST_SIZES = 50;
}
