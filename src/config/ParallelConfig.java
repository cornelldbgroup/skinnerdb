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
	public final static int PRE_BATCH_SIZE = 1000;
	/**
	 * Maximal number of tuples per batch during index generation.
	 */
	public final static int PRE_INDEX_SIZE = 100000;
	/**
	 * Maximal number of tuples if joining.parallel method is applied.
	 */
	public final static int PARALLEL_SIZE = 10000;
	/**
	 * Maximal number of tuples per batch during pre-processing.
	 */
	public static int EXE_THREADS = 20;
	/**
	 * The minimal size of sparse columns.
	 */
	public static int SPARSE_KEY_SIZE = 10000;
}
