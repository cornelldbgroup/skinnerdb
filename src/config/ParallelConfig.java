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
	 * Minimal admissible table that can be selected to split.
	 */
	public final static int MIN_SPLIT_SIZE = 50000;
	/**
	 * Number of threads in the join phase.
	 */
	public static int JOIN_THREADS = 30;

}
