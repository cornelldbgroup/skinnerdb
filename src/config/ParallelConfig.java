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
	 * Whether to collect statistics of all constraints.
	 */
	public final static boolean CONSTRAINTS = false;
	/**
	 * Maximal number of tuples per batch during execution.
	 */
	public static int EXE_THREADS = 1;
	/**
	 * Maximal number of tuples per batch during pre-processing.
	 */
	public static int PRE_THREADS = 30;
	/**
	 * The minimal size of sparse columns.
	 */
	public final static int SPARSE_KEY_SIZE = 10000;
	/**
	 * The minimal size of sparse columns.
	 */
	public final static int SPARSE_FILTER_SIZE = 100;
	/**
	 * The minimum size of partitioned table
	 */
	public final static int PARTITION_SIZE = 50000;
	/**
	 * The maximum size of statistics
	 */
	public final static int STATISTICS_SIZE = 20;
	/**
	 * Whether to assign constraint per thread.
	 */
	public final static boolean CONSTRAINT_PER_THREAD = true;
	/**
	 * The base of round counts to assign a new best join order
	 * to executor thread.
	 */
	public static final int C = 100;
	/**
	 * Parallel specification:
	 * 0: DPDasync
	 * 1: DPDsync
	 * 2: PSS
	 * 3: PSA
	 * 4: Root parallelization
	 * 5: Leaf parallelization
	 * 6: Tree parallelization
	 * 7: Extended PSS
	 * 8: New Adaptive Partition
	 * 9: One search thread and multiple executor threads
	 */
	public static int PARALLEL_SPEC = 8;

	public static final boolean HEURISTIC_SHARING = true;
	public static final boolean HEURISTIC_STOP = false;
}
