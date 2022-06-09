package config;

/**
 * Configuration parameters related to
 * the way in which processing is
 * parallelized.
 * 
 * @author Anonymous
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
	public final static int PRE_INDEX_SIZE = 1000;
	/**
	 * Maximal number of tuples if joining.parallel method is applied.
	 */
	public final static int PARALLEL_SIZE = 10000;
	/**
	 * Minimal size of large key column.
	 */
	public final static int LARGE_KEY_SIZE = 100000;
	/**
	 * Whether to collect statistics of all constraints.
	 */
	public final static boolean CONSTRAINTS = false;
	/**
	 * The size of cache for each batch.
	 */
//    public static int MAX_CACHE_SIZE = Integer.MAX_VALUE;
	public static final int MAX_CACHE_SIZE = 2000000;
    /**
	 * Maximal number of tuples per batch during execution.
	 */
	public static int EXE_THREADS = 24;
	/**
	 * Maximal number of executors in the task parallel.
	 */
	public static int NR_EXECUTORS = 1;
	/**
	 * The number of threads per executor.
	 */
	public static int EXE_EXECUTORS = 30;
	/**
	 * Maximal number of tuples per batch during pre-processing.
	 */
	public static int PRE_THREADS = 30;
	/**
	 * The minimal size of sparse columns.
	 */
	public final static int SPARSE_KEY_SIZE = 1000;
	/**
	 * The minimal size of sparse columns.
	 */
	public final static int SPARSE_FILTER_SIZE = 100;
	/**
	 * The minimum size of partitioned table
	 */
	public final static int PARTITION_SIZE = 40000;
//	public final static int PARTITION_SIZE = 3000;
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
	public static final int C = 20;
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
	 * 10: heuristics and learning
	 * 11: DPL
	 * 12: DPM
	 * 13：DPOP
	 * 14: CAPS
	 */
	public static int PARALLEL_SPEC = 14;
	/**
	 * Number of batches.
	 */
	public static int NR_BATCHES = 60;
	/**
	 * Whether to use heavy hitter detection to switch the split table.
	 */
	public static final boolean HEURISTIC_SHARING = true;
	/**
	 * Whether to terminate whenever the thread finishes.
	 */
	public static final boolean HEURISTIC_STOP = false;
	/**
	 * The top level using heuristic/learning
	 */
	public static final int TOP_LEVEL = 2;
	/**
	 * The policy of heuristics:
	 * 0: The minimal cardinality of the table.
	 * 1: minimal joined cardinality.
	 */
	public static final int HEURISTIC_POLICY = 1;
	/**
	 * Number of threads working on search parallelization
	 */
	public static int SEARCH_THREADS = 4;
	/**
	 * Ratio of search and data parallelization
	 */
	public static final double HYBRID_RATIO = 0.2;
}
