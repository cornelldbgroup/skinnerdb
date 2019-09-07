package statistics;

/**
 * Statistics about the join phase - most statistics
 * refer to counts achieved during the last query
 * evaluation.
 * 
 * @author immanueltrummer
 *
 */
public class JoinStats {
    /**
     * Number of (complete and partial) tuples considered
     * during the last invocation.
     */
    public static long nrTuples = 0;
    /**
     * Number of index lookups during the last invocation.
     */
    public static long nrIndexLookups = 0;
    /**
     * Sum of index entries for the values used in index lookups
     * (e.g., useful for determining the benefit of faster
     * search methods on index entries for the same value).
     */
    public static long nrIndexEntries = 0;
    /**
     * Number of index lookups where the number of corresponding
     * entries is at most one (useful for determining the benefit
     * of specialized indexing methods for unique value columns).
     */
    public static long nrUniqueIndexLookups = 0;
    /**
     * Number of main loop iterations in the last invocation.
     */
    public static long nrIterations = 0;
    /**
     * Number of UCT nodes generated in the last invocation.
     */
    public static long nrUctNodes = 0;
    /**
     * Number of query plans tried during last invocation.
     */
    public static long nrPlansTried = 0;
    /**
     * Average reward obtained during last invocation.
     */
    public static double avgReward = -1;
    /**
     * Maximum reward obtained during last invocation.
     */
    public static double maxReward = -1;
    /**
     * Number of UCT samples taken in last iteration.
     */
    public static long nrSamples = 0;
    /**
     * Total work (including redundant work) done during join
     * (calculated based on table offsets after query evaluation).
     */
    public static double totalWork = 0;
    /**
     * Whether to load data from the disk
     */
    public static boolean cacheMiss = false;
    /**
     * Number of cache miss.
     */
    public static int nrCacheMiss = 0;
    /**
     * Number of round count.
     */
    public static int nrRounds = 0;
    /**
     * current join order.
     */
    public static int[] order;
    /**
     * Number of episodes.
     */
    public static long roundCtr;
}
