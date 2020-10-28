package statistics;

import java.util.ArrayList;
import java.util.List;

/**
 * Statistics about the join phase - most statistics
 * refer to counts achieved during the last query
 * evaluation.
 * 
 * @author Anonymous
 *
 */
public class JoinStats {
	/**
	 * Duration of join phase in milliseconds.
	 */
	public static long joinMillis = 0;
    /**
     * Duration of materialization phase in milliseconds.
     */
    public static long matMillis = 0;
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
     * Join result cardinality of last processed sub-query.
     */
    public static int lastJoinCard = -1;
    /**
     * Total execution time
     */
    public static long exeTime = 0;
    /**
     * Total time of merging results tuples
     */
    public static long mergeTime = 0;
    /**
     * Size of temporary tables and indexes.
     */
    public static long dataSize = 0;
    /**
     * Size of uct tree.
     */
    public static long treeSize = 0;
    /**
     * Size of progress tracker.
     */
    public static long stateSize = 0;
    /**
     * Size of join algorithm.
     */
    public static long joinSize = 0;
    public static List<Integer> nrJoined = new ArrayList<>();
}
