package joining.parallel.statistics;


public class StatsInstance {
    /**
     * Number of (complete and partial) tuples considered
     * during the last invocation.
     */
    public long nrTuples = 0;
    /**
     * Number of index lookups during the last invocation.
     */
    public long nrIndexLookups = 0;
    /**
     * Sum of index entries for the values used in index lookups
     * (e.g., useful for determining the benefit of faster
     * search methods on index entries for the same value).
     */
    public long nrIndexEntries = 0;
    /**
     * Number of index lookups where the number of corresponding
     * entries is at most one (useful for determining the benefit
     * of specialized indexing methods for unique value columns).
     */
    public long nrUniqueIndexLookups = 0;
    /**
     * Number of main loop iterations in the last invocation.
     */
    public long nrIterations = 0;
    /**
     * Number of UCT nodes generated in the last invocation.
     */
    public long nrUctNodes = 0;
    /**
     * Number of query plans tried during last invocation.
     */
    public long nrPlansTried = 0;
    /**
     * Average reward obtained during last invocation.
     */
    public double avgReward = -1;
    /**
     * Maximum reward obtained during last invocation.
     */
    public double maxReward = -1;
    /**
     * Number of UCT samples taken in last iteration.
     */
    public long nrSamples = 0;
    /**
     * Total work (including redundant work) done during join
     * (calculated based on table offsets after query evaluation).
     */
    public double totalWork = 0;

    public StatsInstance() {}

    public void reset() {
        nrTuples = 0;
        nrIndexLookups = 0;
        nrIndexEntries = 0;
        nrUniqueIndexLookups = 0;
        nrIterations = 0;
        nrUctNodes = 0;
        nrPlansTried = 0;
        avgReward = -1;
        maxReward = -1;
        nrSamples = 0;
        totalWork = 0;
    }
}
