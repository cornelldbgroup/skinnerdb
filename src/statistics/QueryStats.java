package statistics;

/**
 * The statistics of running query
 * including the name of query
 * predicates, etc.
 *
 * @author Anonymous
 */
public class QueryStats {
    /**
     * The Name of running query.
     */
    public static String queryName = "test";
    /**
     * Optimal join order.
     */
    public static int[] optimal = null;
    /**
     * End-to-end time for the query.
     */
    public static long time;
}
