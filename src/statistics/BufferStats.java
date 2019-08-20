package statistics;

/**
 * Statistics about the buffer management - most statistics
 * refer to counts achieved during the last query
 * evaluation.
 *
 * @author Ziyun Wei
 *
 */
public class BufferStats {
    /**
     * Number of index lookups during the last invocation.
     */
    public static long nrIndexLookups = 0;
    /**
     * Number of index lookups with a cache hit.
     */
    public static long nrCacheHit = 0;
    /**
     * Number of index lookups with a cache miss.
     */
    public static long nrCacheMiss = 0;
    /**
     * Period of writing indexes to files.
     */
    public static long writeTime = 0;

    public static void initBufferStats() {
        nrIndexLookups = 0;
        nrCacheHit = 0;
        nrCacheMiss = 0;
        writeTime = 0;
    }

}
