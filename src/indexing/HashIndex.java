package indexing;

import config.LoggingConfig;

/**
 * Common superclass of hash indexing structures.
 *
 * @author immanueltrummer
 */
public abstract class HashIndex<T> {
    /**
     * Cardinality of indexed table.
     */
    public final int cardinality;
    /**
     * After indexing: contains for each search key
     * the number of entries, followed by the row
     * numbers at which those entries are found.
     */
    public int[] data;

    /**
     * Initialize for given cardinality of indexed table.
     *
     * @param cardinality number of rows to index
     */
    public HashIndex(int cardinality) {
        this.cardinality = cardinality;
    }

    /**
     * Output given log text if activated.
     *
     * @param logText text to log if activated
     */
    void log(String logText) {
        if (LoggingConfig.INDEXING_VERBOSE) {
            System.out.println(logText);
        }
    }

    public int nextHighestRowInBucket(int dataLocation, int target) {
        int end = this.data[dataLocation] - 1;
        int start = dataLocation + 1;

        int nextHighest = -1;
        while (start <= end) {
            int mid = (start + end) / 2;

            if (this.data[mid] <= target) {
                start = mid + 1;
            } else {
                nextHighest = mid;
                end = mid - 1;
            }
        }

        return nextHighest;
    }

    public abstract int getDataLocation(T data);
}
