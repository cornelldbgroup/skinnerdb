package joining.parallel.indexing;

import config.LoggingConfig;
import indexing.Index;

/**
 * Common superclass of all partition indexing structures.
 *
 * @author immanueltrummer
 *
 */
public abstract class PartitionIndex extends Index {
    /**
     * Initialize for given cardinality of indexed table.
     *
     * @param cardinality number of rows to index
     */
    public PartitionIndex(int cardinality) {
        super(cardinality);
    }

    /**
     * Output given log text if activated.
     *
     * @param logText	text to log if activated
     */
    void log(String logText) {
        if (LoggingConfig.INDEXING_VERBOSE) {
            System.out.println(logText);
        }
    }
}
