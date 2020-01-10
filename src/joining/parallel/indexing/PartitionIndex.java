package joining.parallel.indexing;

import config.LoggingConfig;
import indexing.Index;
import predicate.NonEquiNode;
import predicate.Operator;

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
     * Whether the indices satisfy the given node corresponding to a predicate
     * using the index.
     *
     * @param curTuple      current tuple index
     * @param constant      constant in predicate
     * @param operator      predicate operator
     * @return              evaluation results
     */
    public abstract boolean evaluate(int curTuple, Number constant, Operator operator);
    /**
     * Whether the indices satisfy the given node corresponding to a exist predicate
     * using the index.
     *
     * @param constant      constant in predicate
     * @param operator      predicate operator
     * @return              evaluation results
     */
    public abstract boolean exist(Number constant, Operator operator);

    /**
     * Return the value of row in the column.
     *
     * @param curTuple      the row id.
     * @return              value associated with a given row.
     */
    public abstract Number getNumber(int curTuple);

    /**
     * Sort the elements and initialize the array of sortedRow
     */
    public abstract void sortRows();

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
