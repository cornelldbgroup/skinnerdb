package indexing;

import com.koloboke.collect.IntCollection;
import config.LoggingConfig;

/**
 * Common superclass of all indexing structures.
 * 
 * @author immanueltrummer
 *
 */
public abstract class Index {
	/**
	 * Cardinality of indexed table.
	 */
	public final int cardinality;
	/**
	 * After indexing: contains for each search key
	 * the number of entries, followed by the row
	 * numbers at which those entries are found.
	 */
	public int[] positions;
	/**
	 * After indexing: contains row id where the element
	 * is sorted in an increasing way.
	 */
	public int[] sortedRow;
	/**
	 * After indexing: contains group id that the element
	 * belongs to.
	 */
	public int[] groupIds;
	/**
	 * Initialize for given cardinality of indexed table.
	 * 
	 * @param cardinality	number of rows to index
	 */
	public Index(int cardinality) {
		this.cardinality = cardinality;
	}

	/**
	 * Return a Set of first position for each distinct key.
	 *
	 * @return		Set of first position for each distinct key.
	 */
	public abstract IntCollection posSet();
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
