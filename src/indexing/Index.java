package indexing;

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
	 * Initialize for given cardinality of indexed table.
	 * 
	 * @param cardinality	number of rows to index
	 */
	public Index(int cardinality) {
		this.cardinality = cardinality;
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
