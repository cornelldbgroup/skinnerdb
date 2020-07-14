package indexing;

import config.LoggingConfig;
import config.ParallelConfig;

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
	 * The number of distinct keys.
	 */
	public int nrKeys;
	/**
	 * After indexing: contains for each search key
	 * the number of entries, followed by the row
	 * numbers at which those entries are found.
	 */
	public int[] positions;
	/**
	 * After indexing: contains the thread id that
	 * owns the row. By recording this, evaluating whether
	 * the row belongs to a certain can be sloved in
	 * constant time.
	 */
	public byte[] threadForRows;
	/**
	 * After indexing: contains the group id for
	 * each row. By recording this, grouping rows for
	 * single column can be processed efficiently.
	 */
	public int[] groups;
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
	/**
	 * Check whether the current row is in
	 * the thread's scope.
	 *
	 * @param priorIndex    index in the prior table.
	 * @param curIndex      index of joining table.
	 * @param tid           thread id
	 * @return              binary result of "in scope" evaluation.
	 */
	public boolean inScope(int priorIndex, int curIndex, int tid) {
		int nrThreads = ParallelConfig.JOIN_THREADS;
		tid = (priorIndex + tid) % nrThreads;
		return threadForRows[curIndex] == tid;
	}
	/**
	 * Initialize the group id for each row.
	 */
	public void groupRows() {
		groups = new int[nrKeys];
		int groupID = 0;
		int positionsCtr = 0;
		int nrPos = positions.length;
		while (positionsCtr < nrPos) {
			groups[groupID] = positionsCtr;
			int nrVals = positions[positionsCtr];
			positionsCtr += nrVals + 1;
			groupID++;
		}
	}
}
