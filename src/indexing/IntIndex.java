package indexing;

import joining.join.IndexAccessInfo;

/**
 * Common super class for all indices indexing
 * keys of type integer.
 * 
 * @author immanueltrummer
 *
 */
public abstract class IntIndex extends Index {
	/**
	 * Initializes index for given number of rows.
	 * 
	 * @param cardinality	number of rows to index
	 */
	public IntIndex(int cardinality) {
		super(cardinality);
	}
	/**
	 * Returns index of next tuple with given value
	 * or cardinality of indexed table if no such
	 * tuple exists.
	 * 
	 * @param value			indexed value
	 * @param prevTuple		index of last tuple
	 * @return 	index of next tuple or cardinality
	 */
	public abstract int nextTuple(int value, int prevTuple);
	/**
	 * Returns index of next tuple with given value
	 * or cardinality of indexed table if no such
	 * tuple exists. In order to apply cache for multi-threads,
	 * cached statistics for the indexes are moved to the
	 * according join operator.
	 *
	 * @param value			indexed value
	 * @param prevTuple		index of last tuple
	 * @param accessInfo	index access information
	 * @return 	index of next tuple or cardinality
	 */
	public abstract int nextTuple(int value, int prevTuple, IndexAccessInfo accessInfo);
	/**
	 * Returns index of next tuple with given value
	 * or cardinality of indexed table if no such
	 * tuple exists in the thread's partition.
	 *
	 * @param value			indexed value
	 * @param prevTuple		index of last tuple
	 * @param priorIndex	index of last tuple in the prior table
	 * @param tid			thread id
	 * @param accessInfo	index access information
	 * @return 	index of next tuple or cardinality
	 */
	public abstract int nextTuple(int value, int prevTuple, int priorIndex, int tid,
								  IndexAccessInfo accessInfo);
	/**
	 * Returns the number of entries indexed
	 * for the given value.
	 * 
	 * @param value	count indexed tuples for this value
	 * @return		number of indexed values
	 */
	public abstract int nrIndexed(int value);
}
