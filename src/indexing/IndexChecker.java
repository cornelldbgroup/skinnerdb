package indexing;

import config.CheckConfig;
import data.IntData;

/**
 * Methods for verifying generated indices
 * (useful for debugging).
 * 
 * @author immanueltrummer
 *
 */
public class IndexChecker {
	/**
	 * Verifies that index entries are correct and complete,
	 * returns true iff that is the case or if index checking
	 * is disabled.
	 * 
	 * @param data		indexed data
	 * @param index		index
	 * @return			true iff index passes all checks
	 */
	public static boolean checkIndex(IntData data, IntIndex index) {
		// Is index checking enabled?
		if (CheckConfig.CHECK_INDICES) {
			System.out.println("Verifying index on table with "
					+ "cardinality " + data.cardinality + " ...");
			// Iterate over indexed data
			for (int i=0; i<data.cardinality; ++i) {
				if (!data.isNull.get(i)) {
					// Generate output every 10,000 rows
					if (i % 10000 == 0) {
						System.out.println("Checking row " + i + " ...");
					}
					// Ensure that index contains reference
					int value = data.data[i];
					int nextTuple = index.nextTuple(value, i-1);
					if (nextTuple != i) {
						System.out.println("Next indexed tuple should be " + 
								i + ", but is " + nextTuple + " instead. " +
								"Index on table with cardinality " +
								data.cardinality + " fails check.");
						return false;
					}					
				}
			}
			System.out.println("Index passes all checks.");
		}
		return true;
	}
}
