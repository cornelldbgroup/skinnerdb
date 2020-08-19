package joining.join.wcoj;

import java.util.List;
import java.util.Set;

import config.CheckConfig;
import preprocessing.Context;
import query.ColumnRef;
import query.QueryInfo;

/**
 * Methods for checking LFTJ iterators.
 * 
 * @author immanueltrummer
 *
 */
public class IterChecker {
	/**
	 * Performs several run time checks on iterator,
	 * returns true iff the iterator passes all tests.
	 * 
	 * @param query				the query to optimize
	 * @param context			query execution context
	 * @param aliasID			iterator created for this alias
	 * @param globalVarOrder	global attribute order
	 * @param iter				the iterator to test
	 * @return					true iff iterator passes all tests
	 */
	public static boolean checkIter(QueryInfo query, Context context,
			int aliasID, List<Set<ColumnRef>> globalVarOrder, 
			LFTJiter iter) throws Exception {
		// Are checks activated?
		if (CheckConfig.CHECK_LFTJ_ITERS) {
			// Get table referenced by iterator
			String alias = query.aliases[aliasID];
			String table = context.aliasToFiltered.get(alias);
			// Count equi-join columns in indexed table
			int nrIterCols = 0;
			for (ColumnRef colRef : query.equiJoinCols) {
				if (colRef.aliasName.equalsIgnoreCase(table)) {
					++nrIterCols;
				}
			}
			// Check consistency of column count
			if (nrIterCols != iter.nrLevels ||
					nrIterCols != iter.curTuples.length ||
					nrIterCols != iter.curUBs.length) {
				System.out.println("Inconsistent column counts!");
			}
			// Iterate over indexed columns
			for (int colCtr=0; colCtr<nrIterCols; ++colCtr) {
				System.out.println("Alias " + alias + 
						" - column " + colCtr);
				// Iterate over keys in first column
				iter.open();
				int lastKey = Integer.MIN_VALUE;
				for (int i=0; i<3 && !iter.atEnd(); ++i) {
					iter.next();
					// Check whether we reached end
					if (iter.atEnd()) {
						break;
					}
					if(iter.key()<lastKey) {
						System.out.println("Decreasing "
								+ "keys in column "
								+ colCtr + "!");
						return false;
					}
					lastKey = iter.key();
					System.out.println(lastKey);
				}
			}
			System.out.println("Iterator passes all checks!");
			// Reset to initial state
			iter.reset();
		}
		// Return true by default
		return true;
	}
}
