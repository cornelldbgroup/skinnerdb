package preprocessing;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import operators.Group;
import operators.parallel.GroupIndex;
import query.ColumnRef;

/**
 * Summarizes execution context for query processing (e.g., 
 * mapping from query fragments to previously generated
 * intermediate results).
 * 
 * @author immanueltrummer
 *
 */
public class Context {
	/**
	 * Maps query columns to buffered columns (e.g., pre-processing
	 * may change the original mapping since raw columns are
	 * replaced by filtered versions).
	 */
	public final Map<ColumnRef, ColumnRef> columnMapping =
			new ConcurrentHashMap<ColumnRef, ColumnRef>();
	/**
	 * Maps each query alias to the name of the associated
	 * filtered table after pre-processing (which may be
	 * the same table the alias references originally).
	 */
	public final Map<String, String> aliasToFiltered =
			new ConcurrentHashMap<String, String>();
	/**
	 * References column that contains group IDs for
	 * each row in the join result (null if query
	 * contains no group-by clause).
	 */
	public ColumnRef groupRef = null;
	/**
	 * Contains the number of groups (-1 if query
	 * does not have a group by clause).
	 */
	public int nrGroups = -1;
	/**
	 * Maps each group
	 * to an index containing group id and satisfied rows.
	 */
	public Map<Group, GroupIndex> groupsToIndex;
	public Map<Group, List<Group>> groupsToList;
	/**
	 * Maps aggregation expressions (in string representation)
	 * to columns containing corresponding (per-group) results.
	 */
	public Map<String, ColumnRef> aggToData = new HashMap<>();
	
	@Override
	public String toString() {
		return "Column mapping:\t" + columnMapping + System.lineSeparator() +
				"Filtered alias:\t" + aliasToFiltered + System.lineSeparator() +
				"Group reference:\t" + groupRef + System.lineSeparator() +
				"Nr. of groups:\t" + nrGroups + System.lineSeparator() +
				"Aggregate reuslts:\t" + aggToData;
	}
}