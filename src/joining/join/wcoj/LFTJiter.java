package joining.join.wcoj;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import buffer.BufferManager;
import catalog.CatalogManager;
import data.ColumnData;
import query.ColumnRef;
import query.QueryInfo;

/**
 * Implements the iterator used by the LFTJ.
 * 
 * @author immanueltrummer
 *
 */
public class LFTJiter {
	/**
	 * Contains data of columns that form
	 * the trie levels.
	 */
	final List<ColumnData> trieCols;
	/**
	 * Contains row IDs of rows ordered by
	 * the variables in the order in which
	 * they appear in the global variable
	 * order.
	 */
	final Integer[] tupleOrder;
	/**
	 * Initializes iterator for given query and
	 * relation, and given (global) variable order.
	 * 
	 * @param query				initialize for this query
	 * @param aliasID			initialize for this join table
	 * @param globalVarOrder	global order of variables
	 */
	public LFTJiter(QueryInfo query, int aliasID, 
			List<ColumnRef> globalVarOrder) throws Exception {
		// Get information on target table
		String alias = query.aliases[aliasID];
		String table = query.aliasToTable.get(alias);
		int card = CatalogManager.getCardinality(table);
		// Extract columns used for sorting
		trieCols = new ArrayList<>();
		for (ColumnRef colRef : globalVarOrder) {
			if (colRef.aliasName.equals(alias)) {
				String colName = colRef.columnName;
				ColumnRef bufferRef = new ColumnRef(table, colName);
				ColumnData colData = BufferManager.getData(bufferRef);
				trieCols.add(colData);
			}
		}
		// Initialize tuple order
		tupleOrder = new Integer[card];
		for (int i=0; i<card; ++i) {
			tupleOrder[i] = i;
		}
		// Sort tuples by global variable order
		Arrays.parallelSort(tupleOrder, new Comparator<Integer>() {
			public int compare(Integer row1, Integer row2) {
				for (ColumnData colData : trieCols) {
					int cmp = colData.compareRows(row1, row2);
					if (cmp != 0) {
						return cmp;
					}
				}
				return 0;
			}
		});
	}
}
