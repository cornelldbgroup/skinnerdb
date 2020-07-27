package joining.join.wcoj;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import buffer.BufferManager;
import catalog.CatalogManager;
import data.ColumnData;
import data.IntData;
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
	 * Cardinality of table that we 
	 * are iterating over.
	 */
	final int card;
	/**
	 * Contains data of columns that form
	 * the trie levels.
	 */
	final List<IntData> trieCols;
	/**
	 * Contains row IDs of rows ordered by
	 * the variables in the order in which
	 * they appear in the global variable
	 * order.
	 */
	final Integer[] tupleOrder;
	/**
	 * Number of trie levels (i.e., number
	 * of attributes the trie indexes).
	 */
	final int nrLevels;
	/**
	 * We are at this level of the trie.
	 */
	int curTrieLevel = -1;
	/**
	 * Contains for each trie level the current position
	 * (expressed as tuple index in tuple sort order).
	 */
	final int[] curTuples;
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
		card = CatalogManager.getCardinality(table);
		// Extract columns used for sorting
		trieCols = new ArrayList<>();
		for (ColumnRef colRef : globalVarOrder) {
			if (colRef.aliasName.equals(alias)) {
				String colName = colRef.columnName;
				ColumnRef bufferRef = new ColumnRef(table, colName);
				ColumnData colData = BufferManager.getData(bufferRef);
				trieCols.add((IntData)colData);
			}
		}
		// Initialize position array
		nrLevels = trieCols.size();
		curTuples = new int[nrLevels];
		Arrays.fill(curTuples, 0);
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
	/**
	 * Return key in current level at given tuple.
	 * 
	 * @param tuple		return key of this tuple (in sort order)
	 * @return			key of specified tuple
	 */
	int keyAt(int tuple) {
		IntData curCol = trieCols.get(curTrieLevel);
		int row = tupleOrder[tuple];
		return curCol.data[row];		
	}
	/**
	 * Returns key at current iterator position
	 * (we currently assume integer keys only).
	 * 
	 * @return	key at current level and position
	 */
	public int key() {
		return keyAt(curTuples[curTrieLevel]);
	}
	/**
	 * Proceeds to next key in current trie level.
	 */
	public void next() {
		seek(key()+1);
	}
	/**
	 * Place iterator at first element whose
	 * key is at or above the seek key.
	 * 
	 * @param seekKey	lower bound for next key
	 */
	public void seek(int seekKey) {
		// Prepare for "galloping"
		int curKey = key();
		int step = 1;
		int UBtuple = curTuples[curTrieLevel] + step;
		int UBkey = keyAt(UBtuple);
		// Until key changed or end reached
		while (UBkey < seekKey && UBtuple<card) {
			UBkey = keyAt(UBtuple);
			step *= 2;
			UBtuple += step;
		}
		UBtuple = Math.min(UBtuple, card-1);
		// Set to end position if not found
		if (keyAt(UBtuple) < seekKey) {
			curTuples[curTrieLevel] = card;
			return;
		}
		// Otherwise apply binary search
		int LBtuple = Math.max(UBtuple-step, 0);
		// Search next tuple in tuple range
		int searchLB = LBtuple;
		int searchUB = UBtuple;
		while (searchLB != searchUB) {
			int middle = (searchLB + searchUB)/2;
			if (keyAt(middle)>curKey) {
				searchUB = middle;
			} else {
				searchLB = middle+1;
			}
		}
		// Debugging check
		if (searchLB != searchUB) {
			System.out.println("Error - searchLB " + 
					searchLB + " and searchUB " + searchUB);
		}
		// Advance to next tuple
		curTuples[curTrieLevel] = searchLB;
	}
	/**
	 * Returns true iff the iterator is at the end.
	 * 
	 * @return	true iff iterator is beyond last tuple
	 */
	public boolean atEnd() {
		return curTuples[curTrieLevel] == card;
	}
	/**
	 * Advance to next trie level and reset
	 * iterator to first associated position.
	 */
	public void open() {
		int curTuple = curTuples[curTrieLevel];
		++curTrieLevel;
		curTuples[curTrieLevel] = curTuple;
	}
	/**
	 * Return to last trie level without
	 * changing iterator position.
	 */
	public void up() {
		--curTrieLevel;
	}
}