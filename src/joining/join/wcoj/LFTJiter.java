package joining.join.wcoj;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import buffer.BufferManager;
import catalog.CatalogManager;
import data.ColumnData;
import data.IntData;
import preprocessing.Context;
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
	int curTrieLevel;
	/**
	 * Maximally admissible tuple index
	 * at current level (value in prior
	 * trie levels changes for later
	 * tuples).
	 */
	final int[] curUBs;
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
	 * @param context			execution context
	 * @param aliasID			initialize for this join table
	 * @param globalVarOrder	global order of variables
	 */
	public LFTJiter(QueryInfo query, Context context, int aliasID, 
			List<Set<ColumnRef>> globalVarOrder) throws Exception {
		// Get information on target table
		String alias = query.aliases[aliasID];
		String table = context.aliasToFiltered.get(alias);
		card = CatalogManager.getCardinality(table);
		// Extract columns used for sorting
		trieCols = new ArrayList<>();
		for (Set<ColumnRef> eqClass : globalVarOrder) {
			for (ColumnRef colRef : eqClass) {
				if (colRef.aliasName.equals(alias)) {
					String colName = colRef.columnName;
					ColumnRef bufferRef = new ColumnRef(table, colName);
					ColumnData colData = BufferManager.getData(bufferRef);
					trieCols.add((IntData)colData);
				}
			}			
		}
		// Initialize position array
		nrLevels = trieCols.size();
		curTuples = new int[nrLevels];
		curUBs = new int[nrLevels];
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
					if (cmp == 2) {
						boolean row1null = colData.isNull.get(row1);
						boolean row2null = colData.isNull.get(row2);
						if (row1null && !row2null) {
							return -1;
						} else if (!row1null && row2null) {
							return 1;
						}
					} else if (cmp != 0) {
						return cmp;
					}
				}
				return 0;
			}
		});
		reset();
		// Perform run time checks if activated
		IterChecker.checkIter(query, context, 
				aliasID, globalVarOrder, this);
	}
	/**
	 * Resets all internal variables to state
	 * before first invocation.
	 */
	void reset() {
		Arrays.fill(curTuples, 0);
		Arrays.fill(curUBs, card-1);
		curTrieLevel = -1;
	}
	/**
	 * Return key in current level at given tuple.
	 * 
	 * @param tuple		return key of this tuple (in sort order)
	 * @return			key of specified tuple
	 */
	int keyAt(int tuple) {
		int row = tupleOrder[tuple];
		IntData curCol = trieCols.get(curTrieLevel);
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
	 * Returns (actual) index of currently
	 * considered tuple in its base table.
	 * 
	 * @return	record ID of current tuple
	 */
	public int rid() {
		return tupleOrder[curTuples[curTrieLevel]];
	}
	/**
	 * Proceeds to next key in current trie level.
	 */
	public void next() throws Exception {
		seek(key()+1);
	}
	/**
	 * Seek first tuple whose key in current column
	 * is not below seek key, consider range up to
	 * and including tuple index ub. Returns -1 if
	 * no such tuple can be found.
	 * 
	 * @param seekKey	find tuple with key at least that
	 * @param ub		search for tuples up to this index
	 * @return			next tuple index or -1 if none found
	 */
	public int seekInRange(int seekKey, int ub) throws Exception {
		// Current tuple position is lower bound
		int lb = curTuples[curTrieLevel];
		// Until search bounds collapse
		while (lb < ub) {
			int middle = (lb + ub)/2;
			if (keyAt(middle)>=seekKey) {
				ub = middle;
			} else {
				lb = middle+1;
			}
		}
		// Debugging check
		if (lb != ub) {
			System.out.println("Error - lb " + 
					lb + " and ub " + ub);
		}
		// Check that prior keys did not change
		if (keyAt(lb)>=seekKey) {
			for (int level=0; level<curTrieLevel; ++level) {
				int curTuple = curTuples[curTrieLevel];
				IntData intData = trieCols.get(level);
				int cmp = intData.compareRows(
						tupleOrder[curTuple], tupleOrder[lb]);
				if (cmp != 0) {
					throw new Exception(
							"Inconsistent keys at level " + level +
							" for seek at level " + curTrieLevel +
							"; upper bounds: " + 
							Arrays.toString(curUBs) + 
							"; current tuples: " +
							Arrays.toString(curTuples) +
							"; lb: " + lb + 
							"; key1: " + 
							intData.data[tupleOrder[curTuple]] +
							"; key2: " + 
							intData.data[tupleOrder[lb]]);
				}
			}
		}
		// Return next tuple position or -1
		return keyAt(lb)>=seekKey?lb:-1;
	}
	/*
	public int seekInRange(int seekKey, int ub) {
		// Prepare for "galloping"
		int step = 1;
		int UBtuple = curTuples[curTrieLevel] + step;
		// Don't exceed relation boundaries
		UBtuple = Math.min(UBtuple, ub);
		int UBkey = keyAt(UBtuple);
		// Until key changed or end reached
		while (UBkey < seekKey && UBtuple<ub) {
			UBkey = keyAt(UBtuple);
			step *= 2;
			UBtuple += step;
		}
		UBtuple = Math.min(UBtuple, ub);
		// Set to end position if not found
		if (keyAt(UBtuple) < seekKey) {
			return -1;
		}
		// Otherwise apply binary search
		int LBtuple = Math.max(UBtuple-step, 0);
		// Search next tuple in tuple range
		int searchLB = LBtuple;
		int searchUB = UBtuple;
		while (searchLB < searchUB) {
			int middle = (searchLB + searchUB)/2;
			if (keyAt(middle)>=seekKey) {
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
		// Return index of next tuple
		return searchLB;
	}
	*/
	/**
	 * Place iterator at first element whose
	 * key is at or above the seek key.
	 * 
	 * @param seekKey	lower bound for next key
	 */
	public void seek(int seekKey) throws Exception {
		// Search next tuple in current range
		int next = seekInRange(seekKey, curUBs[curTrieLevel]);
		// Did we find a tuple?
		if (next<0) {
			curTuples[curTrieLevel] = card;
		} else {
			curTuples[curTrieLevel] = next;
		}
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
	public void open() throws Exception {
		int curTuple = curTrieLevel<0 ? 0:curTuples[curTrieLevel];
		int nextUB = card-1;
		if (curTrieLevel>=0) {
			for (int i=0; i<=curTrieLevel; ++i) {
				nextUB = Math.min(curUBs[i], nextUB);
			}
			int curKey = key();
			int nextPos = seekInRange(curKey+1, nextUB);
			if (nextPos>=0) {
				nextUB = Math.min(nextPos-1, nextUB);
			}
		}
		++curTrieLevel;
		curUBs[curTrieLevel] = nextUB;
		curTuples[curTrieLevel] = curTuple;
		/*
		System.out.println("--- Opening iterator");
		System.out.println(Arrays.toString(curUBs));
		System.out.println(Arrays.toString(curTuples));
		*/
	}
	/**
	 * Return to last trie level without
	 * changing iterator position.
	 */
	public void up() {
		--curTrieLevel;
	}
}