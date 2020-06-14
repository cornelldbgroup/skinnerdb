package indexing;

import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMaps;

import data.IntData;

/**
 * Indexes integer values in columns with unique values.
 * 
 * @author immanueltrummer
 *
 */
public class UniqueIntIndex extends IntIndex {
	/**
	 * Maps index key to corresponding row number.
	 */
	public IntIntMap keyToRow;
	/**
	 * Initializes index for given column.
	 * 
	 * @param intData	integer column containing unique values
	 */
	public UniqueIntIndex(IntData intData) {
		super(intData.cardinality);
		int[] data = intData.data;
		keyToRow = HashIntIntMaps.newMutableMap();
		for (int row=0; row<cardinality; ++row) {
			// Don't index null values
			if (!intData.isNull.get(row)) {
				int key = data[row];
				keyToRow.put(key, row);
			}
		}
	}
	@Override
	public int nextTuple(int value, int prevTuple) {
		int onlyRow = keyToRow.getOrDefault(value, cardinality);
		return onlyRow>prevTuple?onlyRow:cardinality;
	}

	@Override
	public int nextTuple(int value, int prevTuple, int priorIndex, int tid) {
		return 0;
	}

	@Override
	public int nrIndexed(int value) {
		return keyToRow.containsKey(value)?1:0;
	}
}
