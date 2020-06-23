package indexing;

import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMaps;

import config.ParallelConfig;
import data.IntData;
import joining.join.DPJoin;

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
	public int nextTuple(int value, int prevTuple, DPJoin dpJoin) {
		int onlyRow = keyToRow.getOrDefault(value, cardinality);
		dpJoin.lastNrVals = 1;
		return onlyRow>prevTuple?onlyRow:cardinality;
	}

	@Override
	public int nextTuple(int value, int prevTuple, int priorIndex, DPJoin dpJoin) {
		int nrThreads = ParallelConfig.JOIN_THREADS;
		int tid = dpJoin.tid;
		tid = (priorIndex + tid) % nrThreads;
		int onlyRow = tid == 0 ? keyToRow.getOrDefault(value, cardinality)
				: cardinality;
		dpJoin.lastNrVals = 1;
		return onlyRow > prevTuple?onlyRow : cardinality;
	}

	@Override
	public int nrIndexed(int value) {
		return keyToRow.containsKey(value)?1:0;
	}
}
