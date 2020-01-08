package indexing;

import com.koloboke.collect.IntCollection;
import com.koloboke.collect.map.IntIntCursor;
import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMaps;

import config.LoggingConfig;
import data.IntData;
import statistics.JoinStats;

import java.util.stream.IntStream;

/**
 * Indexes integer values (not necessarily unique).
 * 
 * @author immanueltrummer
 *
 */
public class IntIndex extends Index {
	/**
	 * Integer data that the index refers to.
	 */
	public final IntData intData;
	/**
	 * After indexing: maps search key to index
	 * of first position at which associated
	 * information is stored.
	 */
	public IntIntMap keyToPositions;
	/**
	 * Create index on the given integer column.
	 * 
	 * @param intData	integer data to index
	 */
	public IntIndex(IntData intData) {
		super(intData.cardinality);
		long startMillis = System.currentTimeMillis();
		// Extract info
		this.intData = intData;
		int[] data = intData.data;
		// Count number of occurrences for each value
		IntIntMap keyToNr = HashIntIntMaps.newMutableMap();
		for (int i=0; i<cardinality; ++i) {
			// Don't index null values
			if (!intData.isNull.get(i)) {
				int value = data[i];
				int nr = keyToNr.getOrDefault(value, 0);
				keyToNr.put(value, nr+1);				
			}
		}
		// Assign each key to the appropriate position offset
		int nrKeys = keyToNr.size();
		log("Number of keys:\t" + nrKeys);
		keyToPositions = HashIntIntMaps.newMutableMap(nrKeys);
		int prefixSum = 0;
		IntIntCursor keyToNrCursor = keyToNr.cursor();
		while (keyToNrCursor.moveNext()) {
			int key = keyToNrCursor.key();
			keyToPositions.put(key, prefixSum);
			// Advance offset taking into account
			// space for row indices and one field
			// storing the number of following indices.
			int nrFields = keyToNrCursor.value() + 1;
			prefixSum += nrFields;
		}
		log("Prefix sum:\t" + prefixSum);
		// Generate position information
		positions = new int[prefixSum];
		for (int i=0; i<cardinality; ++i) {
			if (!intData.isNull.get(i)) {
				int key = data[i];
				int startPos = keyToPositions.get(key);
				positions[startPos] += 1;
				int offset = positions[startPos];
				int pos = startPos + offset;
				positions[pos] = i;				
			}
		}
		// Output statistics for performance tuning
		if (LoggingConfig.INDEXING_VERBOSE) {
			long totalMillis = System.currentTimeMillis() - startMillis;
			log("Created index for integer column with cardinality " + 
					cardinality + " in " + totalMillis + " ms.");
		}
		// Check index if enabled
		IndexChecker.checkIndex(intData, this);
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
	public int nextTuple(int value, int prevTuple) {
		// Get start position for indexed values
		int firstPos = keyToPositions.getOrDefault(value, -1);
		// No indexed values?
		if (firstPos < 0) {
			JoinStats.nrUniqueIndexLookups += 1;
			return cardinality;
		}
		// Can we return first indexed value?
		int firstTuple = positions[firstPos+1];
		if (firstTuple>prevTuple) {
			return firstTuple;
		}
		// Get number of indexed values
		int nrVals = positions[firstPos];
		// Update index-related statistics
		JoinStats.nrIndexEntries += nrVals;
		if (nrVals==1) {
			JoinStats.nrUniqueIndexLookups += 1;
		}
		// Restrict search range via binary search
		int lowerBound = firstPos + 1;
		int upperBound = firstPos + nrVals;
		while (upperBound-lowerBound>1) {
			int middle = lowerBound + (upperBound-lowerBound)/2;
			if (positions[middle] > prevTuple) {
				upperBound = middle;
			} else {
				lowerBound = middle;
			}
		}
		// Get next tuple
		for (int pos=lowerBound; pos<=upperBound; ++pos) {
			if (positions[pos] > prevTuple) {
				return positions[pos];
			}
		}
		// No suitable tuple found
		return cardinality;
	}
	/**
	 * Returns the number of entries indexed
	 * for the given value.
	 * 
	 * @param value	count indexed tuples for this value
	 * @return		number of indexed values
	 */
	public int nrIndexed(int value) {
		int firstPos = keyToPositions.getOrDefault(value, -1);
		if (firstPos<0) {
			return 0;
		} else {
			return positions[firstPos];
		}
	}

	@Override
	public IntCollection posSet() {
		return keyToPositions.values();
	}

	@Override
	public void sortRows() {
		sortedRow = IntStream.range(0, cardinality)
				.boxed().sorted((o1, o2) -> {
					int d1 = intData.data[o1];
					int d2 = intData.data[o2];
					int diff = d1 - d2;
					if (diff == 0) {
						return o1 - o2;
					}
					return diff;
				})
				.mapToInt(ele -> ele).toArray();
	}
}
