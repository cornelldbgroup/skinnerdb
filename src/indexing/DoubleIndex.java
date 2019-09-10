package indexing;

import com.koloboke.collect.map.DoubleIntCursor;
import com.koloboke.collect.map.DoubleIntMap;
import com.koloboke.collect.map.hash.HashDoubleIntMaps;

import config.LoggingConfig;
import data.DoubleData;
import statistics.JoinStats;

/**
 * Indexes double values (not necessarily unique).
 * 
 * @author immanueltrummer
 *
 */
public class DoubleIndex extends Index {
	/**
	 * Double data that the index refers to.
	 */
	public final DoubleData doubleData;
	/**
	 * After indexing: maps search key to index
	 * of first position at which associated
	 * information is stored.
	 */
	public DoubleIntMap keyToPositions;
	/**
	 * Create index on the given double column.
	 * 
	 * @param doubleData	double data to index
	 */
	public DoubleIndex(DoubleData doubleData) {
		super(doubleData.cardinality);
		long startMillis = System.currentTimeMillis();
		// Extract info
		this.doubleData = doubleData;
		double[] data = doubleData.data;
		// Count number of occurrences for each value
		DoubleIntMap keyToNr = HashDoubleIntMaps.newMutableMap();
		for (int i=0; i<cardinality; ++i) {
			// Don't index null values
			if (!doubleData.isNull.get(i)) {
				double value = data[i];
				int nr = keyToNr.getOrDefault(value, 0);
				keyToNr.put(value, nr+1);				
			}
		}
		// Assign each key to the appropriate position offset
		int nrKeys = keyToNr.size();
		log("Number of keys:\t" + nrKeys);
		keyToPositions = HashDoubleIntMaps.newMutableMap(nrKeys);
		int prefixSum = 0;
		DoubleIntCursor keyToNrCursor = keyToNr.cursor();
		while (keyToNrCursor.moveNext()) {
			double key = keyToNrCursor.key();
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
			if (!doubleData.isNull.get(i)) {
				double key = data[i];
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
	public int nextTuple(double value, int prevTuple) {
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
	public int nrIndexed(double value) {
		int firstPos = keyToPositions.getOrDefault(value, -1);
		if (firstPos<0) {
			return 0;
		} else {
			return positions[firstPos];
		}
	}
}
