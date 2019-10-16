package indexing;

import com.koloboke.collect.map.IntIntCursor;
import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMaps;
import config.CheckConfig;
import data.IntData;
import org.omg.Messaging.SYNC_WITH_TRANSPORT;

import java.util.Arrays;

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
	public static boolean checkIndex(IntData data, Index index) {
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

	public static boolean checkIndex(IntData intData, ThreadIntIndex index, int nrThreads) {
		// Is index checking enabled?
		if (CheckConfig.CHECK_INDICES) {
			System.out.println("Verifying index on table with "
					+ "cardinality " + intData.cardinality + " ...");
			// Iterate over indexed data
			IntIntMap keyToNr = HashIntIntMaps.newMutableMap();
			int cardinality = intData.cardinality;
			int[] data = intData.data;
			IntIntMap keyToPositions = HashIntIntMaps.newMutableMap();
			for (int i = 0; i < cardinality; ++i) {
				// Don't index null values
				if (!intData.isNull.get(i)) {
					int value = data[i];
					int nr = keyToNr.getOrDefault(value, 0);
					keyToNr.put(value, nr+1);
				}
			}

			// Assign each key to the appropriate position offset
			int nrKeys = keyToNr.size();
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
			// Generate position information
			int[] positions = new int[prefixSum];
			int[] scopes = new int[cardinality];
			for (int i=0; i<cardinality; ++i) {
				if (!intData.isNull.get(i)) {
					int key = data[i];
					int startPos = keyToPositions.get(key);
					positions[startPos] += 1;
					int offset = positions[startPos];
					int pos = startPos + offset;
					positions[pos] = i;
					scopes[i] = (offset - 1) % nrThreads;
				}
			}
			for (int key: keyToPositions.keySet()) {
				int prefix = keyToPositions.getOrDefault(key, 0);
				int newPrefix = index.keyToPositions.getOrDefault(key, 0);
				int nr = positions[prefix];
				boolean valueEqual = nr == index.positions[newPrefix];
				if (!valueEqual) {
					System.out.println("wrong");
				}
				for (int i = 0; i < nr; i++) {
					if (positions[prefix + 1 + i] != index.positions[newPrefix + 1 + i]) {
						System.out.println("wrong");
					}
				}
			}
			for (int i = 0; i < cardinality; i++) {
				if (scopes[i] != index.scopes[i]) {
					System.out.println("wrong");
				}
			}

			boolean equal = Arrays.equals(positions, index.positions);

			if (!equal) {
				for (int i = 0; i < positions.length; i++)
					if (positions[i] != index.positions[i])
						return false;
				System.out.println("Index failed some checks.");
			}
			System.out.println("Index passes all checks.");
			return equal;
		}
		return true;
	}
}
