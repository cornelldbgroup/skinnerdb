package indexing;

import com.koloboke.collect.map.IntIntCursor;
import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMaps;
import config.LoggingConfig;
import data.IntData;
import statistics.JoinStats;

import java.util.HashSet;
import java.util.Set;

/**
 * Indexes integer values (not necessarily unique).
 * The index is organized by threads.
 *
 * @author Ziyun Wei
 *
 */
public class ThreadIntIndex extends Index {
    /**
     * Integer data that the index refers to.
     */
    public final IntData intData;
    /**
     * Cardinality of indexed table.
     */
    public final int cardinality;
    /**
     * After indexing: contains for each search key
     * the number of entries, followed by the row
     * numbers at which those entries are found.
     */
    public int[] positions;
    /**
     * After indexing: maps search key to index
     * of first position at which associated
     * information is stored.
     */
    public IntIntMap keyToPositions;
    /**
     * Number of threads.
     */
    int nrThreads;
    /**
     * Data is distributed to different scopes of threads.
     */
    Set<Integer>[] scopes;
    /**
     * Position of current iterator.
     */
    int iterPos = -1;
    /**
     * Last valid position for current iterator.
     */
    int lastIterPos = -1;
    /**
     * Create index on the given integer column.
     *
     * @param intData	integer data to index
     */
    public ThreadIntIndex(IntData intData, int nrThreads) {
        long startMillis = System.currentTimeMillis();
        // Extract info
        this.nrThreads = nrThreads;
        this.intData = intData;
        this.cardinality = intData.cardinality;
        this.scopes = new HashSet[nrThreads];
        for (int i = 0; i < scopes.length; i++) {
            this.scopes[i] = new HashSet<>();
        }
        int[] data = intData.data;
        // Count number of occurrences for each value
        IntIntMap keyToNr = HashIntIntMaps.newMutableMap();
        IntIntMap keyToIndex = HashIntIntMaps.newMutableMap();
        for (int i=0; i<cardinality; ++i) {
            // Don't index null values
            if (!intData.isNull.get(i)) {
                int value = data[i];
                int nr = keyToNr.getOrDefault(value, 0);
                keyToNr.put(value, nr+1);
                keyToIndex.putIfAbsent(value, 0);
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
                int startThread = keyToIndex.get(key);
                scopes[startThread].add(i);
                keyToIndex.put(key, (startThread + 1) % nrThreads);
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

    @Override
    public int nextTuple(int value, int prevTuple) {
        // Get start position for indexed values
        int firstPos = keyToPositions.getOrDefault(value, -1);
        // No indexed values?
        if (firstPos < 0) {
//            JoinStats.nrUniqueIndexLookups += 1;
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
//        JoinStats.nrIndexEntries += nrVals;
//        if (nrVals==1) {
//            JoinStats.nrUniqueIndexLookups += 1;
//        }
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
     * Returns index of next tuple with given value
     * or cardinality of indexed table if no such
     * tuple exists.
     *
     * @param value			indexed value
     * @param prevTuple		index of last tuple
     * @param tid			thread id
     * @return 	index of next tuple or cardinality
     */
    public int nextTupleInScope(int value, int prevTuple, int tid) {
        // Get start position for indexed values
        int firstPos = keyToPositions.getOrDefault(value, -1);
        // No indexed values?
        if (firstPos < 0) {
            JoinStats.nrUniqueIndexLookups += 1;
            return cardinality;
        }
        // Can we return first indexed value?
        int nrVals = positions[firstPos];
        int firstOffset = tid + 1;
        if (firstOffset > nrVals) {
            return cardinality;
        }
        int firstTuple = positions[firstPos + firstOffset];
        if (firstTuple > prevTuple) {
            return firstTuple;
        }
        // Get number of indexed values
        int lastOffset = (nrVals - 1) / nrThreads * nrThreads + tid + 1;
        // if the offset is beyond the array?
        if (lastOffset > nrVals) {
            lastOffset -= nrThreads;
        }
        int threadVals = (lastOffset - firstOffset) / nrThreads + 1;
        // Update index-related statistics
//        JoinStats.nrIndexEntries += nrVals;
//        if (nrVals==1) {
//            JoinStats.nrUniqueIndexLookups += 1;
//        }
        // Restrict search range via binary search
        int lowerBound = 0;
        int upperBound = threadVals - 1;
        while (upperBound - lowerBound > 1) {
            int middle = lowerBound + (upperBound - lowerBound) / 2;
            int middleOffset = firstPos + middle * nrThreads + tid + 1;
            if (positions[middleOffset] > prevTuple) {
                upperBound = middle;
            } else {
                lowerBound = middle;
            }
        }
        // Get next tuple
        for (int pos = lowerBound; pos <= upperBound; ++pos) {
            int offset = firstPos + pos * nrThreads + tid + 1;
            int nextTuple = positions[offset];
            if (nextTuple > prevTuple) {
                return nextTuple;
            }
        }
        // No suitable tuple found
        return cardinality;
    }

    @Override
    public boolean evaluate(int priorVal, int curIndex, int splitTable, int nextTable, int tid) {
        boolean equal = priorVal == intData.data[curIndex];
        if (splitTable == nextTable) {
            return equal && scopes[tid].contains(curIndex);
        }
        return equal;
    }

    @Override
    public int nrIndexed(int value) {
        return 0;
    }

    @Override
    public void initIter(int value) {

    }

    @Override
    public int iterNextHigher(int prevTuple) {
        return 0;
    }

    @Override
    public int iterNext() {
        return 0;
    }

    /**
     * Output given log text if activated.
     *
     * @param logText	text to log if activated
     */
    void log(String logText) {
        if (LoggingConfig.INDEXING_VERBOSE) {
            System.out.println(logText);
        }
    }
}
