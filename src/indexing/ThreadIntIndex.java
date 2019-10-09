package indexing;

import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Multiset;
import com.koloboke.collect.map.IntIntCursor;
import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMaps;
import config.LoggingConfig;
import config.ParallelConfig;
import data.IntData;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Indexes integer values (not necessarily unique).
 * The index is organized by threads.
 *
 * @author Ziyun Wei
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
     * new positions for parallel index creation.
     */
    public int[][] newPositions;
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
    public int[] scopes;
    /**
     * The row index of unique values;
     */
    public Set<Integer> distinctValues;

    /**
     * Create index on the given integer column.
     *
     * @param intData integer data to index
     */
//    public ThreadIntIndex(IntData intData, int nrThreads) {
//        long startMillis = System.currentTimeMillis();
//        // Extract info
//        this.nrThreads = nrThreads;
//        this.intData = intData;
//        this.cardinality = intData.cardinality;
//        this.scopes = new int[this.cardinality];
//        this.posList = new ArrayList<>();
//        this.distinctValues = new HashSet<>();
//        int[] data = intData.data;
//        // Count number of occurrences for each value
//        keyToPositions = HashIntIntMaps.newMutableMap();
//        List<Integer> threadList = new ArrayList<>();
////        long timer0 = System.currentTimeMillis();
//        for (int i=0; i<cardinality; ++i) {
//            // Don't index null values
//            if (!intData.isNull.get(i)) {
//                int value = data[i];
//                int pos = keyToPositions.getOrDefault(value, -1);
//                if (pos < 0) {
//                    distinctValues.add(value);
//                    pos = posList.size();
//                    keyToPositions.put(value, pos);
//                    posList.add(1);
//                    threadList.add(0);
//                }
//                else
//                    posList.set(pos, posList.get(pos) + 1);
//            }
//        }
////        long timer1 = System.currentTimeMillis();
//        // Assign each key to the appropriate position offset
//        int nrKeys = posList.size();
//        log("Number of keys:\t" + nrKeys);
//        int prefixSum = 0;
//        for (int i = 0; i < posList.size(); i++) {
//            int val = posList.get(i);
//            posList.set(i, prefixSum);
//            int nrFields = val + 1;
//            prefixSum += nrFields;
//        }
//
////        long timer2 = System.currentTimeMillis();
//        log("Prefix sum:\t" + prefixSum);
//        // Generate position information
//        positions = new int[prefixSum];
//        for (int i=0; i<cardinality; ++i) {
//            if (!intData.isNull.get(i)) {
//                int key = data[i];
//                int pos = keyToPositions.get(key);
//                int startPos = posList.get(pos);
//                int startThread = threadList.get(pos);
//                scopes[i] = startThread;
//                threadList.set(pos, (startThread + 1) % nrThreads);
//
//                positions[startPos] += 1;
//                int offset = positions[startPos];
//                int newPos = startPos + offset;
//                positions[newPos] = i;
//            }
//        }
////        long timer3 = System.currentTimeMillis();
////        System.out.println("Index: " + (timer1 - timer0) + " " + (timer2 - timer1) + " " + (timer3 - timer2));
//        // Output statistics for performance tuning
//        if (LoggingConfig.INDEXING_VERBOSE) {
//            long totalMillis = System.currentTimeMillis() - startMillis;
//            log("Created index for integer column with cardinality " +
//                    cardinality + " in " + totalMillis + " ms.");
//        }
//        // Check index if enabled
//        IndexChecker.checkIndex(intData, this);
//    }

    public ThreadIntIndex(IntData intData, int nrThreads) {
        long startMillis = System.currentTimeMillis();
        // Extract info
        this.nrThreads = nrThreads;
        this.intData = intData;
        this.cardinality = intData.cardinality;
        this.scopes = new int[this.cardinality];
        this.distinctValues = new HashSet<>();

        int[] data = intData.data;
        // Count number of occurrences for each value
        keyToPositions = HashIntIntMaps.newMutableMap();
//        int MAX = ParallelConfig.PRE_BATCH_SIZE;
        int MAX = Integer.MAX_VALUE;
        if (cardinality <= MAX) {
//            long timer0 = System.currentTimeMillis();
            List<Integer> nrList = new ArrayList<>();
            for (int i = 0; i < cardinality; ++i) {
                // Don't index null values
                if (!intData.isNull.get(i)) {
                    int value = data[i];
                    int pos = keyToPositions.getOrDefault(value, -1);
                    if (pos < 0) {
//                        distinctValues.add(value);
                        pos = nrList.size();
                        keyToPositions.put(value, pos);
                        nrList.add(1);
                    }
                    else
                        nrList.set(pos, nrList.get(pos) + 1);
                }
            }
//        long timer1 = System.currentTimeMillis();
            // Assign each key to the appropriate position offset
            int nrKeys = nrList.size();
            newPositions = new int[nrKeys][];
            log("Number of keys:\t" + nrKeys);
            IntIntCursor keyToIndexCursor = keyToPositions.cursor();
            while (keyToIndexCursor.moveNext()) {
                int pos = keyToIndexCursor.value();
                int count = nrList.get(pos);
                newPositions[pos] = new int[count];
            }

            for (int i = 0; i < cardinality; ++i) {
                if (!intData.isNull.get(i)) {
                    int key = data[i];
                    int pos = keyToPositions.get(key);
                    int[] subPos = newPositions[pos];
                    int last = subPos.length - 1;
                    int offset = subPos[last];
                    subPos[offset] = i;
                    if (offset < last) {
                        subPos[last]++;
                    }
                    int startThread = offset % nrThreads;
                    scopes[i] = startThread;
                }
            }
//            long timer1 = System.currentTimeMillis();
//            System.out.println("Sequential: " + (timer1 - timer0) + " ms");
        } else {
            // Divide tuples into batches
//            long timer0 = System.currentTimeMillis();
            List<IndexRange> batches = this.split();

            IntIntMap nrValues = HashIntIntMaps.newMutableMap();
            batches.parallelStream().forEach(batch -> {
                // Evaluate predicate for each table row
                for (int rowCtr = batch.firstTuple; rowCtr <= batch.lastTuple; ++rowCtr) {
                    if (!intData.isNull.get(rowCtr)) {
                        int value = data[rowCtr];
                        batch.add(value);
                    }
                }
            });

//            long timer1 = System.currentTimeMillis();
            int countIndex = 0;
            for (IndexRange batch : batches) {
                IntIntCursor keyToIndexCursor = batch.valuesMap.cursor();
                while (keyToIndexCursor.moveNext()) {
                    int value = keyToIndexCursor.key();
                    int nrs = keyToIndexCursor.value();
                    if (nrValues.containsKey(value)) {
                        nrValues.compute(value, (k, v) -> v + nrs);
                    } else {
                        nrValues.put(value, nrs);
                        keyToPositions.put(value, countIndex);
                        countIndex++;
                    }
                }
            }

            int elementSize = keyToPositions.size();
            newPositions = new int[elementSize][];
//            long timer10 = System.currentTimeMillis();
            keyToPositions.entrySet().parallelStream().forEach(entry -> {
                int value = entry.getKey();
                int index = entry.getValue();
                int count = nrValues.getOrDefault(value, 0);
                newPositions[index] = new int[count];
//                keyToPositions.put(finalValue, index);
                int prefixSum = 0;
                for (IndexRange batch : batches) {
                    AtomicInteger integer = new AtomicInteger(0);
                    int finalPrefixSum = prefixSum;
                    batch.valuesMap.computeIfPresent(value, (k, v) -> {
                        integer.set(v);
                        return finalPrefixSum;
                    });
                    int priorValue = integer.intValue();
                    prefixSum += priorValue;
                }
            });
//            long timer2 = System.currentTimeMillis();
            batches.parallelStream().forEach(batch -> {
                // Evaluate predicate for each table row
                IntIntMap offsets = HashIntIntMaps.newMutableMap();
                for (int rowCtr = batch.firstTuple; rowCtr <= batch.lastTuple; ++rowCtr) {
                    if (!intData.isNull.get(rowCtr)) {
                        int value = data[rowCtr];
                        int prefixSum = batch.valuesMap.getOrDefault(value, 0);
                        int offset = offsets.getOrDefault(value, 0);
                        int pos = prefixSum + offset;
                        int index = keyToPositions.getOrDefault(value, -1);
                        newPositions[index][pos] = rowCtr;
                        int startThread = pos % nrThreads;
                        scopes[rowCtr] = startThread;
                        offsets.put(value, offset + 1);
                    }
                }
            });
//            long timer3 = System.currentTimeMillis();
//            System.out.println("Parallel: " + (timer1 - timer0) + "\t" + (timer10 - timer1) + "\t" + (timer2 - timer10) + "\t" + (timer3 - timer2));
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

//    @Override
//    public int nextTuple(int value, int prevTuple) {
//        // Get start position for indexed values
//        int firstPos = keyToPositions.getOrDefault(value, -1);
//        // No indexed values?
//        if (firstPos < 0) {
//            return cardinality;
//        }
//        firstPos = posList.get(firstPos);
//        // Can we return first indexed value?
//        int firstTuple = positions[firstPos+1];
//        if (firstTuple>prevTuple) {
//            return firstTuple;
//        }
//        // Get number of indexed values
//        int nrVals = positions[firstPos];
//        // Restrict search range via binary search
//        int lowerBound = firstPos + 1;
//        int upperBound = firstPos + nrVals;
//        while (upperBound-lowerBound>1) {
//            int middle = lowerBound + (upperBound-lowerBound)/2;
//            if (positions[middle] > prevTuple) {
//                upperBound = middle;
//            } else {
//                lowerBound = middle;
//            }
//        }
//        // Get next tuple
//        for (int pos=lowerBound; pos<=upperBound; ++pos) {
//            if (positions[pos] > prevTuple) {
//                return positions[pos];
//            }
//        }
//        // No suitable tuple found
//        return cardinality;
//    }
//
//    /**
//     * Returns index of next tuple with given value
//     * or cardinality of indexed table if no such
//     * tuple exists.
//     *
//     * @param value			indexed value
//     * @param prevTuple		index of last tuple
//     * @param tid			thread id
//     * @return 	index of next tuple or cardinality
//     */
//    public int nextTupleInScope(int value, int prevTuple, int tid) {
//        tid = (value + tid) % nrThreads;
//        // Get start position for indexed values
//        int firstPos = keyToPositions.getOrDefault(value, -1);
//        // No indexed values?
//        if (firstPos < 0) {
//            return cardinality;
//        }
//        firstPos = posList.get(firstPos);
//        // Can we return first indexed value?
//        int nrVals = positions[firstPos];
//        int firstOffset = tid + 1;
//        if (firstOffset > nrVals) {
//            return cardinality;
//        }
//        int firstTuple = positions[firstPos + firstOffset];
//        if (firstTuple > prevTuple) {
//            return firstTuple;
//        }
//        // Get number of indexed values
//        int lastOffset = (nrVals - 1) / nrThreads * nrThreads + tid + 1;
//        // if the offset is beyond the array?
//        if (lastOffset > nrVals) {
//            lastOffset -= nrThreads;
//        }
//        int threadVals = (lastOffset - firstOffset) / nrThreads + 1;
//        // Update index-related statistics
//        // Restrict search range via binary search
//        int lowerBound = 0;
//        int upperBound = threadVals - 1;
//        while (upperBound - lowerBound > 1) {
//            int middle = lowerBound + (upperBound - lowerBound) / 2;
//            int middleOffset = firstPos + middle * nrThreads + tid + 1;
//            if (positions[middleOffset] > prevTuple) {
//                upperBound = middle;
//            } else {
//                lowerBound = middle;
//            }
//        }
//        // Get next tuple
//        for (int pos = lowerBound; pos <= upperBound; ++pos) {
//            int offset = firstPos + pos * nrThreads + tid + 1;
//            int nextTuple = positions[offset];
//            if (nextTuple > prevTuple) {
//                return nextTuple;
//            }
//        }
//        // No suitable tuple found
//        return cardinality;
//    }

    @Override
    public int nextTuple(int value, int prevTuple) {
        // Get start position for indexed values
        int index = keyToPositions.getOrDefault(value, -1);
        // No indexed values?
        if (index < 0) {
            return cardinality;
        }
        int[] rowList = newPositions[index];
        // Can we return first indexed value?
        int firstTuple = rowList[0];
        if (firstTuple > prevTuple) {
            return firstTuple;
        }
        // Get number of indexed values
        int nrVals = rowList.length;
        // Restrict search range via binary search
        int lowerBound = 0;
        int upperBound = nrVals - 1;
        while (upperBound - lowerBound > 1) {
            int middle = lowerBound + (upperBound - lowerBound) / 2;
            if (rowList[middle] > prevTuple) {
                upperBound = middle;
            } else {
                lowerBound = middle;
            }
        }
        // Get next tuple
        for (int pos = lowerBound; pos <= upperBound; ++pos) {
            int val = rowList[pos];
            if (val > prevTuple) {
                return val;
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
     * @param value     indexed value
     * @param prevTuple index of last tuple
     * @param tid       thread id
     * @return index of next tuple or cardinality
     */
    public int nextTupleInScope(int value, int prevTuple, int tid) {
        tid = (value + tid) % nrThreads;
        // Get start position for indexed values
        int index = keyToPositions.getOrDefault(value, -1);
        // No indexed values?
        if (index < 0) {
            return cardinality;
        }
        int[] rowList = newPositions[index];
        // Can we return first indexed value?
        int nrVals = rowList.length;
        int firstOffset = tid;
        if (firstOffset >= nrVals) {
            return cardinality;
        }
        int firstTuple = rowList[firstOffset];
        if (firstTuple > prevTuple) {
            return firstTuple;
        }
        // Get number of indexed values
        int lastOffset = (nrVals - 1) / nrThreads * nrThreads + tid;
        // if the offset is beyond the array?
        if (lastOffset >= nrVals) {
            lastOffset -= nrThreads;
        }
        int threadVals = (lastOffset - firstOffset) / nrThreads + 1;
        // Update index-related statistics
        // Restrict search range via binary search
        int lowerBound = 0;
        int upperBound = threadVals - 1;
        while (upperBound - lowerBound > 1) {
            int middle = lowerBound + (upperBound - lowerBound) / 2;
            int middleOffset = middle * nrThreads + tid;
            if (rowList[middleOffset] > prevTuple) {
                upperBound = middle;
            } else {
                lowerBound = middle;
            }
        }
        // Get next tuple
        for (int pos = lowerBound; pos <= upperBound; ++pos) {
            int offset = pos * nrThreads + tid;
            int nextTuple = rowList[offset];
            if (nextTuple > prevTuple) {
                return nextTuple;
            }
        }
        // No suitable tuple found
        return cardinality;
    }

    @Override
    public boolean evaluate(int priorVal, int curIndex, int splitTable, int nextTable, int tid) {
        tid = (priorVal + tid) % nrThreads;

        boolean equal = priorVal == intData.data[curIndex];
        if (splitTable == nextTable) {
            return equal && scopes[curIndex] == tid;
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
     * @param logText text to log if activated
     */
    void log(String logText) {
        if (LoggingConfig.INDEXING_VERBOSE) {
            System.out.println(logText);
        }
    }

    /**
     * Splits table with given cardinality into tuple batches
     * according to the configuration for parallel processing.
     *
     * @return list of row ranges (batches)
     */
    public List<IndexRange> split() {
        List<IndexRange> batches = new ArrayList<>();
        int batchSize = Math.max(ParallelConfig.PRE_BATCH_SIZE, cardinality / 100);
        for (int batchCtr = 0; batchCtr * batchSize < cardinality;
             ++batchCtr) {
            int startIdx = batchCtr * batchSize;
            int tentativeEndIdx = startIdx + batchSize - 1;
            int endIdx = Math.min(cardinality - 1, tentativeEndIdx);
            IndexRange indexRange = new IndexRange(startIdx, endIdx, batchCtr);
            batches.add(indexRange);
        }
        return batches;
    }
}
