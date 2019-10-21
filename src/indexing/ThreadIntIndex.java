package indexing;

import com.koloboke.collect.map.IntIntCursor;
import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMaps;
import config.LoggingConfig;
import config.ParallelConfig;
import data.IntData;
import query.ColumnRef;

import java.util.*;
import java.util.stream.IntStream;

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
     * The associated column reference mentioned in the query.
     */
    public final ColumnRef queryRef;

    /**
     * Create index on the given integer column.
     *
     * @param intData       integer data to index.
     * @param nrThreads     the number of threads.
     * @param colRef        column reference.
     * @param queryRef      column reference mentioned in the query.
     * @param index         original index associated with the column.
     * @param policy        parallel policy.
     */
    public ThreadIntIndex(IntData intData, int nrThreads, ColumnRef colRef, ColumnRef queryRef, ThreadIntIndex index, IndexPolicy policy) {
        long startMillis = System.currentTimeMillis();
        // Extract info
        this.nrThreads = nrThreads;
        this.intData = intData;
        this.cardinality = intData.cardinality;
        this.scopes = new int[this.cardinality];
        this.queryRef = queryRef;

        // Count number of occurrences for each value
        int MAX = 10000;
        if (policy == IndexPolicy.Key) {
            keyIndex(index);
        }
        else if (cardinality <= MAX || policy == IndexPolicy.Sequential) {
            sequentialIndex(colRef);
        }
        else if (policy == IndexPolicy.Sparse) {
            parallelSparseIndex(colRef, index);
        }
        else {
            parallelDenseIndex(colRef);
        }

        // Output statistics for performance tuning
        if (LoggingConfig.INDEXING_VERBOSE) {
            long totalMillis = System.currentTimeMillis() - startMillis;
            log(colRef + ": Created index for integer column with cardinality " +
                    cardinality + " in " + totalMillis + " ms.");
        }
        // Check index if enabled
        IndexChecker.checkIndex(intData, this, nrThreads);
    }


    @Override
    public int nextTuple(int value, int prevTuple) {
        // Get start position for indexed values
        int firstPos = keyToPositions.getOrDefault(value, -1);
        // No indexed values?
        if (firstPos < 0) {
            return cardinality;
        }
        // Can we return first indexed value?
        int firstTuple = positions[firstPos + 1];
        if (firstTuple > prevTuple) {
            return firstTuple;
        }
        // Get number of indexed values
        int nrVals = positions[firstPos];
        // Restrict search range via binary search
        int lowerBound = firstPos + 1;
        int upperBound = firstPos + nrVals;
        while (upperBound - lowerBound > 1) {
            int middle = lowerBound + (upperBound - lowerBound) / 2;
            if (positions[middle] > prevTuple) {
                upperBound = middle;
            } else {
                lowerBound = middle;
            }
        }
        // Get next tuple
        for (int pos = lowerBound; pos <= upperBound; ++pos) {
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
     * @param value     indexed value
     * @param prevTuple index of last tuple
     * @param tid       thread id
     * @return index of next tuple or cardinality
     */
    public int nextTupleInScope(int value, int prevTuple, int tid) {
        tid = (value + tid) % nrThreads;
        // Get start position for indexed values
        int firstPos = keyToPositions.getOrDefault(value, -1);
        // No indexed values?
        if (firstPos < 0) {
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
        int batchSize = Math.max(ParallelConfig.PRE_INDEX_SIZE, cardinality / 100);
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

    /**
     * Sequentially generate index for each column.
     * This function is called when the data is loading and the size is small.
     *
     * @param colRef
     */
    public void sequentialIndex(ColumnRef colRef) {
        int[] data = intData.data;
        // Count number of occurrences for each value
        IntIntMap keyToNr = HashIntIntMaps.newMutableMap(this.cardinality);
        for (int i = 0; i < cardinality; ++i) {
            // Don't index null values
            if (!intData.isNull.get(i)) {
                int value = data[i];
                int nr = keyToNr.getOrDefault(value, 0);
                keyToNr.put(value, nr + 1);
            }
        }
        // Assign each key to the appropriate position offset
        int nrKeys = keyToNr.size();
        keyToPositions = HashIntIntMaps.newMutableMap(nrKeys);
        log(colRef + ": Number of keys:\t" + nrKeys);
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
        log(colRef + "Prefix sum:\t" + prefixSum);
        // Generate position information
        positions = new int[prefixSum];
        for (int i = 0; i < cardinality; ++i) {
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
    }

    /**
     * Parallel method optimized for dense column.
     * A dense column is defined as large cardinality
     * but small number of distinct values.
     *
     * @param colRef
     */
    private void parallelDenseIndex(ColumnRef colRef) {
        int[] data = intData.data;
        // Divide tuples into batches
        List<IndexRange> batches = this.split();
        IntIntMap keyToNr = HashIntIntMaps.newMutableMap(this.cardinality);
        batches.parallelStream().forEach(batch -> {
            // Evaluate predicate for each table row
            for (int rowCtr = batch.firstTuple; rowCtr <= batch.lastTuple; ++rowCtr) {
                if (!intData.isNull.get(rowCtr)) {
                    int value = data[rowCtr];
                    batch.add(value);
                }
            }
        });
        for (IndexRange batch : batches) {
            IntIntCursor keyToNrCursor = batch.valuesMap.cursor();
            while (keyToNrCursor.moveNext()) {
                int key = keyToNrCursor.key();
                int value = keyToNrCursor.value();
                keyToNr.computeIfPresent(key, (k, v) -> v + value);
                keyToNr.putIfAbsent(key, value);
            }
        }

        int nrKeys = keyToNr.size();
        keyToPositions = HashIntIntMaps.newMutableMap(nrKeys);
        IntIntCursor keyToNrCursor = keyToNr.cursor();
        int prefixSum = 0;
        int len = cardinality + nrKeys;
        positions = new int[len];
        while (keyToNrCursor.moveNext()) {
            int key = keyToNrCursor.key();
            int nr = keyToNrCursor.value();
            keyToPositions.put(key, prefixSum);
            positions[prefixSum] = nr;
            // Advance offset taking into account
            // space for row indices and one field
            // storing the number of following indices.
            int nrFields = nr + 1;
            prefixSum += nrFields;
        }
        int nrBatches = batches.size();
        IntStream.range(0, nrBatches).parallel().forEach(bid -> {
            IndexRange batch = batches.get(bid);
            IntIntCursor batchCursor = batch.valuesMap.cursor();
            batch.prefixMap = HashIntIntMaps.newMutableMap(batch.valuesMap.size());
            while (batchCursor.moveNext()) {
                int key = batchCursor.key();
                int prefix = 1;
                int startPos = keyToPositions.getOrDefault(key, 0);
                for (int i = 0; i < bid; i++) {
                    prefix += batches.get(i).valuesMap.getOrDefault(key, 0);
                }
                batch.prefixMap.put(key, prefix + startPos);
            }
            // Evaluate predicate for each table row
            for (int rowCtr = batch.firstTuple; rowCtr <= batch.lastTuple; ++rowCtr) {
                if (!intData.isNull.get(rowCtr)) {
                    int value = data[rowCtr];
                    int firstPos = keyToPositions.getOrDefault(value, 0);
                    int pos = batch.prefixMap.computeIfPresent(value, (k, v) -> v + 1) - 1;
                    positions[pos] = rowCtr;
                    int startThread = (pos - firstPos - 1) % nrThreads;
                    scopes[rowCtr] = startThread;
                }
            }
        });
    }
    /**
     * Parallel method optimized for key column.
     * A key column is defined as unique value of each row.
     *
     * @param intIndex     original index associated with the column.
     */
    private void keyIndex(ThreadIntIndex intIndex) {
        int[] data = intData.data;
//        keyToPositions = HashIntIntMaps.newMutableMap(this.cardinality);
        // Count number of occurrences for each value
        int MAX = ParallelConfig.PRE_BATCH_SIZE;
        if (!ParallelConfig.PARALLEL_PRE || cardinality <= MAX) {
            keyToPositions = HashIntIntMaps.newMutableMap(this.cardinality);
            positions = new int[2 * cardinality];
            for (int i = 0; i < cardinality; ++i) {
                if (!intData.isNull.get(i)) {
                    int key = data[i];
                    int newPos = i * 2;
                    keyToPositions.put(key, newPos);
                    positions[newPos] = 1;
                    int pos = newPos + 1;
                    positions[pos] = i;
                    scopes[i] = 0;
                }
            }
        } else {
            keyToPositions = intIndex.keyToPositions;
            positions = new int[intIndex.positions.length];
            List<IndexRange> batches = this.split();
            batches.parallelStream().forEach(batch -> {
                // Evaluate predicate for each table row
                for (int rowCtr = batch.firstTuple; rowCtr <= batch.lastTuple; ++rowCtr) {
                    if (!intData.isNull.get(rowCtr)) {
                        int key = data[rowCtr];
                        int newPos = keyToPositions.getOrDefault(key, 0);
                        positions[newPos] = 1;
                        int pos = newPos + 1;
                        positions[pos] = rowCtr;
                        scopes[rowCtr] = 0;
                    }
                }
            });
        }
    }
    /**
     * Parallel method optimized for sparse column.
     * A sparse column is defined as large cardinality
     * but less number of occurrence for each value.
     *
     * @param index     original index associated with the column.
     */
    private void parallelSparseIndex(ColumnRef colRef, ThreadIntIndex index) {
        long timer0 = System.currentTimeMillis();
        int[] data = intData.data;
        keyToPositions = index.keyToPositions;
        // Count number of occurrences for each value
        positions = new int[index.positions.length];
        List<IndexRange> batches = this.split();
        batches.parallelStream().forEach(batch -> {
            // Evaluate predicate for each table row
            for (int rowCtr = batch.firstTuple; rowCtr <= batch.lastTuple; ++rowCtr) {
                if (!intData.isNull.get(rowCtr)) {
                    int value = data[rowCtr];
                    batch.add(value);
                }
            }
        });

        int nrBatches = batches.size();
        long timer1 = System.currentTimeMillis();
        IntStream.range(0, nrBatches).parallel().forEach(bid -> {
            IndexRange batch = batches.get(bid);
            IntIntCursor batchCursor = batch.valuesMap.cursor();
            batch.prefixMap = HashIntIntMaps.newMutableMap(batch.valuesMap.size());
            while (batchCursor.moveNext()) {
                int key = batchCursor.key();
                int prefix = 1;
                int localNr = batchCursor.value();
                int startPos = keyToPositions.getOrDefault(key, -1);

                for (int i = 0; i < bid; i++) {
                    prefix += batches.get(i).valuesMap.getOrDefault(key, 0);
                }
                int nrValue = positions[startPos];
                if (nrValue == 0) {
                    int nr = prefix - 1 + localNr;
                    for (int i = bid + 1; i < nrBatches; i++) {
                        nr += batches.get(i).valuesMap.getOrDefault(key, 0);
                    }
                    positions[startPos] = nr;
                    nrValue = nr;
                }
                if (nrValue > 1) {
                    batch.prefixMap.put(key, prefix + startPos);
                }
            }
            // Evaluate predicate for each table row
            for (int rowCtr = batch.firstTuple; rowCtr <= batch.lastTuple; ++rowCtr) {
                if (!intData.isNull.get(rowCtr)) {
                    int value = data[rowCtr];
                    int firstPos = keyToPositions.getOrDefault(value, -1);
                    int nr = positions[firstPos];
                    if (nr == 1) {
                        positions[firstPos + 1] = rowCtr;
                        scopes[rowCtr] = 0;
                    }
                    else {
                        int pos = batch.prefixMap.computeIfPresent(value, (k, v) -> v + 1) - 1;
                        positions[pos] = rowCtr;
                        int startThread = (pos - firstPos - 1) % nrThreads;
                        scopes[rowCtr] = startThread;
                    }
                }
            }
        });
        long timer2 = System.currentTimeMillis();
        //            long timer3 = System.currentTimeMillis();
        System.out.println(colRef + " Sparse Parallel: " + (timer1 - timer0) + "\t"
                + (timer2 - timer1));
    }

}
