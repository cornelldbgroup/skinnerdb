package joining.parallel.indexing;

import com.koloboke.collect.IntCollection;
import com.koloboke.collect.map.DoubleIntCursor;
import com.koloboke.collect.map.DoubleIntMap;
import com.koloboke.collect.map.hash.HashDoubleIntMaps;
import config.ParallelConfig;
import data.DoubleData;
import predicate.Operator;
import query.ColumnRef;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.IntStream;

public class DoublePartitionIndex extends PartitionIndex {
    /**
     * Integer data that the index refers to.
     */
    public final DoubleData doubleData;
    /**
     * After indexing: maps search key to index
     * of first position at which associated
     * information is stored.
     */
    public DoubleIntMap keyToPositions;
    /**
     * Number of threads.
     */
    int nrThreads;
    /**
     * Data is distributed to different scopes of threads.
     */
    public byte[] scopes;
    /**
     * The associated column reference mentioned in the query.
     */
    public final ColumnRef queryRef;
    /**
     * Create index on the given integer column.
     *
     * @param doubleData        double data to index.
     * @param nrThreads         the number of threads.
     * @param colRef            column reference.
     * @param queryRef          column reference mentioned in the query.
     * @param origin            original index associated with the column.
     * @param policy            joining.parallel policy.
     */
    public DoublePartitionIndex(DoubleData doubleData, int nrThreads, ColumnRef colRef, ColumnRef queryRef,
                             DoublePartitionIndex origin, IndexPolicy policy) {
        super(doubleData.cardinality);
        // Extract info
        this.nrThreads = nrThreads;
        this.doubleData = doubleData;
        this.scopes = new byte[this.cardinality];
        this.queryRef = queryRef;

        if (policy == IndexPolicy.Key) {
            keyColumnIndex(origin);
        }
        else if (policy == IndexPolicy.Sparse) {
            parallelSparseIndex(origin);
        }
        else if (policy == IndexPolicy.Sequential) {
            sequentialIndex(colRef);
            groupIds = new int[keyToPositions.size()];
            int id = 0;
            for (Integer pos: keyToPositions.values()) {
                groupIds[id] = pos;
                id++;
            }
        }
        else {
            parallelDenseIndex(colRef);
        }
    }

    /**
     * Parallel method optimized for key column.
     * A key column is defined as unique value of each row.
     *
     * @param doubleIndex     original index associated with the column.
     */
    private void keyColumnIndex(DoublePartitionIndex doubleIndex) {
        double[] data = doubleData.data;
        int MAX = ParallelConfig.PRE_BATCH_SIZE;
        // if the cardinality is too small
        if (cardinality <= MAX) {
            keyToPositions = HashDoubleIntMaps.newMutableMap(this.cardinality);
            positions = new int[2 * cardinality];
            for (int i = 0; i < cardinality; ++i) {
                if (!doubleData.isNull.get(i)) {
                    double key = data[i];
                    int newPos = i * 2;
                    keyToPositions.put(key, newPos);
                    positions[newPos] = 1;
                    int pos = newPos + 1;
                    positions[pos] = i;
                    scopes[i] = 0;
                }
            }
        } else {
            keyToPositions = doubleIndex.keyToPositions;
            positions = new int[doubleIndex.positions.length];
            List<DoubleIndexRange> batches = this.split();
            batches.parallelStream().forEach(batch -> {
                // Evaluate predicate for each table row
                for (int rowCtr = batch.firstTuple; rowCtr <= batch.lastTuple; ++rowCtr) {
                    if (!doubleData.isNull.get(rowCtr)) {
                        double key = data[rowCtr];
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
     * @param doubleIndex     original index associated with the column.
     */
    private void parallelSparseIndex(DoublePartitionIndex doubleIndex) {
        double[] data = doubleData.data;
        keyToPositions = doubleIndex.keyToPositions;
        positions = new int[doubleIndex.positions.length];
        List<DoubleIndexRange> batches = this.split();
        // Count number of occurrences for each batch.
        batches.parallelStream().forEach(batch -> {
            // Evaluate predicate for each table row
            for (int rowCtr = batch.firstTuple; rowCtr <= batch.lastTuple; ++rowCtr) {
                if (!doubleData.isNull.get(rowCtr)) {
                    double value = data[rowCtr];
                    batch.add(value);
                }
            }
        });
        int nrBatches = batches.size();
        // joining.parallel fill prefix and indices value into positions array.
        IntStream.range(0, nrBatches).parallel().forEach(bid -> {
            DoubleIndexRange batch = batches.get(bid);
            DoubleIntCursor batchCursor = batch.valuesMap.cursor();
            batch.prefixMap = HashDoubleIntMaps.newMutableMap(batch.valuesMap.size());
            while (batchCursor.moveNext()) {
                double key = batchCursor.key();
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
                if (!doubleData.isNull.get(rowCtr)) {
                    double value = data[rowCtr];
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
                        scopes[rowCtr] = (byte) startThread;
                    }
                }
            }
        });
    }

    /**
     * Sequentially generate index for each column.
     * This function is called when the data is loading and the size is small.
     *
     * @param colRef
     */
    private void sequentialIndex(ColumnRef colRef) {
        double[] data = doubleData.data;
        // Count number of occurrences for each value
        DoubleIntMap keyToNr = HashDoubleIntMaps.newMutableMap(this.cardinality);
        for (int i = 0; i < cardinality; ++i) {
            // Don't index null values
            if (!doubleData.isNull.get(i)) {
                double value = data[i];
                int nr = keyToNr.getOrDefault(value, 0);
                keyToNr.put(value, nr + 1);
            }
        }
        // Assign each key to the appropriate position offset
        int nrKeys = keyToNr.size();
        keyToPositions = HashDoubleIntMaps.newMutableMap(nrKeys);
        log(colRef + ": Number of keys:\t" + nrKeys);
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
        log(colRef + "Prefix sum:\t" + prefixSum);
        // Generate position information
        positions = new int[prefixSum];
        for (int i = 0; i < cardinality; ++i) {
            if (!doubleData.isNull.get(i)) {
                double key = data[i];
                int startPos = keyToPositions.get(key);
                positions[startPos] += 1;
                int offset = positions[startPos];
                int pos = startPos + offset;
                positions[pos] = i;
                scopes[i] = (byte) ((offset - 1) % nrThreads);
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
        double[] data = doubleData.data;
        // Divide tuples into batches
        List<DoubleIndexRange> batches = this.split();
        DoubleIntMap keyToNr = HashDoubleIntMaps.newMutableMap(this.cardinality);
        batches.parallelStream().forEach(batch -> {
            // Evaluate predicate for each table row
            for (int rowCtr = batch.firstTuple; rowCtr <= batch.lastTuple; ++rowCtr) {
                if (!doubleData.isNull.get(rowCtr)) {
                    double value = data[rowCtr];
                    batch.add(value);
                }
            }
        });
        for (DoubleIndexRange batch : batches) {
            DoubleIntCursor keyToNrCursor = batch.valuesMap.cursor();
            while (keyToNrCursor.moveNext()) {
                double key = keyToNrCursor.key();
                int value = keyToNrCursor.value();
                keyToNr.computeIfPresent(key, (k, v) -> v + value);
                keyToNr.putIfAbsent(key, value);
            }
        }

        int nrKeys = keyToNr.size();
        keyToPositions = HashDoubleIntMaps.newMutableMap(nrKeys);
        DoubleIntCursor keyToNrCursor = keyToNr.cursor();
        int prefixSum = 0;
        int len = cardinality + nrKeys;
        positions = new int[len];
        while (keyToNrCursor.moveNext()) {
            double key = keyToNrCursor.key();
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
        // calculate prefix sum for each bath in joining.parallel
        IntStream.range(0, nrBatches).parallel().forEach(bid -> {
            DoubleIndexRange batch = batches.get(bid);
            DoubleIntCursor batchCursor = batch.valuesMap.cursor();
            batch.prefixMap = HashDoubleIntMaps.newMutableMap(batch.valuesMap.size());
            while (batchCursor.moveNext()) {
                double key = batchCursor.key();
                int prefix = 1;
                int startPos = keyToPositions.getOrDefault(key, 0);
                for (int i = 0; i < bid; i++) {
                    prefix += batches.get(i).valuesMap.getOrDefault(key, 0);
                }
                batch.prefixMap.put(key, prefix + startPos);
            }
            // Evaluate predicate for each table row
            for (int rowCtr = batch.firstTuple; rowCtr <= batch.lastTuple; ++rowCtr) {
                if (!doubleData.isNull.get(rowCtr)) {
                    double value = data[rowCtr];
                    int firstPos = keyToPositions.getOrDefault(value, 0);
                    int pos = batch.prefixMap.computeIfPresent(value, (k, v) -> v + 1) - 1;
                    positions[pos] = rowCtr;
                    int startThread = (pos - firstPos - 1) % nrThreads;
                    scopes[rowCtr] = (byte) startThread;
                }
            }
        });
    }


    /**
     * Splits table with given cardinality into tuple batches
     * according to the configuration for joining.parallel processing.
     *
     * @return list of row ranges (batches)
     */
    private List<DoubleIndexRange> split() {
        List<DoubleIndexRange> batches = new ArrayList<>();
        int batchSize = Math.max(ParallelConfig.PRE_INDEX_SIZE, cardinality / 100);
        for (int batchCtr = 0; batchCtr * batchSize < cardinality;
             ++batchCtr) {
            int startIdx = batchCtr * batchSize;
            int tentativeEndIdx = startIdx + batchSize - 1;
            int endIdx = Math.min(cardinality - 1, tentativeEndIdx);
            DoubleIndexRange doubleIndexRange = new DoubleIndexRange(startIdx, endIdx, batchCtr);
            batches.add(doubleIndexRange);
        }
        return batches;
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
    public int nextTuple(double value, int prevTuple, int[] nextSize) {
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
        if (nextSize != null)
            nextSize[0] = nrVals;
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
     * tuple exists. In this function we only run within partitions.
     *
     * @param value     indexed value
     * @param prevTuple index of last tuple
     * @param tid       thread id
     * @return index of next tuple or cardinality
     */
    public int nextTupleInScope(double value, int priorIndex, int prevTuple, int tid, int[] nextSize) {
        tid = (priorIndex + tid) % nrThreads;
        // Get start position for indexed values
        int firstPos = keyToPositions.getOrDefault(value, -1);
        // No indexed values?
        if (firstPos < 0) {
            return cardinality;
        }
        // Can we return first indexed value?
        int nrVals = positions[firstPos];
        if (nextSize != null)
            nextSize[0] = nrVals;
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

    /**
     * Evaluate the given predicate on current tuple
     * indices and returns true iff all predicates evaluate
     * to true.
     *
     * @param priorVal      value in the prior table.
     * @param curIndex      index of joining table.
     * @return              binary result of evaluation.
     */
    public boolean evaluate(double priorVal, int curIndex) {
        return priorVal == doubleData.data[curIndex];
    }

    /**
     * Evaluate the given predicate on current tuple
     * indices and returns true iff all predicates evaluate
     * to true and indices are in partitions for given thread.
     *
     * @param priorVal      value in the prior table.
     * @param priorIndex    index in the prior table.
     * @param curIndex      index of joining table.
     * @param tid           thread id
     * @return              binary result of evaluation.
     */
    public boolean evaluateInScope(double priorVal, int priorIndex, int curIndex, int tid) {
        tid = (priorIndex + tid) % nrThreads;
        return priorVal == doubleData.data[curIndex] && scopes[curIndex] == tid;
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

    @Override
    public boolean evaluate(int curTuple, Number constant, Operator operator) {
        double target = constant.doubleValue();
        double value = doubleData.data[curTuple];
        if (operator == Operator.EqualsTo) {
            return value == target;
        }
        else if (operator == Operator.GreaterThan) {
            return value > target;
        }
        else if (operator == Operator.GreaterThanEquals) {
            return value >= target;
        }
        else if (operator == Operator.MinorThan) {
            return value < target;
        }
        else if (operator == Operator.MinorThanEquals) {
            return value <= target;
        }
        else if (operator == Operator.NotEqualsTo) {
            return value != target;
        }
        else if (operator == Operator.NotEqualsAll) {
            return !keyToPositions.containsKey(target);
        }
        System.out.println("Wrong");
        return false;
    }

    @Override
    public boolean exist(Number constant, Operator operator) {
        double target = constant.doubleValue();
        if (operator == Operator.EqualsTo) {
            return keyToPositions.containsKey(target);
        }
        else if (operator == Operator.NotEqualsTo) {
            return keyToPositions.size() > 1 || !keyToPositions.containsKey(target);
        }
        System.out.println("Wrong");
        return false;
    }

    @Override
    public Number getNumber(int curTuple) {
        return doubleData.data[curTuple];
    }

    @Override
    public void sortRows() {
        sortedRow = IntStream.range(0, cardinality)
                .boxed().sorted(Comparator.comparingDouble(i -> doubleData.data[i]))
                .mapToInt(ele -> ele).toArray();
    }

    @Override
    public IntCollection posSet() {
        return keyToPositions.values();
    }
}
