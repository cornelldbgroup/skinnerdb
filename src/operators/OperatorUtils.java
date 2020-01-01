package operators;

import config.ParallelConfig;
import data.DoubleData;
import data.IntData;
import data.LongData;

import java.util.ArrayList;
import java.util.List;

public class OperatorUtils {
    /**
     * Splits table with given cardinality into tuple batches
     * according to the configuration for joining.parallel processing.
     *
     * @return list of row ranges (batches)
     */
    public static List<RowRange> split(int cardinality) {
        List<RowRange> batches = new ArrayList<>();
        int batchSize = Math.max(ParallelConfig.PARALLEL_SIZE, cardinality / 100);
        for (int batchCtr = 0; batchCtr * batchSize < cardinality;
             ++batchCtr) {
            int startIdx = batchCtr * batchSize;
            int tentativeEndIdx = startIdx + batchSize - 1;
            int endIdx = Math.min(cardinality - 1, tentativeEndIdx);
            RowRange rowRange = new RowRange(startIdx, endIdx);
            batches.add(rowRange);
        }
        return batches;
    }

    public static List<RowRange> split(int cardinality, int nrBatches) {
        List<RowRange> batches = new ArrayList<>();
        int maxSize = (int) Math.floor((cardinality + 0.0) / nrBatches);
        int remainingSize = cardinality - maxSize * nrBatches;
        int start = 0;
        for (int batchCtr = 0; start < cardinality; ++batchCtr) {
            int startIdx = start;
            int tentativeEndIdx = start + maxSize + (batchCtr < remainingSize ? 0 : -1);
            int endIdx = Math.min(cardinality - 1, tentativeEndIdx);
            RowRange rowRange = new RowRange(startIdx, endIdx);
            start = endIdx + 1;
            batches.add(rowRange);
        }
        return batches;
    }

    public static List<Integer> filterBatch(IntData intData,
                                            RowRange batch) {
        List<Integer> result = new ArrayList<>();
        // Evaluate predicate for each table row
        int batchData = 0;
        for (int resultRow = batch.firstTuple; resultRow <= batch.lastTuple; ++resultRow) {
            // Either map row to row or row to group
            if (!intData.isNull.get(resultRow)) {
                batchData += intData.data[resultRow];
            }
        }
        result.add(batchData);
        return result;
    }

    public static List<Integer> filterBatch(IntData intData,
                                     RowRange batch, List<Integer> groupRows) {
        List<Integer> result = new ArrayList<>();
        // Evaluate predicate for each table row
        int batchData = 0;
        for (int resultRow = batch.firstTuple; resultRow <= batch.lastTuple; ++resultRow) {
            // Either map row to row or row to group
            int row = groupRows.get(resultRow);
            if (!intData.isNull.get(row)) {
                batchData += intData.data[row];
            }
        }
        result.add(batchData);
        return result;
    }

    public static List<Long> filterBatch(LongData longData,
                                         RowRange batch) {
        List<Long> result = new ArrayList<>();
        // Evaluate predicate for each table row
        long batchData = 0;
        for (int resultRow = batch.firstTuple; resultRow <= batch.lastTuple; ++resultRow) {
            // Either map row to row or row to group
            if (!longData.isNull.get(resultRow)) {
                batchData += longData.data[resultRow];
            }
        }
        result.add(batchData);
        return result;
    }

    public static List<Long> filterBatch(LongData longData,
                                  RowRange batch, List<Integer> groupRows) {
        List<Long> result = new ArrayList<>();
        // Evaluate predicate for each table row
        long batchData = 0;
        for (int resultRow = batch.firstTuple; resultRow <= batch.lastTuple; ++resultRow) {
            // Either map row to row or row to group
            int row = groupRows.get(resultRow);
            if (!longData.isNull.get(row)) {
                batchData += longData.data[row];
            }
        }
        result.add(batchData);
        return result;
    }

    public static List<Double> filterBatch(DoubleData doubleData,
                                           RowRange batch) {
        List<Double> result = new ArrayList<>();
        // Evaluate predicate for each table row
        double batchData = 0;
        for (int resultRow = batch.firstTuple; resultRow <= batch.lastTuple; ++resultRow) {
            // Either map row to row or row to group
            if (!doubleData.isNull.get(resultRow)) {
                batchData += doubleData.data[resultRow];
            }
        }
        result.add(batchData);
        return result;
    }

    public static List<Double> filterBatch(DoubleData doubleData,
                                           RowRange batch, List<Integer> groupRows) {
        List<Double> result = new ArrayList<>();
        // Evaluate predicate for each table row
        double batchData = 0;
        for (int resultRow = batch.firstTuple; resultRow <= batch.lastTuple; ++resultRow) {
            // Either map row to row or row to group
            int row = groupRows.get(resultRow);
//            int row = resultRow;
            if (!doubleData.isNull.get(row)) {
                batchData += doubleData.data[row];
            }
        }
        result.add(batchData);
        return result;
    }

    public static List<int[]> avgBatch(IntData intData,
                                      RowRange batch, List<Integer> groupRows) {
        List<int[]> result = new ArrayList<>();
        // Evaluate predicate for each table row
        int batchData = 0;
        int batchNr = 0;
        for (int resultRow = batch.firstTuple; resultRow <= batch.lastTuple; ++resultRow) {
            // Either map row to row or row to group
            int row = groupRows.get(resultRow);
            if (!intData.isNull.get(row)) {
                batchData += intData.data[row];
                batchNr++;
            }
        }
        result.add(new int[]{batchData, batchNr});
        return result;
    }

    public static List<long[]> avgBatch(LongData longData,
                                                        RowRange batch, List<Integer> groupRows) {
        List<long[]> result = new ArrayList<>();
        // Evaluate predicate for each table row
        long batchData = 0;
        int batchNr = 0;
        for (int resultRow = batch.firstTuple; resultRow <= batch.lastTuple; ++resultRow) {
            // Either map row to row or row to group
            int row = groupRows.get(resultRow);
            if (!longData.isNull.get(row)) {
                batchData += longData.data[row];
                batchNr++;
            }
        }
        result.add(new long[]{batchData, batchNr});
        return result;
    }
    public static List<double[]> avgBatch(DoubleData doubleData,
                                                        RowRange batch, List<Integer> groupRows) {
        List<double[]> result = new ArrayList<>();
        // Evaluate predicate for each table row
        double batchData = 0;
        int batchNr = 0;
        for (int resultRow = batch.firstTuple; resultRow <= batch.lastTuple; ++resultRow) {
            // Either map row to row or row to group
            int row = groupRows.get(resultRow);
            if (!doubleData.isNull.get(row)) {
                batchData += doubleData.data[row];
                batchNr++;
            }
        }
        result.add(new double[]{batchData, batchNr});
        return result;
    }
}
