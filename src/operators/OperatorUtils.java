package operators;

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
        int batchSize = Math.max(10000, cardinality / 300);
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

    public static List<RowRange> split(int cardinality,
                                       int maxBatchSize,
                                       int minBatchNum) {
        List<RowRange> batches = new ArrayList<>();
        int batchSize = Math.max(maxBatchSize, cardinality / minBatchNum);
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
}
