package joining.parallel.join;

import com.koloboke.collect.set.IntSet;
import data.DoubleData;
import expressions.ExpressionInfo;
import joining.parallel.indexing.DoublePartitionIndex;

public class JoinDoublePartitionWrapper extends JoinPartitionIndexWrapper {
    /**
     * Reference to prior double column data.
     */
    final DoubleData priorDoubleData;
    /**
     * Reference to next double index.
     */
    final DoublePartitionIndex nextDoubleIndex;
    /**
     * Initialize index wrapper for
     * given query and join order.
     *
     * @param equiPred join predicate associated with join index wrapper.
     * @param order    the order of join tables.
     */
    public JoinDoublePartitionWrapper(ExpressionInfo equiPred, int[] order) {
        super(equiPred, order);
        priorDoubleData = (DoubleData)priorData;
        nextDoubleIndex = (DoublePartitionIndex)nextIndex;
    }

    @Override
    public void reset(int[] tupleIndices) {

    }

    @Override
    public int nextIndex(int[] tupleIndices, int[] nextSize) {
        int priorTuple = tupleIndices[priorTable];
        double priorVal = priorDoubleData.data[priorTuple];
        int curTuple = tupleIndices[nextTable];
        return nextDoubleIndex.nextTuple(priorVal, curTuple, nextTable, nextSize);
    }

    @Override
    public int nextIndexFromLast(int[] tupleIndices, int[] nextSize) {
        return 0;
    }

    @Override
    public int nextIndexFromLast(int[] tupleIndices, int[] nextSize, int tid) {
        return 0;
    }

    @Override
    public int nextIndexInScope(int[] tupleIndices, int tid, int[] nextSize) {
        int priorTuple = tupleIndices[priorTable];
        double priorVal = priorDoubleData.data[priorTuple];
        int curTuple = tupleIndices[nextTable];
        return nextDoubleIndex.nextTupleInScope(priorVal, priorTuple, curTuple, tid, nextTable, nextSize);
    }

    @Override
    public int nextIndexInScope(int[] tupleIndices, int tid, int nrDPThreads, int[] nextSize) {
        int priorTuple = tupleIndices[priorTable];
        double priorVal = priorDoubleData.data[priorTuple];
        int curTuple = tupleIndices[nextTable];
        return nextDoubleIndex.nextTupleInScope(priorVal, priorTuple, curTuple, tid, nextTable, nrDPThreads, nextSize);
    }

    @Override
    public int nextIndexInScope(int[] tupleIndices, int tid, int[] nextSize, IntSet finishedThreads) {
        int priorTuple = tupleIndices[priorTable];
        double priorVal = priorDoubleData.data[priorTuple];
        int curTuple = tupleIndices[nextTable];
        return nextDoubleIndex.nextTupleInScope(priorVal, priorTuple, curTuple, tid, nextTable, nextSize, finishedThreads);
    }

    @Override
    public boolean evaluate(int[] tupleIndices) {
        int priorTuple = tupleIndices[priorTable];
        double priorVal = priorDoubleData.data[priorTuple];
        int curTuple = tupleIndices[nextTable];
        return nextDoubleIndex.evaluate(priorVal, curTuple);
    }

    @Override
    public boolean evaluateInScope(int[] tupleIndices, int tid) {
        int priorTuple = tupleIndices[priorTable];
        double priorVal = priorDoubleData.data[priorTuple];
        int curTuple = tupleIndices[nextTable];
        return nextDoubleIndex.evaluateInScope(priorVal, priorTuple, curTuple, tid);
    }

    @Override
    public boolean evaluateInScope(int[] tupleIndices, int tid, int nrDPThreads) {
        int priorTuple = tupleIndices[priorTable];
        double priorVal = priorDoubleData.data[priorTuple];
        int curTuple = tupleIndices[nextTable];
        return nextDoubleIndex.evaluateInScope(priorVal, priorTuple, curTuple, tid, nrDPThreads);
    }

    @Override
    public boolean evaluateInScope(int[] tupleIndices, int tid, IntSet finishedThreads) {
        int priorTuple = tupleIndices[priorTable];
        double priorVal = priorDoubleData.data[priorTuple];
        int curTuple = tupleIndices[nextTable];
        return nextDoubleIndex.evaluateInScope(priorVal, priorTuple, curTuple, tid, finishedThreads);
    }

    @Override
    public int nrIndexed(int[] tupleIndices) {
        int priorTuple = tupleIndices[priorTable];
        double priorVal = priorDoubleData.data[priorTuple];
        return nextDoubleIndex.nrIndexed(priorVal);
    }

    @Override
    public int indexSize(int[] tupleIndices, int[] points) {
        int priorTuple = tupleIndices[priorTable];
        double priorVal = priorDoubleData.data[priorTuple];
        int prevTuple = tupleIndices[nextTable];
        if (nextDoubleIndex.unique) {
            int onlyRow = nextDoubleIndex.keyToPositions.getOrDefault(priorVal, -1);
            if (onlyRow > prevTuple) {
                points[0] = onlyRow;
                points[1] = onlyRow;
                return 1;
            }
            else {
                points[0] = 0;
                points[1] = -1;
                return 0;
            }
        }
        else {
            int firstPos = nextDoubleIndex.keyToPositions.getOrDefault(priorVal, -1);
            if (firstPos < 0) {
                points[0] = 0;
                points[1] = -1;
                return 0;
            }
            // Get number of indexed values
            int nrVals = nextDoubleIndex.positions[firstPos];
            int[] positions = nextDoubleIndex.positions;
            // Can we return first indexed value?
            int firstTuple = positions[firstPos + 1];
            if (firstTuple > prevTuple) {
                points[0] = firstPos + 1;
                points[1] = firstPos + nrVals;
                return nrVals;
            }
            int size = nrVals;
            for (int i = 2; i <= nrVals; i++) {
                if (positions[firstPos + i] > prevTuple) {
                    points[0] = firstPos + i;
                    points[1] = firstPos + nrVals;
                    size = nrVals - i + 1;
                    break;
                }
            }
            return size;
        }
    }
}
