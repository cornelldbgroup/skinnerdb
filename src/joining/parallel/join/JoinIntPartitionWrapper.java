package joining.parallel.join;

import com.koloboke.collect.set.IntSet;
import data.IntData;
import expressions.ExpressionInfo;
import joining.parallel.indexing.IntPartitionIndex;

public class JoinIntPartitionWrapper extends JoinPartitionIndexWrapper {
    /**
     * Reference to prior integer column data.
     */
    final IntData priorIntData;
    /**
     * Reference to next integer index.
     */
    final IntPartitionIndex nextIntIndex;
    /**
     * Initializes wrapper providing access to integer index
     * on column that appears in equi-join predicate.
     *
     * @param equiPred join predicate associated with join index wrapper.
     * @param order    the order of join tables.
     */
    public JoinIntPartitionWrapper(ExpressionInfo equiPred, int[] order) throws Exception {
        super(equiPred, order);
        priorIntData = (IntData)priorData;
        nextIntIndex = (IntPartitionIndex)nextIndex;
    }

    @Override
    public void reset(int[] tupleIndices) {
        int priorTuple = tupleIndices[priorTable];
        int priorVal = priorIntData.data[priorTuple];
        int prevTuple = tupleIndices[nextTable];
        if (!(lastValue == priorVal &&
                lastPositionsStart >= 0 &&
                nextIntIndex.positions[lastPositionsStart] <= prevTuple)) {
            lastPositionsStart = -1;
            lastPositionsEnd = -1;
        }
    }

    @Override
    public int nextIndex(int[] tupleIndices, int[] nextSize) {
        int priorTuple = tupleIndices[priorTable];
        int priorVal = priorIntData.data[priorTuple];
        int curTuple = tupleIndices[nextTable];
        return nextIntIndex.nextTuple(priorVal, curTuple, nextTable, nextSize);
    }

    @Override
    public int nextIndexFromLast(int[] tupleIndices, int[] nextSize) {
        return 0;
    }

    @Override
    public int nextIndexFromLast(int[] tupleIndices, int[] nextSize, int tid) {
        int priorTuple = tupleIndices[priorTable];
        int priorVal = priorIntData.data[priorTuple];
        int prevTuple = tupleIndices[nextTable];
        int cardinality = nextIntIndex.cardinality;
        if (nextIntIndex.unique) {
            int onlyRow = nextIntIndex.keyToPositions.getOrDefault(priorVal, cardinality);
            return onlyRow > prevTuple ? onlyRow : cardinality;
        }
        else {
            int[] positions = nextIntIndex.positions;
            int nextIndex = -1;
            if (lastPositionsStart >= 0) {
                for (int i = lastPositionsStart + 1; i <= lastPositionsEnd; i++) {
                    nextIndex = positions[i];
                    if (nextIndex > prevTuple) {
                        lastPositionsStart = i;
                        return nextIndex;
                    }
                }
            }
            else {
                int firstPos = lastValue == priorVal ?
                        lastFirst : nextIntIndex.keyToPositions.getOrDefault(priorVal, -1);
                lastValue = priorVal;
                lastFirst = firstPos;
                if (firstPos < 0) {
                    return cardinality;
                }
                // Get number of indexed values
                int nrVals = nextIntIndex.positions[firstPos];
                // Can we return first indexed value?
                int firstTuple = nextIntIndex.positions[firstPos + 1];
                if (firstTuple > prevTuple) {
                    lastPositionsStart = firstPos + 1;
                    lastPositionsEnd = firstPos + nrVals;
                    return firstTuple;
                }
                for (int i = 2; i <= nrVals; i++) {
                    nextIndex = positions[firstPos + i];
                    if (nextIndex > prevTuple) {
                        lastPositionsStart = firstPos + i;
                        lastPositionsEnd = firstPos + nrVals;
                        return nextIndex;
                    }
                }
            }
            lastPositionsStart = -1;
            return cardinality;
        }
    }

    @Override
    public int nextIndexInScope(int[] tupleIndices, int tid, int[] nextSize) {
        int priorTuple = tupleIndices[priorTable];
        int priorVal = priorIntData.data[priorTuple];
        int curTuple = tupleIndices[nextTable];
        return nextIntIndex.nextTupleInScope(priorVal, priorTuple, curTuple, tid, nextTable, nextSize);
    }

    @Override
    public int nextIndexInScope(int[] tupleIndices, int tid, int nrDPThreads, int[] nextSize) {
        int priorTuple = tupleIndices[priorTable];
        int priorVal = priorIntData.data[priorTuple];
        int curTuple = tupleIndices[nextTable];
        return nextIntIndex.nextTupleInScope(priorVal, priorTuple, curTuple, tid, nextTable, nrDPThreads, nextSize);
    }

    @Override
    public int nextIndexInScope(int[] tupleIndices, int tid, int[] nextSize, IntSet finishedThreads) {
        int priorTuple = tupleIndices[priorTable];
        int priorVal = priorIntData.data[priorTuple];
        int curTuple = tupleIndices[nextTable];
        return nextIntIndex.nextTupleInScope(priorVal, priorTuple, curTuple, tid, nextTable, nextSize, finishedThreads);
    }

    @Override
    public boolean evaluate(int[] tupleIndices) {
        int priorTuple = tupleIndices[priorTable];
        int priorVal = priorIntData.data[priorTuple];
        int curTuple = tupleIndices[nextTable];
        return nextIntIndex.evaluate(priorVal, curTuple);
    }

    @Override
    public boolean evaluateInScope(int[] tupleIndices, int tid) {
        int priorTuple = tupleIndices[priorTable];
        int priorVal = priorIntData.data[priorTuple];
        int curTuple = tupleIndices[nextTable];
        return nextIntIndex.evaluateInScope(priorVal, priorTuple, curTuple, tid);
    }

    @Override
    public boolean evaluateInScope(int[] tupleIndices, int tid, int nrDPThreads) {
        int priorTuple = tupleIndices[priorTable];
        int priorVal = priorIntData.data[priorTuple];
        int curTuple = tupleIndices[nextTable];
        return nextIntIndex.evaluateInScope(priorVal, priorTuple, curTuple, tid, nrDPThreads);
    }

    @Override
    public boolean evaluateInScope(int[] tupleIndices, int tid, IntSet finishedThreads) {
        int priorTuple = tupleIndices[priorTable];
        int priorVal = priorIntData.data[priorTuple];
        int curTuple = tupleIndices[nextTable];
        return nextIntIndex.evaluateInScope(priorVal, priorTuple, curTuple, tid, finishedThreads);
    }

    @Override
    public int nrIndexed(int[] tupleIndices) {
        int priorTuple = tupleIndices[priorTable];
        int priorVal = priorIntData.data[priorTuple];
        return nextIntIndex.nrIndexed(priorVal);
    }

    @Override
    public int indexSize(int[] tupleIndices, int[] points) {
        int priorTuple = tupleIndices[priorTable];
        int priorVal = priorIntData.data[priorTuple];
        int prevTuple = tupleIndices[nextTable];
        int diff = prevTuple == 0 ? 0 : 1;
        if (nextIntIndex.unique) {
            int onlyRow = nextIntIndex.keyToPositions.getOrDefault(priorVal, -1);
            if (onlyRow - prevTuple >= diff) {
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
            int firstPos = nextIntIndex.keyToPositions.getOrDefault(priorVal, -1);
            if (firstPos < 0 || nextIntIndex.positions[firstPos] == 0) {
                points[0] = 0;
                points[1] = -1;
                return 0;
            }
            // Get number of indexed values
            int nrVals = nextIntIndex.positions[firstPos];
            int[] positions = nextIntIndex.positions;
            // Can we return first indexed value?
            int firstTuple = positions[firstPos + 1];
            if (firstTuple - prevTuple >= diff) {
                points[0] = firstPos + 1;
                points[1] = firstPos + nrVals;
                return nrVals;
            }
            int size = nrVals;
            for (int i = 2; i <= nrVals; i++) {
                if (positions[firstPos + i] - prevTuple >=diff) {
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
