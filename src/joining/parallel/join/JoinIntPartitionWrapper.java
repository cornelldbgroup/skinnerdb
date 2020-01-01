package joining.parallel.join;

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
    public int nextIndex(int[] tupleIndices, int[] nextSize) {
        int priorTuple = tupleIndices[priorTable];
        int priorVal = priorIntData.data[priorTuple];
        int curTuple = tupleIndices[nextTable];
        return nextIntIndex.nextTuple(priorVal, curTuple, nextSize);
    }

    @Override
    public int nextIndexInScope(int[] tupleIndices, int tid, int[] nextSize) {
        int priorTuple = tupleIndices[priorTable];
        int priorVal = priorIntData.data[priorTuple];
        int curTuple = tupleIndices[nextTable];
        return nextIntIndex.nextTupleInScope(priorVal, priorTuple, curTuple, tid, nextSize);
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
}
