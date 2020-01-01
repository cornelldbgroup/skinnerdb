package joining.parallel.join;

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
    public int nextIndex(int[] tupleIndices, int[] nextSize) {
        int priorTuple = tupleIndices[priorTable];
        double priorVal = priorDoubleData.data[priorTuple];
        int curTuple = tupleIndices[nextTable];
        return nextDoubleIndex.nextTuple(priorVal, curTuple, nextSize);
    }

    @Override
    public int nextIndexInScope(int[] tupleIndices, int tid, int[] nextSize) {
        int priorTuple = tupleIndices[priorTable];
        double priorVal = priorDoubleData.data[priorTuple];
        int curTuple = tupleIndices[nextTable];
        return nextDoubleIndex.nextTupleInScope(priorVal, priorTuple, curTuple, tid, nextSize);
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
}
