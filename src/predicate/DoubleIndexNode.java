package predicate;

import buffer.BufferManager;
import joining.parallel.indexing.DoublePartitionIndex;
import joining.parallel.indexing.IntPartitionIndex;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import query.ColumnRef;
import query.QueryInfo;

import java.util.List;
import java.util.Map;

public class DoubleIndexNode extends NonEquiNode {
    DoublePartitionIndex priorIndex;
    DoublePartitionIndex nextIndex;
    public DoubleIndexNode(Expression expression, List<EqualsTo> equiJoinPreds, Map<ColumnRef, ColumnRef> columnMappings, QueryInfo query) {
        super(expression, Operator.EquiIndices);
        for (EqualsTo join : equiJoinPreds) {
            Column left = (Column) join.getLeftExpression();
            String leftName = left.getTable().getName();
            String leftColumn = left.getColumnName();
            ColumnRef leftRef = columnMappings.get(new ColumnRef(leftName, leftColumn));

            Column column = (Column) join.getRightExpression();
            String aliasName = column.getTable().getName();
            String columnName = column.getColumnName();
            ColumnRef colRef = new ColumnRef(aliasName, columnName);
            ColumnRef dbRef = columnMappings.get(colRef);
            // Check for available index
            priorIndex = (DoublePartitionIndex) BufferManager.colToIndex.get(leftRef);
            nextIndex = (DoublePartitionIndex) BufferManager.colToIndex.get(dbRef);
            leftTable = query.aliasToIndex.get(leftName);
            rightTable = query.aliasToIndex.get(aliasName);
        }
    }

    public int nextIndex(int[] tupleIndices, int[] nextSize) {
        int priorTuple = tupleIndices[leftTable];
        double priorVal = priorIndex.doubleData.data[priorTuple];
        int curTuple = tupleIndices[rightTable];
        return nextIndex.nextTuple(priorVal, curTuple, nextSize);
    }

    public boolean curIndex(int[] tupleIndices) {
        int priorTuple = tupleIndices[leftTable];
        double priorVal = priorIndex.doubleData.data[priorTuple];
        int curTuple = tupleIndices[rightTable];
        return priorVal == nextIndex.doubleData.data[curTuple];
    }
}
