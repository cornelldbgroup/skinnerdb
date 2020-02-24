package predicate;

import buffer.BufferManager;
import indexing.Index;
import joining.parallel.indexing.DoublePartitionIndex;
import joining.parallel.indexing.IntPartitionIndex;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import query.ColumnRef;
import query.QueryInfo;

import java.util.List;
import java.util.Map;

public class IntIndexNode extends NonEquiNode {
    final IntPartitionIndex priorIndex;
    final IntPartitionIndex nextIndex;

    public IntIndexNode(Expression expression,
                        List<EqualsTo> equiJoinPreds,
                        Map<ColumnRef, ColumnRef> columnMappings,
                        QueryInfo query) {
        super(Operator.EquiIndices, equiJoinPreds, query);
        if (equiJoinPreds.size() > 0) {
            EqualsTo join = equiJoinPreds.get(0);
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
            IntPartitionIndex priorIndex = (IntPartitionIndex) BufferManager.colToIndex.get(leftRef);
            IntPartitionIndex nextIndex = (IntPartitionIndex) BufferManager.colToIndex.get(dbRef);
            int leftTable = query.aliasToIndex.get(leftName);
            int rightTable = query.aliasToIndex.get(aliasName);
            if (rightTable < leftTable) {
                this.priorIndex = nextIndex;
                this.nextIndex = priorIndex;
            }
            else {
                this.priorIndex = priorIndex;
                this.nextIndex = nextIndex;
            }
        }
        else {
            priorIndex = null;
            nextIndex = null;
        }
    }

    public int nextIndex(int[] tupleIndices, int[] nextSize) {
        int priorTuple = tupleIndices[leftTable];
        int priorVal = priorIndex.intData.data[priorTuple];
        int curTuple = tupleIndices[rightTable];
        return nextIndex.nextTuple(priorVal, curTuple, 0, nextSize);
    }

    public boolean curIndex(int[] tupleIndices) {
        int priorTuple = tupleIndices[leftTable];
        double priorVal = priorIndex.intData.data[priorTuple];
        int curTuple = tupleIndices[rightTable];
        return priorVal == nextIndex.intData.data[curTuple];
    }
}
