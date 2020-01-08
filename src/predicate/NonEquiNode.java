package predicate;

import indexing.Index;
import joining.parallel.indexing.PartitionIndex;
import net.sf.jsqlparser.expression.Expression;


public class NonEquiNode {
    NonEquiNode left;
    NonEquiNode right;
    Expression expression;
    public PartitionIndex nonEquiIndex;
    public PartitionIndex leftIndex;
    public PartitionIndex rightIndex;
    public Number constant;
    public Operator operator;
    public int table;
    public int leftTable;
    public int rightTable;

    public NonEquiNode(NonEquiNode left, NonEquiNode right, Expression expression,
                       Index index, Number constant, Operator operator, int table) {
        this.left = left;
        this.right = right;
        this.expression = expression;
        this.nonEquiIndex = (PartitionIndex) index;
        this.constant = constant;
        this.operator = operator;
        this.table = table;
    }

    public NonEquiNode(NonEquiNode left, NonEquiNode right, Expression expression,
                       Index leftIndex, Index rightIndex, Operator operator,
                       int leftTable, int rightTable) {
        this.left = left;
        this.right = right;
        this.expression = expression;
        this.leftIndex = (PartitionIndex) leftIndex;
        this.rightIndex = (PartitionIndex) rightIndex;
        this.operator = operator;
        this.leftTable = leftTable;
        this.rightTable = rightTable;
    }

    public boolean evaluate(int[] tupleIndices) {
        if (operator == Operator.OR) {
            return left.evaluate(tupleIndices) || right.evaluate(tupleIndices);
        }
        else if (operator == Operator.AND) {
            return left.evaluate(tupleIndices) && right.evaluate(tupleIndices);
        }
        else if (operator == Operator.Not) {
            return !left.evaluate(tupleIndices);
        }
        else {
            if (constant != null) {
                int curTuple = tupleIndices[table];
                return nonEquiIndex.evaluate(curTuple, constant, operator);
            }
            else {
                int leftTuple = tupleIndices[leftTable];
                int rightTuple = tupleIndices[rightTable];
                Number constant = rightIndex.getNumber(rightTuple);
                boolean evaluation = leftIndex.evaluate(leftTuple, constant, operator);
                if (operator == Operator.NotEqualsAll && !evaluation) {
                    tupleIndices[rightTable] = rightIndex.cardinality - 1;
                }
                return evaluation;
            }
        }
    }
}
