package predicate;

import buffer.BufferManager;
import indexing.Index;
import joining.parallel.indexing.DoublePartitionIndex;
import joining.parallel.indexing.IntPartitionIndex;
import joining.parallel.indexing.PartitionIndex;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import query.ColumnRef;
import query.QueryInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The tree structure of nonEquiJoin predicates.
 * By parsing the complex predicates into tree structure,
 * the conjunction of predicates is able to be evaluated.
 *
 * @author  Anonymous
 */
public class NonEquiNode {
    /**
     * The node translated from left expression.
     */
    final NonEquiNode left;
    /**
     * The node translated from right expression.
     */
    final NonEquiNode right;
    /**
     * The index corresponding to left expression.
     */
    public final PartitionIndex leftIndex;
    /**
     * The index corresponding to right expression.
     */
    public final PartitionIndex rightIndex;
    /**
     * Whether the predicate has a constant at one side.
     */
    public final Number constant;
    /**
     * The operator of this predicate.
     * It could be AND, OR, ADDITION, ...
     */
    public final Operator operator;
    /**
     * The index of table.
     */
    public final int table;
    /**
     * The index of left table
     */
    public final int leftTable;
    /**
     * The index of right table
     */
    public final int rightTable;

    public NonEquiNode(NonEquiNode left, NonEquiNode right, Expression expression,
                       Index index, Number constant, Operator operator, int table) {
        this.left = left;
        this.right = right;
        this.leftIndex = (PartitionIndex) index;
        this.rightIndex = null;
        this.constant = constant;
        this.operator = operator;
        this.table = table;
        this.leftTable = -1;
        this.rightTable = -1;
    }

    // equi-indices
    public NonEquiNode(Operator operator, List<EqualsTo> equiJoinPreds,
                       QueryInfo query) {
        this.operator = operator;
        this.left = null;
        this.right = null;
        this.constant = null;
        this.table = -1;
        this.leftIndex = null;
        this.rightIndex = null;
        if (equiJoinPreds.size() > 0) {
            EqualsTo join = equiJoinPreds.get(0);
            Column left = (Column) join.getLeftExpression();
            String leftName = left.getTable().getName();
            Column column = (Column) join.getRightExpression();
            String aliasName = column.getTable().getName();
            int leftTable = query.aliasToIndex.get(leftName);
            int rightTable = query.aliasToIndex.get(aliasName);
            if (rightTable < leftTable) {
                this.leftTable = rightTable;
                this.rightTable = leftTable;
            }
            else {
                this.leftTable = leftTable;
                this.rightTable = rightTable;
            }

        }
        else {
            leftTable = -1;
            rightTable = -1;
        }
    }

    public NonEquiNode(NonEquiNode left, NonEquiNode right, Expression expression,
                       Index leftIndex, Index rightIndex, Operator operator,
                       int leftTable, int rightTable) {
        this.left = left;
        this.right = right;

        PartitionIndex orderedLeftIndex = (PartitionIndex) leftIndex;
        PartitionIndex orderedRightIndex = (PartitionIndex) rightIndex;

        this.operator = operator;

        if ((operator == Operator.EqualsExist
                || operator == Operator.NotEqualsAll)
                && rightTable < leftTable) {
            this.leftTable = rightTable;
            this.rightTable = leftTable;
            this.leftIndex = orderedRightIndex;
            this.rightIndex = orderedLeftIndex;
        }
        else {
            this.leftTable = leftTable;
            this.rightTable = rightTable;
            this.leftIndex = orderedLeftIndex;
            this.rightIndex = orderedRightIndex;

        }
        this.constant = null;
        this.table = -1;
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
        else if (operator == Operator.NotAll) {
            boolean evaluation = !left.evaluate(tupleIndices);
            return evaluation;
        }
        else if (operator == Operator.NotEqualsAll) {
            Number constant = leftIndex.getNumber(leftTable);
            int rightTuple = tupleIndices[rightTable];
            boolean evaluation = rightIndex.evaluate(rightTuple, constant, operator);
            tupleIndices[rightTable] = rightIndex.cardinality - 1;
            return evaluation;
        }
        else {
            if (constant != null) {
                int curTuple = tupleIndices[table];
                return leftIndex.evaluate(curTuple, constant, operator);
            }
            else {
                int leftTuple = tupleIndices[leftTable];
                int rightTuple = tupleIndices[rightTable];
                Number constant = rightIndex.getNumber(rightTuple);
                boolean evaluation = leftIndex.evaluate(leftTuple, constant, operator);
                return evaluation;
            }
        }
    }

    public boolean evaluate(int[] tupleIndices, int nextTable, int cardinality) {
        if (operator == Operator.OR) {
            return left.evaluate(tupleIndices) || right.evaluate(tupleIndices);
        }
        else if (operator == Operator.AND) {
            return left.evaluate(tupleIndices) && right.evaluate(tupleIndices);
        }
        else if (operator == Operator.Not) {
            return !left.evaluate(tupleIndices);
        }
        else if (operator == Operator.Exist) {
            NonEquiNode equals = left;
            NonEquiNode nonEquals = right;
            // special case: when it starts from 0
            if (tupleIndices[nextTable] == 0 && equals.curIndex(tupleIndices)) {
                // whether there exist a row satisfying all predicates?
                if (nonEquals.evaluate(tupleIndices, nextTable, cardinality)) {
                    tupleIndices[nextTable] = cardinality;
                    return true;
                }
            }
            // jump to the next satisfied row in the join table.
            int nextTuple = equals.nextIndex(tupleIndices, null);

            // whether there is no satisfied row?
            if (nextTuple == cardinality) {
                tupleIndices[nextTable] = cardinality;
                return false;
            }

            // evaluate nonEquiJoin predicates
            tupleIndices[nextTable] = nextTuple;
            boolean evaluation = nonEquals.evaluate(tupleIndices, nextTable, cardinality);
            // whether there exist a row satisfying all predicates?
            if (evaluation) {
                tupleIndices[nextTable] = cardinality;
                return true;
            }
            tupleIndices[nextTable] = Math.max(1, nextTuple - 1);
            return false;
        }
        else if (operator == Operator.NotExist) {
            NonEquiNode equals = left;
            NonEquiNode nonEquals = right;
            // special case: when it starts from 0
            if (tupleIndices[nextTable] == 0 && equals.curIndex(tupleIndices)) {
                // whether there exist a row satisfying all predicates?
                if (nonEquals.evaluate(tupleIndices, nextTable, cardinality)) {
                    tupleIndices[nextTable] = cardinality;
                    return false;
                }
            }
            // jump to the next satisfied row in the join table.
            int nextTuple = equals.nextIndex(tupleIndices, null);

            // whether there is no satisfied row?
            if (nextTuple == cardinality) {
                tupleIndices[nextTable] = cardinality;
                return true;
            }

            // evaluate nonEquiJoin predicates
            tupleIndices[nextTable] = nextTuple;
            boolean evaluation = nonEquals.evaluate(tupleIndices, nextTable, cardinality);
            // whether there exist a row satisfying all predicates?
            if (evaluation) {
                tupleIndices[nextTable] = cardinality;
                return false;
            }
            tupleIndices[nextTable] = Math.max(1, nextTuple - 1);
            return false;
        }
        else if (operator == Operator.EqualsExist) {
            int leftTuple = tupleIndices[leftTable];
            Number constant = leftIndex.getNumber(leftTuple);
            boolean evaluation = rightIndex.exist(constant, Operator.EqualsTo);
            tupleIndices[nextTable] = cardinality - 1;
            return evaluation;
        }
        else if (operator == Operator.NotEqualsAll) {
            int leftTuple = tupleIndices[leftTable];
            int rightTuple = tupleIndices[rightTable];
            Number constant = leftIndex.getNumber(leftTuple);
            boolean evaluation = rightIndex.evaluate(rightTuple, constant, operator);
            tupleIndices[nextTable] = cardinality;
            return evaluation;
        }
        else {
            if (constant != null) {
                int curTuple = tupleIndices[table];
                return leftIndex.evaluate(curTuple, constant, operator);
            }
            else {
                int leftTuple = tupleIndices[leftTable];
                int rightTuple = tupleIndices[rightTable];
                Number constant = rightIndex.getNumber(rightTuple);
                boolean evaluation = leftIndex.evaluate(leftTuple, constant, operator);
                return evaluation;
            }
        }
    }



    public boolean evaluateUnary(int curTuple) {
        if (operator == Operator.OR) {
            return left.evaluateUnary(curTuple) || right.evaluateUnary(curTuple);
        }
        else if (operator == Operator.AND) {
            return left.evaluateUnary(curTuple) && right.evaluateUnary(curTuple);
        }
        else {
            if (constant != null) {
                return leftIndex.evaluate(curTuple, constant, operator);
            }
        }
        return false;
    }

    public int nextIndex(int[] tupleIndices, int[] nextSize) {
        return 0;
    }

    public boolean curIndex(int[] tupleIndices) {
        return false;
    }

}
