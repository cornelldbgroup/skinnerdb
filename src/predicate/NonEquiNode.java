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
 * @author  Ziyun Wei
 */
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

    // equi-indices
    public NonEquiNode(Expression expression, Operator operator) {
        this.expression = expression;
        this.operator = operator;
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
        if (operator == Operator.EqualsExist) {
            if (rightTable < leftTable) {
                int table = this.rightTable;
                this.rightTable = this.leftTable;
                this.leftTable = table;
                PartitionIndex index = this.rightIndex;
                this.rightIndex = this.leftIndex;
                this.leftIndex = index;
            }
        }
        if (operator == Operator.Variable) {

        }
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
            if (rightTable < leftTable) {
                int table = rightTable;
                rightTable = leftTable;
                leftTable = table;
                PartitionIndex index = rightIndex;
                rightIndex = leftIndex;
                leftIndex = index;
            }
            Number constant = leftIndex.getNumber(leftTable);
            int rightTuple = tupleIndices[rightTable];
            boolean evaluation = rightIndex.evaluate(rightTuple, constant, operator);
            tupleIndices[rightTable] = rightIndex.cardinality - 1;
            return evaluation;
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
            if (tupleIndices[nextTable] == 0 && curIndex(tupleIndices)) {
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
            tupleIndices[nextTable]--;
            return false;
        }
        else if (operator == Operator.NotExist) {
            NonEquiNode equals = left;
            NonEquiNode nonEquals = right;
            // special case: when it starts from 0
            if (tupleIndices[nextTable] == 0 && curIndex(tupleIndices)) {
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
            tupleIndices[nextTable]--;
            return false;
        }
        else if (operator == Operator.EqualsExist) {
            boolean evaluation = exist(tupleIndices);
            tupleIndices[nextTable] = cardinality - 1;
            return evaluation;
        }
        else if (operator == Operator.NotEqualsAll) {
            if (rightTable < leftTable) {
                int table = rightTable;
                rightTable = leftTable;
                leftTable = table;
                PartitionIndex index = rightIndex;
                rightIndex = leftIndex;
                leftIndex = index;
            }
            int leftTuple = tupleIndices[leftTable];
            int rightTuple = tupleIndices[rightTable];
            Number constant = leftIndex.getNumber(leftTuple);
            boolean evaluation = rightIndex.evaluate(rightTuple, constant, operator);
            tupleIndices[nextTable] = cardinality - 1;
            return evaluation;
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
                return evaluation;
            }
        }
    }

    public boolean exist(int[] tupleIndices) {
        if (operator == Operator.OR) {
            return left.exist(tupleIndices) || right.exist(tupleIndices);
        }
        // TODO
        else if (operator == Operator.AND) {
            if (left.operator != Operator.EqualsTo) {
                NonEquiNode node = right;
                right = left;
                left = node;
            }
            if (left.rightTable < left.leftTable) {
                int table = left.rightTable;
                left.rightTable = left.leftTable;
                left.leftTable = table;
                PartitionIndex index = left.rightIndex;
                left.rightIndex = left.leftIndex;
                left.leftIndex = index;
            }
            if (left.leftIndex instanceof IntPartitionIndex) {
                IntPartitionIndex partition = (IntPartitionIndex) left.rightIndex;
                int target = ((IntPartitionIndex) left.leftIndex).intData.data[tupleIndices[left.leftTable]];
                int pos = partition.keyToPositions.getOrDefault(target, -1);
                if (pos < 0) {
                    return false;
                }
                else {
                    int nrs = partition.positions[pos];
                    for (int i = 1; i <= nrs; i++) {
                        int index = partition.positions[i + pos];
                        tupleIndices[left.rightTable] = index;
                        if (right.evaluate(tupleIndices)) {
                            return true;
                        }
                    }
                }
            }
            else if (left.leftIndex instanceof DoublePartitionIndex) {
                DoublePartitionIndex partition = (DoublePartitionIndex) left.rightIndex;
                double target = ((DoublePartitionIndex) left.leftIndex).doubleData.data[tupleIndices[left.leftTable]];
                int pos = partition.keyToPositions.getOrDefault(target, -1);
                if (pos < 0) {
                    return false;
                }
                else {
                    int nrs = partition.positions[pos];
                    for (int i = 1; i <= nrs; i++) {
                        int index = partition.positions[i];
                        tupleIndices[left.rightTable] = index;
                        if (right.evaluate(tupleIndices)) {
                            return true;
                        }
                    }
                }
            }
        }
        else if (operator == Operator.EqualsExist) {
            int leftTuple = tupleIndices[leftTable];
            Number constant = leftIndex.getNumber(leftTuple);
            return rightIndex.exist(constant, Operator.EqualsTo);
        }
        else if (operator == Operator.EqualsTo || operator == Operator.NotEqualsTo) {
            if (rightTable < leftTable) {
                int table = rightTable;
                rightTable = leftTable;
                leftTable = table;
                PartitionIndex index = rightIndex;
                rightIndex = leftIndex;
                leftIndex = index;
            }

            int leftTuple = tupleIndices[leftTable];
            Number constant = leftIndex.getNumber(leftTuple);
            boolean evaluation = rightIndex.exist(constant, operator);
            return evaluation;
        }

        return false;
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
                return nonEquiIndex.evaluate(curTuple, constant, operator);
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
