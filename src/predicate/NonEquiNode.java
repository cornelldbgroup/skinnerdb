package predicate;

import indexing.Index;
import joining.parallel.indexing.DoublePartitionIndex;
import joining.parallel.indexing.IntPartitionIndex;
import joining.parallel.indexing.PartitionIndex;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;


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
        else if (operator == Operator.NotExist) {
            boolean evaluation = !left.exist(tupleIndices);
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

    public boolean notExist(int[] tupleIndices) {
        // TODO
        if (operator == Operator.OR) {
            return false;
        }
        else if (operator == Operator.AND) {
            return !left.exist(tupleIndices) || !right.exist(tupleIndices);
        }
        else if (operator == Operator.EqualsTo) {

        }
        else if (operator == Operator.NotEqualsTo) {

        }
        return false;
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


}
