package predicate;

import indexing.Index;
import joining.parallel.indexing.PartitionIndex;
import net.sf.jsqlparser.expression.Expression;

public class OperationNode {
    OperationNode left;
    OperationNode right;
    Expression expression;
    public double constant;
    public Operator operator;

    public OperationNode(OperationNode left, OperationNode right, Expression expression,
                       double constant, Operator operator) {
        this.left = left;
        this.right = right;
        this.expression = expression;
        this.constant = constant;
        this.operator = operator;
    }

    public double evaluate(double value) {
        if (operator == Operator.Addition) {
            return left.evaluate(value) + right.evaluate(value);
        }
        else if (operator == Operator.Subtraction) {
            return left.evaluate(value) - right.evaluate(value);
        }
        else if (operator == Operator.Multiplication) {
            return left.evaluate(value) * right.evaluate(value);
        }
        else if (operator == Operator.Division) {
            return left.evaluate(value) / right.evaluate(value);
        }
        else if (operator == Operator.Constant) {
            return constant;
        }
        else {
            return value;
        }
    }
}
