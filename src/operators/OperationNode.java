package operators;
import net.sf.jsqlparser.expression.Expression;

/**
 * Convert math operations into tree structure.
 * When mapping is parallelized, it is required
 * to conduct some math operations on the selected
 * columns. The results can be obtained from the
 * evaluation of the operation tree.
 *
 * @author Ziyun Wei
 */
public class OperationNode {
    /**
     * The left term in the math operation.
     */
    OperationNode left;
    /**
     * The right term in the math operation.
     */
    OperationNode right;
    /**
     * The original expression that contains the math operation.
     */
    Expression expression;
    /**
     * The constant term in the math operation.
     */
    public double constant;
    /**
     * The specific operator in the math operation.
     */
    public Operator operator;

    /**
     * Initialize the operation tree.
     *
     * @param left          left term in the clause
     * @param right         right term in the clause
     * @param expression    original select expression
     * @param constant      the constant in the term
     * @param operator      math operator
     */
    public OperationNode(OperationNode left, OperationNode right, Expression expression,
                       double constant, Operator operator) {
        this.left = left;
        this.right = right;
        this.expression = expression;
        this.constant = constant;
        this.operator = operator;
    }

    /**
     *
     *
     * @param value
     * @return
     */
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
