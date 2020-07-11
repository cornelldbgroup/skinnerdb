package operators;

import expressions.normalization.PlainVisitor;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.schema.Column;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Test whether the select column needs to do some
 * math operations. The class is separated from the
 * ExpressionCompiler in order to avoid potential
 * concurrent overhead.
 *
 * @author Ziyun Wei
 *
 */
public class OperationTest extends PlainVisitor {
	/**
	 * Contains a tree that represents a formulation of math operations
	 */
	public final Deque<OperationNode> operationNodes =
			new ArrayDeque<>();
	/**
	 * Initialize operation test for postprocessing.
	 *
	 */
	public OperationTest() {

	}

	@Override
	public void visit(DoubleValue doubleValue) {
		// No indexes for double values currently
		operationNodes.push(new OperationNode(null, null, doubleValue,
				doubleValue.getValue(), Operator.Constant));
	}

	@Override
	public void visit(LongValue longValue) {
		// Can use index
		operationNodes.push(new OperationNode(null, null, longValue,
				(double) longValue.getValue(), Operator.Constant));
	}

	@Override
	public void visit(Addition addition) {
		addition.getLeftExpression().accept(this);
		OperationNode left = operationNodes.pop();
		addition.getRightExpression().accept(this);
		OperationNode right = operationNodes.pop();
		OperationNode node = new OperationNode(left, right, addition, 0, Operator.Addition);
		operationNodes.push(node);
	}

	@Override
	public void visit(Division division) {
		division.getLeftExpression().accept(this);
		OperationNode left = operationNodes.pop();
		division.getRightExpression().accept(this);
		OperationNode right = operationNodes.pop();
		OperationNode node = new OperationNode(left, right, division, 0, Operator.Division);
		operationNodes.push(node);
	}

	@Override
	public void visit(Multiplication multiplication) {
		multiplication.getLeftExpression().accept(this);
		OperationNode left = operationNodes.pop();
		multiplication.getRightExpression().accept(this);
		OperationNode right = operationNodes.pop();
		OperationNode node = new OperationNode(left, right, multiplication, 0, Operator.Multiplication);
		operationNodes.push(node);
	}

	@Override
	public void visit(Subtraction subtraction) {
		subtraction.getLeftExpression().accept(this);
		OperationNode left = operationNodes.pop();
		subtraction.getRightExpression().accept(this);
		OperationNode right = operationNodes.pop();
		OperationNode node = new OperationNode(left, right, subtraction, 0, Operator.Subtraction);
		operationNodes.push(node);
	}

	@Override
	public void visit(Function function) {
		operationNodes.push(new OperationNode(null, null, function, 0, Operator.Variable));
	}

	@Override
	public void visit(Column column) {
		operationNodes.push(new OperationNode(null, null, column, 0, Operator.Variable));
	}

}
