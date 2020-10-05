package expressions.compilation;

/**
 * Generic interface for evaluating expressions - we dynamically
 * generate byte code implementing this interface for specific
 * expressions.
 * 
 * @author Anonymous
 *
 */
public interface ExpressionEvaluator {
	/**
	 * Uses tuple indices in the expression interface and
	 * places evaluation results there.
	 */
	void evaluate();
}
