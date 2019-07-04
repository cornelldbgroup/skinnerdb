package expressions.compilation;

/**
 * Generic interface for evaluating expressions - we dynamically
 * generate byte code implementing this interface for specific
 * expressions.
 * 
 * @author immanueltrummer
 *
 */
public interface ExpressionEvaluator {
	/**
	 * Uses tuple indices in the expression interface and
	 * places evaluation results there.
	 */
	void evaluate();
}
