package expressions.compilation;

/**
 * Interface for evaluating Boolean expressions
 * on unary tables.
 * 
 * @author immanueltrummer
 *
 */
public interface UnaryBoolEval {
	/**
	 * Evaluate tuple at given row, returns
	 * 1 if true, -1 if false, and 0 if the
	 * evaluation returns SQL null.
	 * 
	 * @param tupleIdx	row index for evaluation
	 * @return			-1 for false, 1 for true, 0 for null
	 */
	public byte evaluate(int tupleIdx);
}
