package expressions.compilation;

/**
 * Interface for evaluating string
 * expressions on single tables.
 * 
 * @author immanueltrummer
 *
 */
public interface UnaryStringEval {
	/**
	 * Evaluates an expression of string result type.
	 * Returns true if the result is not NULL and
	 * stores result in given array.
	 * 
	 * @param tupleIdx	evaluate on row with that index
	 * @param result	store result at position zero
	 * @return			true iff result is not null
	 */
	public boolean evaluate(int tupleIdx, String[] result);
}
