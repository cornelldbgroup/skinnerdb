package expressions.compilation;

/**
 * Interface for evaluating long
 * expressions on single tables.
 * 
 * @author immanueltrummer
 *
 */
public interface UnaryLongEval {
	/**
	 * Evaluates an expression of long result type.
	 * Returns true if the result is not NULL and
	 * stores result in given array.
	 * 
	 * @param tupleIdx	evaluate on row with that index
	 * @param result	store result at position zero
	 * @return			true iff result is not null
	 */
	public boolean evaluate(int tupleIdx, long[] result);
}

