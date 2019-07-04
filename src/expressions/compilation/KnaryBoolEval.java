package expressions.compilation;

/**
 * Interface for evaluating Boolean expressions
 * on multiple tables.
 * 
 * @author immanueltrummer
 *
 */
public interface KnaryBoolEval {
	/**
	 * Given tuple indices for each base table,
	 * evaluate Boolean expression on this
	 * tuple combination.
	 * 
	 * @param tupleIndices	vector of tuple indices
	 * @return				1 if true, 0 for NULL, -1 for false
	 */
	public byte evaluate(int[] tupleIndices);
}
