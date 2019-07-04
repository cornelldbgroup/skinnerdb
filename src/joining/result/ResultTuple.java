package joining.result;

import java.util.Arrays;

/**
 * Represents one tuple of the join result.
 * 
 * @author immanueltrummer
 *
 */
public class ResultTuple {
	/**
	 * One index for each base table (thereby
	 * characterizing the join result tuple).
	 */
	public final int[] baseIndices;
	/**
	 * Initializes result tuple for given
	 * base table indices.
	 * 
	 * @param baseIndices	tuple index for each base table
	 */
	public ResultTuple(int[] baseIndices) {
		int nrTables = baseIndices.length;
		this.baseIndices = Arrays.copyOf(baseIndices, nrTables);
	}
	@Override
	public boolean equals(Object other) {
		ResultTuple otherTuple = (ResultTuple)other;
		return Arrays.equals(baseIndices, otherTuple.baseIndices);
	}
	@Override
	public int hashCode() {
		return Arrays.hashCode(baseIndices);
	}
	@Override
	public String toString() {
		return Arrays.toString(baseIndices);
	}
}
