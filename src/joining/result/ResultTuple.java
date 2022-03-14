package joining.result;

import indexing.Index;

import java.util.Arrays;
import java.util.List;

/**
 * Represents one tuple of the join result.
 * 
 * @author Anonymous
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
		this.baseIndices = baseIndices.clone();
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

//	/**
//	 * Generate group keys based on group index.
//	 *
//	 * @param sourceIndexes
//	 * @return
//	 */
//	public long groupKey(List<Index> sourceIndexes, List<Integer> tableIndexes) {
//		long groupBits = 0;
//		int card = 1;
//		for (int columnCtr = 0; columnCtr < sourceIndexes.size(); columnCtr++) {
//			Index index = sourceIndexes.get(columnCtr);
//			int table = tableIndexes.get(columnCtr);
//			int row = baseIndices[table];
//			groupBits += index.groupKey(row) * card;
//			card *= index.groupIds.length;
//		}
//		return groupBits;
//	}
}
