package data;

import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import joining.result.ResultTuple;

/**
 * Represents content of string column.
 * 
 * @author immanueltrummer
 *
 */
public class StringData extends ColumnData implements Serializable {
	/**
	 * Holds actual string data.
	 */
	public final String[] data;
	/**
	 * Initializes data array for given cardinality.
	 * 
	 * @param cardinality	number of rows
	 */
	public StringData(int cardinality) {
		super(cardinality);
		this.data = new String[cardinality];
	}
	
	@Override
	public int compareRows(int row1, int row2) {
		if (isNull.get(row1) || isNull.get(row2)) {
			return 2;
		} else {
			int cmp = data[row1].compareTo(data[row2]);
			return cmp>0?1:(cmp<0?-1:0);
		}
	}
	
	@Override
	public int hashForRow(int row) {
		return data[row].hashCode();
	}

	@Override
	public void swapRows(int row1, int row2) {
		// Swap data
		String temp = data[row1];
		data[row1] = data[row2];
		data[row2] = temp;
		// Swap NULL flags
		super.swapRows(row1, row2);
	}

	@Override
	public void store(String path) throws Exception {
		Files.createDirectories(Paths.get(path).getParent());
		FileOutputStream fileOut = new FileOutputStream(path);
		ObjectOutputStream objOut = new ObjectOutputStream(fileOut);
		objOut.writeObject(this);
		objOut.close();
		fileOut.close();
	}

	@Override
	public ColumnData copyRows(List<Integer> rowsToCopy) {
		StringData copyColumn = new StringData(rowsToCopy.size());
		int copiedRowCtr = 0;
		for (int row : rowsToCopy) {
			// Treat special case: inserted null values
			if (row==-1) {
				copyColumn.data[copiedRowCtr] = "NULL";
				copyColumn.isNull.set(copiedRowCtr);
			} else {
				copyColumn.data[copiedRowCtr] = data[row];
				copyColumn.isNull.set(copiedRowCtr, isNull.get(row));
			}
			++copiedRowCtr;
		}
		return copyColumn;
	}

	@Override
	public ColumnData copyRows(Collection<ResultTuple> tuples, int tableIdx) {
		StringData copyColumn = new StringData(tuples.size());
		int copiedRowCtr = 0;
		for (ResultTuple compositeTuple : tuples) {
			int baseTuple = compositeTuple.baseIndices[tableIdx];
			copyColumn.data[copiedRowCtr] = data[baseTuple];
			copyColumn.isNull.set(copiedRowCtr, isNull.get(baseTuple));
			++copiedRowCtr;
		}
		return copyColumn;
	}

	@Override
	public ColumnData copyRows(BitSet rowsToCopy) {
		StringData copyColumn = new StringData(rowsToCopy.cardinality());
		int copiedRowCtr = 0;
		for (int row=rowsToCopy.nextSetBit(0); row!=-1; 
				row=rowsToCopy.nextSetBit(row+1)) {
			copyColumn.data[copiedRowCtr] = data[row];
			copyColumn.isNull.set(copiedRowCtr, isNull.get(row));
			++copiedRowCtr;
		}
		return copyColumn;
	}
}
