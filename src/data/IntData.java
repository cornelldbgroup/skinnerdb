package data;

import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;

import com.koloboke.collect.set.hash.HashIntSet;
import com.koloboke.collect.set.hash.HashIntSets;
import config.ParallelConfig;
import joining.result.ResultTuple;
import operators.Filter;
import operators.RowRange;

/**
 * Represents content of integer column.
 *
 * @author immanueltrummer
 */
public class IntData extends ColumnData implements Serializable {
    /**
     * Holds integer data.
     */
    public final int[] data;

    /**
     * Initializes data array for given cardinality.
     *
     * @param cardinality number of rows
     */
    public IntData(int cardinality) {
        super(cardinality);
        this.data = new int[cardinality];
    }

    @Override
    public int compareRows(int row1, int row2) {
        if (isNull.get(row1) || isNull.get(row2)) {
            return 2;
        } else {
            return Integer.compare(data[row1], data[row2]);
        }
    }

    @Override
    public int hashForRow(int row) {
        return Integer.hashCode(data[row]);
    }

    @Override
    public void swapRows(int row1, int row2) {
        // Swap values
        int tempValue = data[row1];
        data[row1] = data[row2];
        data[row2] = tempValue;
        // Swap NULL values
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
        int cardinality = rowsToCopy.size();
        IntData copyColumn = new IntData(cardinality);
        int copiedRowCtr = 0;
        for (int row : rowsToCopy) {
            // Treat special case: insertion of null values
            if (row==-1) {
                copyColumn.data[copiedRowCtr] = 0;
                copyColumn.isNull.set(copiedRowCtr);
            } else {
                copyColumn.data[copiedRowCtr] = data[row];
                copyColumn.isNull.set(copiedRowCtr, isNull.get(row));
            }
            ++copiedRowCtr;
        }
//		if (cardinality <= ParallelConfig.PRE_BATCH_SIZE) {
//            int copiedRowCtr = 0;
//			for (int row : rowsToCopy) {
//				// Treat special case: insertion of null values
//				if (row==-1) {
//					copyColumn.data[copiedRowCtr] = 0;
//					copyColumn.isNull.set(copiedRowCtr);
//				} else {
//					copyColumn.data[copiedRowCtr] = data[row];
//					copyColumn.isNull.set(copiedRowCtr, isNull.get(row));
//				}
//				++copiedRowCtr;
//			}
//		}
//		else {
//            List<RowRange> batches = Filter.split(cardinality);
//            batches.parallelStream().map(batch -> {
//                BitSet isNull = new BitSet(cardinality);
//                // Evaluate predicate for each table row
//                for (int rowCtr = batch.firstTuple; rowCtr <= batch.lastTuple; ++rowCtr) {
//                    int row = rowsToCopy.get(rowCtr);
//                    if (row==-1) {
//                        isNull.set(rowCtr);
//                        copyColumn.data[rowCtr] = 0;
//                    } else {
//                        isNull.set(rowCtr, isNull.get(row));
//                        copyColumn.data[rowCtr] = data[row];
//                    }
//                }
//                return isNull;
//            }).forEach(copyColumn.isNull::or);
//		}
        return copyColumn;
    }

    @Override
    public ColumnData copyRows(Collection<ResultTuple> tuples, int tableIdx) {
        IntData copyColumn = new IntData(tuples.size());
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
        IntData copyColumn = new IntData(rowsToCopy.cardinality());
        int copiedRowCtr = 0;
        for (int row = rowsToCopy.nextSetBit(0); row != -1;
             row = rowsToCopy.nextSetBit(row + 1)) {
            copyColumn.data[copiedRowCtr] = data[row];
            copyColumn.isNull.set(copiedRowCtr, isNull.get(row));
            ++copiedRowCtr;
        }
        return copyColumn;
    }
}
