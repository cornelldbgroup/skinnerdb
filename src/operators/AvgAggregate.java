package operators;

import buffer.BufferManager;
import catalog.CatalogManager;
import catalog.info.ColumnInfo;
import catalog.info.TableInfo;
import com.google.common.util.concurrent.AtomicDoubleArray;
import data.ColumnData;
import data.DoubleData;
import data.IntData;
import data.LongData;
import query.ColumnRef;
import types.SQLtype;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.stream.IntStream;

/**
 * Calculates the average (total or per group)
 * from a given input column.
 * 
 * @author Anonymous
 *
 */
public class AvgAggregate {
	/**
	 * Calculates aggregate from source data for each group
	 * (or total if no groups are specified) and stores
	 * result in given target column.
	 * 
	 * @param sourceRef		reference to source column
	 * @param nrGroups		number of groups
	 * @param groupRef		assigns source rows to group IDs
	 * @param targetRef		store results in this column
	 * @throws Exception
	 */
	public static void execute(ColumnRef sourceRef, int nrGroups,
			ColumnRef groupRef, ColumnRef targetRef) throws Exception {
		// Get information about source column
		String srcRel = sourceRef.aliasName;
		SQLtype srcType = CatalogManager.getColumn(sourceRef).type;
		int srcCard = CatalogManager.getCardinality(srcRel);
		ColumnData srcData = BufferManager.getData(sourceRef);
		// Create row to group assignments
		boolean grouping = groupRef != null;
		int[] groups = grouping?((IntData)
				BufferManager.getData(groupRef)).data:
					new int[srcCard];
		// Generate target column
		int targetCard = grouping ? nrGroups:1;
		ColumnData genericTarget = null;
		IntData intTarget = null;
		LongData longTarget = null;
		DoubleData doubleTarget = null;
		switch (srcType) {
		case INT:
			intTarget = new IntData(targetCard);
			genericTarget = intTarget;
			BufferManager.colToData.put(targetRef, intTarget);
			break;
		case LONG:
			longTarget = new LongData(targetCard);
			genericTarget = longTarget;
			BufferManager.colToData.put(targetRef, longTarget);
			break;
		case DOUBLE:
			doubleTarget = new DoubleData(targetCard);
			genericTarget = doubleTarget;
			BufferManager.colToData.put(targetRef, doubleTarget);
			break;
		default:
			throw new Exception("Error - no sum over " + 
					srcType + " allowed");
		}
		// Register target column in catalog
		String targetRel = targetRef.aliasName;
		String targetCol = targetRef.columnName;
		TableInfo targetRelInfo = CatalogManager.
				currentDB.nameToTable.get(targetRel);
		ColumnInfo targetColInfo = new ColumnInfo(targetCol, 
				srcType, false, false, false, false);
		targetRelInfo.addColumn(targetColInfo);
		// Update catalog statistics on result table
		CatalogManager.updateStats(targetRel);
		// Set target values to null
		for (int row=0; row<targetCard; ++row) {
			genericTarget.isNull.set(row);
		}
		int[] numbers = new int[targetCard];
		// Switch according to column type (to avoid casts)
		switch (srcType) {
		case INT:
			IntData intSrc = (IntData)srcData;
			// Iterate over input column
			for (int row=0; row<srcCard; ++row) {
				// Check for null values
				if (!srcData.isNull.get(row)) {
					int group = groups[row];
					genericTarget.isNull.set(group, false);
					intTarget.data[group] += intSrc.data[row];
					numbers[group]++;
				}
			}
			IntData finalIntTarget = intTarget;
			IntStream.range(0, targetCard).forEach(i -> finalIntTarget.data[i] /= numbers[i]);

			break;
		case LONG:
			LongData longSrc = (LongData)srcData;
			// Iterate over input column
			for (int row=0; row<srcCard; ++row) {
				// Check for null values
				if (!srcData.isNull.get(row)) {
					int group = groups[row];
					genericTarget.isNull.set(group, false);
					longTarget.data[group] += longSrc.data[row];
					numbers[group]++;
				}
			}
			LongData finalLongTarget = longTarget;
			IntStream.range(0, targetCard).forEach(i -> finalLongTarget.data[i] /= numbers[i]);
			break;
		case DOUBLE:
			DoubleData doubleSrc = (DoubleData)srcData;
			// Iterate over input column
			for (int row=0; row<srcCard; ++row) {
				// Check for null values
				if (!srcData.isNull.get(row)) {
					int group = groups[row];
					genericTarget.isNull.set(group, false);
					doubleTarget.data[group] += doubleSrc.data[row];
					numbers[group]++;
				}
			}
			DoubleData finalDoubleTarget = doubleTarget;
			IntStream.range(0, targetCard).forEach(i -> finalDoubleTarget.data[i] /= numbers[i]);
			break;
		default:
			throw new Exception("Unsupported type: " + srcType);
		}
	}

	public static void parallelExecute(ColumnRef sourceRef, int nrGroups,
							   ColumnRef groupRef, ColumnRef targetRef) throws Exception {
		// Get information about source column
		String srcRel = sourceRef.aliasName;
		SQLtype srcType = CatalogManager.getColumn(sourceRef).type;
		int srcCard = CatalogManager.getCardinality(srcRel);
		ColumnData srcData = BufferManager.getData(sourceRef);
		// Create row to group assignments
		boolean grouping = groupRef != null;
		// Generate target column
		int targetCard = grouping ? nrGroups:1;
		ColumnData genericTarget = null;
		IntData intTarget = null;
		DoubleData doubleTarget = null;
		switch (srcType) {
			case INT:
				intTarget = new IntData(targetCard);
				genericTarget = intTarget;
				BufferManager.colToData.put(targetRef, intTarget);
				break;
			case DOUBLE:
				doubleTarget = new DoubleData(targetCard);
				genericTarget = doubleTarget;
				BufferManager.colToData.put(targetRef, doubleTarget);
				break;
			default:
				throw new Exception("Error - no sum over " +
						srcType + " allowed");
		}
		// Register target column in catalog
		String targetRel = targetRef.aliasName;
		String targetCol = targetRef.columnName;
		TableInfo targetRelInfo = CatalogManager.
				currentDB.nameToTable.get(targetRel);
		ColumnInfo targetColInfo = new ColumnInfo(targetCol,
				srcType, false, false, false, false);
		targetRelInfo.addColumn(targetColInfo);
		// Update catalog statistics on result table
		CatalogManager.updateStats(targetRel);
		// Initialize batches
		if (nrGroups < 100) {
			int[] groups = grouping ? ((IntData)
					BufferManager.getData(groupRef)).data:
					new int[srcCard];
			// Initialize batches
			List<RowRange> batches = MapRows.split(srcCard);
			int nrBatches = batches.size();
			// Switch according to column type (to avoid casts)
			switch (srcType) {
				case INT:
				{
					int[][] values = new int[nrBatches][targetCard];
					int[][] nums = new int[nrBatches][targetCard];
					IntData intSrc = (IntData) srcData;
					// Iterate over input column
					IntStream.range(0, nrBatches).parallel().forEach(bid -> {
						int[] localValues = values[bid];
						int[] localNums = nums[bid];
						RowRange batch = batches.get(bid);
						int first = batch.firstTuple;
						int last = batch.lastTuple;
						for (int row = first; row <= last; ++row) {
							// Check for null values
							if (!srcData.isNull.get(row)) {
								int group = groups[row];
								int value = intSrc.data[row];
								localValues[group] += value;
								localNums[group]++;
							}
						}
					});
					for (int groupCtr = 0; groupCtr < targetCard; ++groupCtr) {
						int agg = 0;
						int num = 0;
						for (int batchCtr = 0; batchCtr < nrBatches; batchCtr++) {
							agg += values[batchCtr][groupCtr];
							num += nums[batchCtr][groupCtr];
						}
						intTarget.data[groupCtr] = agg / num;
					}
					break;
				}
				case DOUBLE: {
					double[][] values = new double[nrBatches][targetCard];
					int[][] nums = new int[nrBatches][targetCard];
					DoubleData doubleSrc = (DoubleData) srcData;
					// Iterate over input column
					IntStream.range(0, nrBatches).parallel().forEach(bid -> {
						double[] localValues = values[bid];
						int[] localNums = nums[bid];
						RowRange batch = batches.get(bid);
						int first = batch.firstTuple;
						int last = batch.lastTuple;
						for (int row = first; row <= last; ++row) {
							// Check for null values
							if (!srcData.isNull.get(row)) {
								int group = groups[row];
								double value = doubleSrc.data[row];
								localValues[group] += value;
								localNums[group]++;
							}
						}
					});
					for (int groupCtr = 0; groupCtr < targetCard; ++groupCtr) {
						double agg = 0;
						int num = 0;
						for (int batchCtr = 0; batchCtr < nrBatches; batchCtr++) {
							agg += values[batchCtr][groupCtr];
							num += nums[batchCtr][groupCtr];
						}
						doubleTarget.data[groupCtr] = agg / num;
					}
					break;
				}
				default:
					throw new Exception("Unsupported type: " + srcType);
			}
		}
		else {
			int[] groups = grouping ? ((IntData)
					BufferManager.getData(groupRef)).data:
					new int[srcCard];
			// Initialize batches
			List<RowRange> batches = MapRows.split(srcCard);
			int nrBatches = batches.size();
			// Switch according to column type (to avoid casts)
			switch (srcType) {
				case INT:
				{
					AtomicIntegerArray values = new AtomicIntegerArray(targetCard);
					IntData intSrc = (IntData) srcData;
					IntData finalIntTarget = intTarget;
					// Iterate over input column
					IntStream.range(0, nrBatches).parallel().forEach(bid -> {
						RowRange batch = batches.get(bid);
						int first = batch.firstTuple;
						int last = batch.lastTuple;
						for (int row = first; row <= last; ++row) {
							// Check for null values
							if (!srcData.isNull.get(row)) {
								int group = groups[row];
								int value = intSrc.data[row];
								values.getAndAdd(group, value);
							}
						}
					});

					MapRows.split(targetCard).parallelStream().forEach(targetBatch -> {
						int first = targetBatch.firstTuple;
						int last = targetBatch.lastTuple;
						for (int groupCtr = first; groupCtr <= last; ++groupCtr) {
							finalIntTarget.data[groupCtr] = values.get(groupCtr);
						}
					});
					break;
				}
				case DOUBLE: {
					AtomicDoubleArray values = new AtomicDoubleArray(targetCard);
					DoubleData doubleSrc = (DoubleData) srcData;
					DoubleData finalDoubleTarget = doubleTarget;
					// Iterate over input column
					IntStream.range(0, nrBatches).parallel().forEach(bid -> {
						RowRange batch = batches.get(bid);
						int first = batch.firstTuple;
						int last = batch.lastTuple;
						for (int row = first; row <= last; ++row) {
							// Check for null values
							if (!srcData.isNull.get(row)) {
								int group = groups[row];
								double value = doubleSrc.data[row];
								values.getAndAdd(group, value);
							}
						}
					});

					MapRows.split(targetCard).parallelStream().forEach(targetBatch -> {
						int first = targetBatch.firstTuple;
						int last = targetBatch.lastTuple;
						for (int groupCtr = first; groupCtr <= last; ++groupCtr) {
							finalDoubleTarget.data[groupCtr] = values.get(groupCtr);
						}
					});
					break;
				}
				default:
					throw new Exception("Unsupported type: " + srcType);
			}
		}
	}
}
