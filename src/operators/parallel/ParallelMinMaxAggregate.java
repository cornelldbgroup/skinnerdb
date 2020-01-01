package operators.parallel;

import buffer.BufferManager;
import catalog.CatalogManager;
import catalog.info.ColumnInfo;
import catalog.info.TableInfo;
import data.ColumnData;
import data.IntData;
import operators.OperatorUtils;
import operators.RowRange;
import predicate.Operator;
import query.ColumnRef;
import types.SQLtype;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Calculates minimum or maximum values (per group
 * or overall) from a given input data column. 
 * 
 * @author immanueltrummer
 *
 */
public class ParallelMinMaxAggregate {
	/**
	 * Calculates aggregate from source data for each group
	 * (or total if no groups are specified) and stores
	 * result in given target column.
	 * 
	 * @param sourceRef		reference to source column
	 * @param nrGroups		number of groups
	 * @param groupData		assigns source rows to group IDs
	 * @param isMax			whether to calculate maximum (otherwise: minimum)
	 * @param targetRef		store results in this column
	 * @throws Exception
	 */
	public static void execute(ColumnRef sourceRef, int nrGroups,
			ColumnRef groupRef, boolean isMax, ColumnRef targetRef) 
					throws Exception {
		// Get information about source column
		String srcRel = sourceRef.aliasName;
		SQLtype srcType = CatalogManager.getColumn(sourceRef).type;
		int cardinality = CatalogManager.getCardinality(srcRel);
		ColumnData srcData = BufferManager.getData(sourceRef);
		// Will store (ordered) indices of result rows
		List<Integer> resultRows = new ArrayList<>();
		// Do we aggregate per group or overall?
		if (groupRef==null) {
			// No grouping
			int resultRow = -1;
			List<RowRange> batches = OperatorUtils.split(cardinality);
			int nrBatches = batches.size();
			int[] batchRows = new int[nrBatches];
			IntStream.range(0, nrBatches).parallel().forEach(bid -> {
				RowRange batch = batches.get(bid);
				int batchRow = -1;
				for (int row = batch.firstTuple; row <= batch.lastTuple; ++row) {
					if (!srcData.isNull.get(row)) {
						// Is this the first row?
						if (batchRow==-1) {
							batchRow = row;
						} else {
							int cmp = srcData.compareRows(batchRow, row);
							if (cmp==-1 && isMax || cmp==1 && !isMax) {
								batchRow = row;
							}
						}
					}
				}
				batchRows[bid] = batchRow;
			});
			for (int bid = 0; bid < nrBatches; bid++) {
				int row = batchRows[bid];
				if (batchRows[bid] != -1) {
					// Is this the first row?
					if (resultRow==-1) {
						resultRow = row;
					} else {
						int cmp = srcData.compareRows(resultRow, row);
						if (cmp==-1 && isMax || cmp==1 && !isMax) {
							resultRow = row;
						}
					}
				}
			}
			resultRows.add(resultRow);
		} else {
			// Aggregate by group
			// Each group will be associated with row index
			// Iterate over source rows and update groups
			int[] groups = ((IntData)BufferManager.getData(groupRef)).data;
			List<RowRange> batches = OperatorUtils.split(cardinality);
			int nrBatches = batches.size();
			int[][] batchRows = new int[nrGroups][nrBatches];
			IntStream.range(0, nrBatches).parallel().forEach(bid -> {
				RowRange batch = batches.get(bid);
				for (int gid = 0; gid < nrGroups; gid++) {
					batchRows[gid][bid] = -1;
				}
				for (int row = batch.firstTuple; row <= batch.lastTuple; ++row) {
					if (!srcData.isNull.get(row)) {
						int group = groups[row];
						if (batchRows[group][bid]==-1) {
							// First considered row for group
							batchRows[group][bid] = row;
						} else {
							// Need to compare against prior minimum/maximum
							int priorRow = batchRows[group][bid];
							// Need to replace prior min/max?
							int cmp = srcData.compareRows(priorRow, row);
							if (cmp<0 && isMax || cmp>0 && !isMax) {
								batchRows[group][bid] = row;
							}
						}
					}
				}
			});
			int[] rowForGroup = new int[nrGroups];
			IntStream.range(0, nrGroups).parallel().forEach(gid -> {
				for (int bid = 0; bid < nrBatches; bid++) {
					int row = batchRows[gid][bid];
					if (row != -1) {
						if (rowForGroup[gid] == -1) {
							// First considered row for group
							rowForGroup[gid] = row;
						} else {
							// Need to compare against prior minimum/maximum
							int priorRow = rowForGroup[gid];
							// Need to replace prior min/max?
							int cmp = srcData.compareRows(priorRow, row);
							if (cmp<0 && isMax || cmp>0 && !isMax) {
								rowForGroup[gid] = row;
							}
						}
					}
				}
			});

			// Collect result rows
			for (int group=0; group<nrGroups; ++group) {
				int row = rowForGroup[group];
				resultRows.add(row);
			}
		}
		// Materialize aggregation result
		ColumnData targetData = srcData.copyRows(resultRows);
		BufferManager.colToData.put(targetRef, targetData);
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
	}
}
