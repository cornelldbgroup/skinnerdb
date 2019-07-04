package operators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import buffer.BufferManager;
import catalog.CatalogManager;
import catalog.info.ColumnInfo;
import catalog.info.TableInfo;
import data.ColumnData;
import data.IntData;
import query.ColumnRef;
import types.SQLtype;

/**
 * Calculates minimum or maximum values (per group
 * or overall) from a given input data column. 
 * 
 * @author immanueltrummer
 *
 */
public class MinMaxAggregate {
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
			for (int row=0; row<cardinality; ++row) {
				// Only consider non-NULL values
				if (!srcData.isNull.get(row)) {
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
			// where minimal/maximal group value is located.
			int[] rowForGroup = new int[nrGroups];
			Arrays.fill(rowForGroup, -1);
			// Iterate over source rows and update groups
			int[] groups = ((IntData)BufferManager.getData(groupRef)).data;
			for (int row=0; row<cardinality; ++row) {
				// Only consider non-NULL values
				if (!srcData.isNull.get(row)) {
					int group = groups[row];
					if (rowForGroup[group]==-1) {
						// First considered row for group
						rowForGroup[group] = row;
					} else {
						// Need to compare against prior minimum/maximum
						int priorRow = rowForGroup[group];
						// Need to replace prior min/max?
						int cmp = srcData.compareRows(priorRow, row);
						if (cmp<0 && isMax || cmp>0 && !isMax) {
							rowForGroup[group] = row;
						}
					}					
				}
			}
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
