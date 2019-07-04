package operators;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import buffer.BufferManager;
import catalog.CatalogManager;
import catalog.info.ColumnInfo;
import data.ColumnData;
import data.IntData;
import query.ColumnRef;
import types.SQLtype;

/**
 * Calculates for each row in an input table
 * a consecutive group ID, based on values
 * in a given set of columns.
 * 
 * @author immanueltrummer
 *
 */
public class GroupBy {
	/**
	 * Iterates over rows of input colunms (must have the
	 * same cardinality) and calculates consecutive
	 * group values that are stored in target column.
	 * 
	 * @param sourceRefs	source column references
	 * @param targetRef		store group ID in that column
	 * @return				number of groups
	 * @throws Exception
	 */
	public static int execute(Collection<ColumnRef> sourceRefs, 
			ColumnRef targetRef) throws Exception {
		// Register result column
		String targetTbl = targetRef.aliasName;
		String targetCol = targetRef.columnName;
		ColumnInfo targetInfo = new ColumnInfo(targetCol, 
				SQLtype.INT, false, false, false, false);
		CatalogManager.currentDB.nameToTable.get(targetTbl).addColumn(targetInfo);
		// Generate result column and load it into buffer
		String firstSourceTbl = sourceRefs.iterator().next().aliasName;
		int cardinality = CatalogManager.getCardinality(firstSourceTbl);
		IntData groupData = new IntData(cardinality);
		BufferManager.colToData.put(targetRef, groupData);
		// Get data of source columns
		List<ColumnData> sourceCols = new ArrayList<ColumnData>();
		for (ColumnRef srcRef : sourceRefs) {
			sourceCols.add(BufferManager.getData(srcRef));
		}
		// Fill result column
		Map<Group, Integer> groupToID = new HashMap<Group, Integer>();
		int nextGroupID = 0;
		for (int rowCtr=0; rowCtr<cardinality; ++rowCtr) {
			Group curGroup = new Group(rowCtr, sourceCols);
			if (!groupToID.containsKey(curGroup)) {
				groupToID.put(curGroup, nextGroupID);
				++nextGroupID;
			}
			int groupID = groupToID.get(curGroup);
			groupData.data[rowCtr] = groupID;
		}
		// Update catalog statistics
		CatalogManager.updateStats(targetTbl);
		// Retrieve data for 
		return nextGroupID;
	}
}
