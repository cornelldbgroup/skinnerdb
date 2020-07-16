package operators;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import buffer.BufferManager;
import catalog.CatalogManager;
import catalog.info.ColumnInfo;
import data.ColumnData;
import data.IntData;
import indexing.Index;
import query.ColumnRef;
import threads.ThreadPool;
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
		return nextGroupID;
	}

	public static int paraExecuteDenseGroups(Collection<ColumnRef> sourceRefs,
											 List<Index> sourceIndexes,
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

		// Split groups into batches
		List<RowRange> batches = OperatorUtils.split(cardinality, 50, 500);
		ExecutorService executorService = ThreadPool.postExecutorService;
		List<Future<Integer>> dummyFutures = new ArrayList<>();

		int nrGroupColumns = sourceCols.size();
		int nrGroups = 1;
		for (Index srcIndex : sourceIndexes) {
			nrGroups *= srcIndex.nrKeys;
		}

		// Fill result column in parallel
		for (RowRange batch: batches) {
			int startRow = batch.firstTuple;
			int endRow = batch.lastTuple;
			dummyFutures.add(executorService.submit(() -> {
				for (int rowCtr = startRow; rowCtr < endRow; ++rowCtr) {
					int groupID = 0;
					int base = 1;
					for (int indexCtr = 0; indexCtr < nrGroupColumns; indexCtr++) {
						Index index = sourceIndexes.get(indexCtr);
						int columnGroupID = index.getGroupID(rowCtr);
						groupID += base * columnGroupID;
						base *= index.nrKeys;
					}
					groupData.data[rowCtr] = groupID;
				}
				return 0;
			}));
		}
		for (int batchCtr = 0; batchCtr < batches.size(); batchCtr++) {
			Future<Integer> batchFuture = dummyFutures.get(batchCtr);
		}
		// Update catalog statistics
		CatalogManager.updateStats(targetTbl);
		return nrGroups;

	}
}
