package operators.parallel;

import buffer.BufferManager;
import catalog.CatalogManager;
import catalog.info.ColumnInfo;
import catalog.info.TableInfo;
import com.google.common.util.concurrent.AtomicDouble;
import data.*;
import operators.Group;
import operators.OperatorUtils;
import operators.RowRange;
import query.ColumnRef;
import types.SQLtype;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Calculates the sum (total or per group)
 * from a given input column.
 * 
 * @author Ziyun Wei
 *
 */
public class ParallelSumAggregate {
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
	public static void execute(ColumnRef sourceRef,
							   int nrGroups,
							   Map<Group, GroupIndex> groupMapping,
							   ColumnRef groupRef,
							   ColumnRef targetRef) throws Exception {
		// Get information about source column
		SQLtype srcType = CatalogManager.getColumn(sourceRef).type;
		ColumnData srcData = BufferManager.getData(sourceRef);
		// Create row to group assignments
		boolean grouping = groupRef != null;
		// Generate target column
		int targetCard = grouping ? nrGroups:1;
		// Handle constant column
		if (srcData instanceof ConstantData) {
			ConstantData constantData = (ConstantData) srcData;
			if (grouping) {
				System.out.println("groupBy");
				IntData finalIntTarget = new IntData(targetCard);
				int constValue = (int) constantData.constant;
				BufferManager.colToData.put(targetRef, finalIntTarget);
				groupMapping.values().parallelStream().forEach(groupIndex -> {
					int gid = groupIndex.gid;
					int cardinality = groupIndex.rows.size();
					int value = constValue * cardinality;
					finalIntTarget.data[gid] = value;
				});
			}
			else {
				System.out.println("No groupBy");
				throw new RuntimeException("Why there is no groupBy?");
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
			return;
		}
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
		// Switch according to column type (to avoid casts)
		switch (srcType) {
		case INT:
		{
			IntData intSrc = (IntData)srcData;
			IntData finalIntTarget = intTarget;
			if (grouping) {
				System.out.println("groupBy");
				groupMapping.values().parallelStream().forEach(groupIndex -> {
					int groupCard = groupIndex.getCard();
					int gid = groupIndex.gid;
					List<RowRange> batches = OperatorUtils.split(groupCard);
					int data = batches.parallelStream().flatMap(batch ->
							OperatorUtils.filterBatch(intSrc, batch, groupIndex.rows).parallelStream()).reduce(0, Integer::sum);
					if (data != 0) {
//						genericTarget.isNull.set(gid, false);
						finalIntTarget.data[gid] = data;
					}
				});
			}
			else {
				System.out.println("No groupBy");
				int card = intSrc.cardinality;
				List<RowRange> batches = OperatorUtils.split(card);
				int data = batches.parallelStream().flatMap(batch ->
						OperatorUtils.filterBatch(intSrc, batch).parallelStream()).reduce(0, Integer::sum);
				if (data != 0) {
//						genericTarget.isNull.set(gid, false);
					finalIntTarget.data[0] = data;
				}
			}
			break;
		}
		case LONG: {
			LongData longSrc = (LongData)srcData;
			LongData finalLongTarget = longTarget;
			if (grouping) {
				System.out.println("groupBy");
				groupMapping.values().parallelStream().forEach(groupIndex -> {
					int groupCard = groupIndex.getCard();
					int gid = groupIndex.gid;
					List<RowRange> batches = OperatorUtils.split(groupCard);
					long data = batches.parallelStream().flatMap(batch ->
							OperatorUtils.filterBatch(longSrc, batch, groupIndex.rows).parallelStream()).reduce(0L, Long::sum);
					if (data != 0) {
//						genericTarget.isNull.set(gid, false);
						finalLongTarget.data[gid] = data;
					}
				});
			}
			else {
				System.out.println("No groupBy");
				int card = longSrc.cardinality;
				List<RowRange> batches = OperatorUtils.split(card);
				long data = batches.parallelStream().flatMap(batch ->
						OperatorUtils.filterBatch(longSrc, batch).parallelStream()).reduce(0L, Long::sum);
				if (data != 0) {
//						genericTarget.isNull.set(gid, false);
					finalLongTarget.data[0] = data;
				}
			}
			break;
		}
		case DOUBLE: {
			DoubleData doubleSrc = (DoubleData) srcData;
			DoubleData finalDoubleTarget = doubleTarget;
			if (grouping) {
				System.out.println("groupBy");
				List<RowRange> groupBatches = OperatorUtils.split(groupMapping.size());
				groupMapping.values().parallelStream().forEach(groupIndex -> {
					int groupCard = groupIndex.getCard();
					int gid = groupIndex.gid;
//					double data = groupIndex.rows.parallelStream().reduce(0.0, Integer::sum);
					List<RowRange> batches = OperatorUtils.split(groupCard);
					double data = batches.parallelStream().flatMap(batch ->
							OperatorUtils.filterBatch(doubleSrc, batch, groupIndex.rows).parallelStream()).reduce(0.0, Double::sum);
					if (data != 0) {
//						genericTarget.isNull.set(gid, false);
						finalDoubleTarget.data[gid] = data;
					}
				});
			}
			else {
				System.out.println("No groupBy");
				int card = doubleSrc.cardinality;
				List<RowRange> batches = OperatorUtils.split(card);
				double data = batches.parallelStream().flatMap(batch ->
						OperatorUtils.filterBatch(doubleSrc, batch).parallelStream()).reduce(0.0, Double::sum);
				if (data != 0) {
//						genericTarget.isNull.set(gid, false);
					finalDoubleTarget.data[0] = data;
				}
			}
			break;
		}
		default:
			throw new Exception("Unsupported type: " + srcType);
		}
	}
}
