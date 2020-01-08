package operators.parallel;

import buffer.BufferManager;
import catalog.CatalogManager;
import catalog.info.ColumnInfo;
import catalog.info.TableInfo;
import com.koloboke.collect.IntCollection;
import data.ColumnData;
import data.DoubleData;
import data.IntData;
import data.LongData;
import expressions.ExpressionInfo;
import indexing.Index;
import joining.parallel.indexing.DoublePartitionIndex;
import joining.parallel.indexing.IntPartitionIndex;
import operators.Group;
import operators.OperationTest;
import operators.OperatorUtils;
import operators.RowRange;
import predicate.OperationNode;
import predicate.Operator;
import preprocessing.Context;
import query.ColumnRef;
import types.SQLtype;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

/**
 * Calculates the average (total or per group)
 * from a given input column.
 * 
 * @author Ziyun Wei
 *
 */
public class ParallelAvgAggregate {
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
		int targetCard = grouping ? nrGroups : 1;
		DoubleData target = new DoubleData(targetCard);
		BufferManager.colToData.put(targetRef, target);
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
		final int nrNewGroups = nrGroups == -1 ? 1 : nrGroups;
		switch (srcType) {
		case INT:
		{
			IntData intSrc = (IntData)srcData;
			if (grouping) {
				System.out.println("groupBy");
				groupMapping.values().parallelStream().forEach(groupIndex -> {
					int groupCard = groupIndex.getCard();
					int gid = groupIndex.gid;
					List<RowRange> batches = OperatorUtils.split(groupCard);
					int[] pair = batches.parallelStream().flatMap(batch ->
							OperatorUtils.avgBatch(intSrc, batch, groupIndex.rows).parallelStream()).reduce(
									new int[]{0, 0}, (a, b) -> new int[]{a[0] + b[0], a[1] + b[1]});
					double data = (pair[0] + 0.0) / pair[1];
					if (data != 0) {
//						genericTarget.isNull.set(gid, false);
						target.data[gid] = data;
					}
				});
			}
			else {
				System.out.println("No groupBy");
				throw new RuntimeException("Why there is no groupBy?");
			}
			break;
		}
		case LONG: {
			LongData longSrc = (LongData) srcData;
			if (grouping) {
				System.out.println("groupBy");
				groupMapping.values().parallelStream().forEach(groupIndex -> {
					int groupCard = groupIndex.getCard();
					int gid = groupIndex.gid;
					List<RowRange> batches = OperatorUtils.split(groupCard);
					long[] pair = batches.parallelStream().flatMap(batch ->
							OperatorUtils.avgBatch(longSrc, batch, groupIndex.rows).parallelStream()).reduce(
							new long[]{0, 0}, (a, b) -> new long[]{a[0] + b[0], a[1] + b[1]});
					double data = (pair[0] + 0.0) / pair[1];
					if (data != 0) {
//						genericTarget.isNull.set(gid, false);
						target.data[gid] = data;
					}
				});
			}
			else {
				System.out.println("No groupBy");
				throw new RuntimeException("Why there is no groupBy?");
			}
			break;
		}
		case DOUBLE: {
			DoubleData doubleSrc = (DoubleData) srcData;
			if (grouping) {
				System.out.println("groupBy");
				groupMapping.values().parallelStream().forEach(groupIndex -> {
					int groupCard = groupIndex.getCard();
					int gid = groupIndex.gid;
					List<RowRange> batches = OperatorUtils.split(groupCard);
					double[] pair = batches.parallelStream().flatMap(batch ->
							OperatorUtils.avgBatch(doubleSrc, batch, groupIndex.rows).parallelStream()).reduce(
							new double[]{0.0, 0.0}, (a, b) -> new double[]{a[0] + b[0], a[1] + b[1]});
					double data = pair[0] / pair[1];
					if (data != 0) {
//						genericTarget.isNull.set(gid, false);
						target.data[gid] = data;
					}
				});
			}
			else {
				System.out.println("No groupBy");
				throw new RuntimeException("Why there is no groupBy?");
			}
			break;
		}
		default:
			throw new Exception("Unsupported type: " + srcType);
		}
	}

	/**
	 * Calculates aggregate from source data for each group
	 * (or total if no groups are specified) and stores
	 * result in given target column.
	 *
	 * @param sourceRef		reference to source column
	 * @param targetRef		store results in this column
	 * @throws Exception
	 */
	public static void executeIndex(ColumnRef sourceRef,
									Index index,
									ColumnRef targetRef, ExpressionInfo expr) throws Exception {
		// Get information about source column
		SQLtype srcType = CatalogManager.getColumn(sourceRef).type;
		ColumnData srcData = BufferManager.getData(sourceRef);
		// Create row to group assignments
		// Generate target column
		int targetCard = index.groupIds.length;
		DoubleData target = new DoubleData(targetCard);
		BufferManager.colToData.put(targetRef, target);
		// Register target column in catalog
		String targetRel = targetRef.aliasName;
		String targetCol = targetRef.columnName;
		TableInfo targetRelInfo = CatalogManager.
				currentDB.nameToTable.get(targetRel);
		ColumnInfo targetColInfo = new ColumnInfo(targetCol,
				srcType, false, false, false, false);
		targetRelInfo.addColumn(targetColInfo);
		OperationTest operationTest = new OperationTest();
		expr.finalExpression.accept(operationTest);
		OperationNode operationNode = operationTest.operationNodes.pop();
		// check more mappings
		OperationNode evaluator = operationNode.operator == Operator.Variable ? null : operationNode;

		// Update catalog statistics on result table
		CatalogManager.updateStats(targetRel);


		// Switch according to column type (to avoid casts)
		switch (srcType) {
			case INT:
			{
				IntData intSrc = (IntData)srcData;
				System.out.println("groupBy");
				int[] positions = index.positions;
				int[] gids = index.groupIds;
				IntStream.range(0, gids.length).parallel().forEach(gid -> {
					int pos = gids[gid];
					int groupCard = positions[pos];
					double data = 0;
					for (int i = pos + 1; i <= pos + groupCard; i++) {
						int rid = positions[i];
						data += intSrc.data[rid];
					}
					if (data != 0) {
						data = data / groupCard;
					}
					if (evaluator != null) {
						data = evaluator.evaluate(data);
					}
					target.data[gid] = data;
				});
				break;
			}
			case DOUBLE: {
				DoubleData intSrc = (DoubleData)srcData;
				System.out.println("groupBy");
				int[] positions = index.positions;
				int[] gids = index.groupIds;
				IntStream.range(0, gids.length).parallel().forEach(gid -> {
					int pos = gids[gid];
					int groupCard = positions[pos];
					double data = 0;
					for (int i = pos + 1; i <= pos + groupCard; i++) {
						int rid = positions[i];
						data += intSrc.data[rid];
					}
					if (data != 0) {
						data = data / groupCard;
					}
					if (evaluator != null) {
						data = evaluator.evaluate(data);
					}
					target.data[gid] = data;
				});
				break;
			}
			default:
				throw new Exception("Unsupported type: " + srcType);
		}
	}
}
