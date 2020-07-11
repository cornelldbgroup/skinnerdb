package operators;

import buffer.BufferManager;
import catalog.CatalogManager;
import catalog.info.ColumnInfo;
import catalog.info.TableInfo;
import data.ColumnData;
import data.DoubleData;
import data.IntData;
import expressions.ExpressionInfo;
import indexing.Index;
import query.ColumnRef;
import types.SQLtype;

import java.util.Arrays;
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
	 * Calculates average aggregate from source data for each group
	 * (or total if no groups are specified) and stores
	 * result in given target column.
	 *
	 * @param sourceRef		reference to source column
	 * @param targetRef		store results in this column
	 * @throws Exception
	 */
	public static void executeWithIndex(ColumnRef sourceRef, Index index,
										ColumnRef targetRef, ExpressionInfo expr) throws Exception {
		// Get information about source column
		SQLtype srcType = CatalogManager.getColumn(sourceRef).type;
		ColumnData srcData = BufferManager.getData(sourceRef);
		// Generate target column
		int targetCard = index.groupForRows.length;
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
		// Check more mappings
		OperationNode evaluator = operationNode.operator == Operator.Variable ? null : operationNode;

		// Update catalog statistics on result table
		CatalogManager.updateStats(targetRel);

		// Switch according to column type (to avoid casts)
		switch (srcType) {
			case INT:
			{
				IntData intSrc = (IntData)srcData;
				int[] positions = index.positions;
				int[] gids = index.groupForRows;
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
				DoubleData doubleSrc = (DoubleData)srcData;
				int[] positions = index.positions;
				int[] gids = index.groupForRows;
				Arrays.parallelSetAll(target.data, gid -> {
					int pos = gids[gid];
					int groupCard = positions[pos];
					double value = 0;
					for (int i = pos + 1; i <= pos + groupCard; i++) {
						int rid = positions[i];
						value += doubleSrc.data[rid];
					}
					if (value != 0) {
						value = value / groupCard;
					}
					if (evaluator != null) {
						value = evaluator.evaluate(value);
					}
					return value;
				});
				break;
			}
			default:
				throw new Exception("Unsupported type: " + srcType);
		}
	}
}
