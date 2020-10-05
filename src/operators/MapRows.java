package operators;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import buffer.BufferManager;
import catalog.CatalogManager;
import catalog.info.ColumnInfo;
import catalog.info.TableInfo;
import config.ParallelConfig;
import data.*;
import expressions.ExpressionInfo;
import expressions.compilation.EvaluatorType;
import expressions.compilation.ExpressionCompiler;
import expressions.compilation.UnaryDoubleEval;
import expressions.compilation.UnaryIntEval;
import expressions.compilation.UnaryLongEval;
import expressions.compilation.UnaryStringEval;
import joining.parallel.indexing.IntIndexRange;
import query.ColumnRef;
import types.JavaType;
import types.SQLtype;
import types.TypeUtil;

/**
 * Evaluates a given expression on each row of a source table and
 * stores results in newly created column.
 * 
 * @author Anonymous
 *
 */
public class MapRows {
	/**
	 * Creates new column that contains one value for
	 * each row in the source relation.
	 * 
	 * @param sourceRel		we iterate over rows of this source relation
	 * @param expression	each source row is mapped using this expression
	 * @param columnMapping	maps query to database columns
	 * @param aggMapping	maps SQL aggregate expressions to columns
	 * @param groupRef		optionally assign each row to a group -
	 * 						result contains one row per group if specified.
	 * @param nrGroups		specifies the number of groups (not used if
	 * 						no group reference is specified).
	 * @param targetRef		store results in this target column
	 * @throws Exception
	 */
	public static void execute(String sourceRel, ExpressionInfo expression, 
			Map<ColumnRef, ColumnRef> columnMapping, Map<String, ColumnRef> aggMapping, 
			ColumnRef groupRef, int nrGroups, ColumnRef targetRef) throws Exception {
		// Do we map to groups?
		boolean groupBy = groupRef!=null;
		// Register target column in catalog
		SQLtype resultType = expression.resultType;
		JavaType jResultType = TypeUtil.toJavaType(resultType);
		String targetTable = targetRef.aliasName;
		String targetCol = targetRef.columnName;
		TableInfo targetTblInf = CatalogManager.
				currentDB.nameToTable.get(targetTable);
		ColumnInfo targetColInf = new ColumnInfo(targetCol, 
				resultType, false, false, false, false);
		targetTblInf.addColumn(targetColInf);
		// Prepare generating result data
		int inCard = CatalogManager.getCardinality(sourceRel);
		int outCard = groupBy?nrGroups:inCard;
		IntData groupData = groupBy?
				(IntData)BufferManager.getData(groupRef):
					null;
		// Create result data and load into buffer
		switch (jResultType) {
		case INT:
		{
			// Compile mapping expression
			ExpressionCompiler unaryCompiler = new ExpressionCompiler(
					expression, columnMapping, null, aggMapping, 
					EvaluatorType.UNARY_INT);
			expression.finalExpression.accept(unaryCompiler);
			UnaryIntEval unaryIntEval = unaryCompiler.getUnaryIntEval();
			// Generate result data and store in buffer
			IntData intResult = new IntData(outCard);
			if (groupBy && outCard<0) {
				intResult.isNull.set(0, outCard-1);
			}
			BufferManager.colToData.put(targetRef, intResult);
			// Iterate over source table and store results
			int[] rowResult = new int[1];
			for (int srcRow=0; srcRow<inCard; ++srcRow) {
				// Either map row to row or row to group
				int targetRow = !groupBy?srcRow:groupData.data[srcRow];
				boolean notNull = unaryIntEval.evaluate(srcRow, rowResult);
				if (!groupBy || notNull) {
					intResult.isNull.set(targetRow, !notNull);
					intResult.data[targetRow] = rowResult[0];					
				}
			}			
		}
			break;
		case LONG:
		{
			// Compile mapping expression
			ExpressionCompiler unaryCompiler = new ExpressionCompiler(
					expression, columnMapping, null, aggMapping, 
					EvaluatorType.UNARY_LONG);
			expression.finalExpression.accept(unaryCompiler);
			UnaryLongEval unaryLongEval = unaryCompiler.getUnaryLongEval();
			// Generate result data and store in buffer
			LongData longResult = new LongData(outCard);
			if (groupBy && outCard<0) {
				longResult.isNull.set(0, outCard-1);
			}
			BufferManager.colToData.put(targetRef, longResult);
			// Iterate over source table and store results
			long[] rowResult = new long[1];
			for (int srcRow=0; srcRow<inCard; ++srcRow) {
				// Either map row to row or row to group
				int targetRow = !groupBy?srcRow:groupData.data[srcRow];
				boolean notNull = unaryLongEval.evaluate(srcRow, rowResult);
				if (!groupBy || notNull) {
					longResult.isNull.set(targetRow, !notNull);
					longResult.data[targetRow] = rowResult[0];					
				}
			}			
		}
			break;
		case DOUBLE:
		{
			// Compile mapping expression
			ExpressionCompiler unaryCompiler = new ExpressionCompiler(
					expression, columnMapping, null, aggMapping, 
					EvaluatorType.UNARY_DOUBLE);
			expression.finalExpression.accept(unaryCompiler);
			UnaryDoubleEval unaryDoubleEval = unaryCompiler.getUnaryDoubleEval();
			// Generate result data and store in buffer
			DoubleData doubleResult = new DoubleData(outCard);
			if (groupBy && outCard<0) {
				doubleResult.isNull.set(0, outCard-1);
			}
			BufferManager.colToData.put(targetRef, doubleResult);
			// Iterate over source table and store results
			double[] rowResult = new double[1];
			for (int srcRow=0; srcRow<inCard; ++srcRow) {
				// Either map row to row or row to group
				int targetRow = !groupBy?srcRow:groupData.data[srcRow];
				boolean notNull = unaryDoubleEval.evaluate(srcRow, rowResult);
				if (!groupBy || notNull) {
					doubleResult.isNull.set(targetRow, !notNull);
					doubleResult.data[targetRow] = rowResult[0];					
				}
			}			
		}
			break;
		case STRING:
		{
			// Compile mapping expression
			ExpressionCompiler unaryCompiler = new ExpressionCompiler(
					expression, columnMapping, null, aggMapping,
					EvaluatorType.UNARY_STRING);
			expression.finalExpression.accept(unaryCompiler); 
			UnaryStringEval unaryStringEval = unaryCompiler.getUnaryStringEval();
			// Generate result data and store in buffer
			StringData stringResult = new StringData(outCard);
			if (groupBy && outCard<0) {
				stringResult.isNull.set(0, outCard-1);
			}
			BufferManager.colToData.put(targetRef, stringResult);
			// Iterate over source table and store results
			String[] rowResult = new String[1];
			for (int srcRow=0; srcRow<inCard; ++srcRow) {
				// Either map row to row or row to group
				int targetRow = !groupBy?srcRow:groupData.data[srcRow];
				boolean notNull = unaryStringEval.evaluate(srcRow, rowResult);
				if (!groupBy || notNull) {
					stringResult.isNull.set(targetRow, !notNull);
					stringResult.data[targetRow] = rowResult[0];					
				}
			}
		}
			break;
		}
		// Update catalog statistics
		CatalogManager.updateStats(targetTable);
	}

	public static void parallelExecute(String sourceRel, ExpressionInfo expression,
							   Map<ColumnRef, ColumnRef> columnMapping, Map<String, ColumnRef> aggMapping,
							   ColumnRef groupRef, int nrGroups, ColumnRef targetRef) throws Exception {
		// Do we map to groups?
		boolean groupBy = groupRef!=null;
		// Register target column in catalog
		SQLtype resultType = expression.resultType;
		JavaType jResultType = TypeUtil.toJavaType(resultType);
		String targetTable = targetRef.aliasName;
		String targetCol = targetRef.columnName;
		TableInfo targetTblInf = CatalogManager.
				currentDB.nameToTable.get(targetTable);
		ColumnInfo targetColInf = new ColumnInfo(targetCol,
				resultType, false, false, false, false);
		targetTblInf.addColumn(targetColInf);
		// Prepare generating result data
		int inCard = CatalogManager.getCardinality(sourceRel);
		int outCard = groupBy?nrGroups:inCard;
		IntData groupData = groupBy?
				(IntData)BufferManager.getData(groupRef):
				null;
		List<RowRange> batches = split(inCard);
		// Create result data and load into buffer
		switch (jResultType) {
			case INT:
			{
				// Compile mapping expression
				ExpressionCompiler unaryCompiler = new ExpressionCompiler(
						expression, columnMapping, null, aggMapping,
						EvaluatorType.UNARY_INT);
				expression.finalExpression.accept(unaryCompiler);
				UnaryIntEval unaryIntEval = unaryCompiler.getUnaryIntEval();
				// Generate result data and store in buffer
				IntData intResult = new IntData(outCard);
				BufferManager.colToData.put(targetRef, intResult);
				// Iterate over source table and store results
				batches.parallelStream().forEach(batch -> {
					int[] rowResult = new int[1];
					int first = batch.firstTuple;
					int last = batch.lastTuple;
					for (int srcRow = first; srcRow <= last; ++srcRow) {
						// Either map row to row or row to group
						int targetRow = !groupBy ? srcRow : groupData.data[srcRow];
						boolean notNull = unaryIntEval.evaluate(srcRow, rowResult);
						if (!groupBy || notNull) {
							intResult.data[targetRow] = rowResult[0];
						}
					}
				});
			}
			break;
			case DOUBLE:
			{
				// Compile mapping expression
				ExpressionCompiler unaryCompiler = new ExpressionCompiler(
						expression, columnMapping, null, aggMapping,
						EvaluatorType.UNARY_DOUBLE);
				expression.finalExpression.accept(unaryCompiler);
				UnaryDoubleEval unaryDoubleEval = unaryCompiler.getUnaryDoubleEval();
				// Generate result data and store in buffer
				DoubleData doubleResult = new DoubleData(outCard);
				BufferManager.colToData.put(targetRef, doubleResult);
				// Iterate over source table and store results
				batches.parallelStream().forEach(batch -> {
					double[] rowResult = new double[1];
					int first = batch.firstTuple;
					int last = batch.lastTuple;
					for (int srcRow = first; srcRow <= last; ++srcRow) {
						// Either map row to row or row to group
						int targetRow = !groupBy ? srcRow : groupData.data[srcRow];
						boolean notNull = unaryDoubleEval.evaluate(srcRow, rowResult);
						if (!groupBy || notNull) {
							doubleResult.data[targetRow] = rowResult[0];
						}
					}
				});
			}
			break;
		}
		// Update catalog statistics
		CatalogManager.updateStats(targetTable);
	}

	/**
	 * Splits table with given cardinality into tuple batches
	 * according to the configuration for joining.parallel processing.
	 *
	 * @param cardinality	cardinality of table to split
	 * @return				list of row ranges (batches)
	 */
	public static List<RowRange> split(int cardinality) {
		List<RowRange> batches = new ArrayList<RowRange>();
		int batchSize = Math.max(ParallelConfig.PRE_INDEX_SIZE, cardinality / 600);
		for (int batchCtr=0; batchCtr * batchSize < cardinality;
			 ++batchCtr) {
			int startIdx = batchCtr * batchSize;
			int tentativeEndIdx = startIdx + batchSize - 1;
			int endIdx = Math.min(cardinality - 1, tentativeEndIdx);
			RowRange rowRange = new RowRange(startIdx, endIdx);
			batches.add(rowRange);
		}
		return batches;
	}

	/**
	 * Splits table with given cardinality into tuple batches
	 * according to the configuration for joining.parallel processing.
	 *
	 * @param cardinality	cardinality of table to split
	 * @return				list of row ranges (batches)
	 */
	public static List<RowRange> sortedSplit(int cardinality, ColumnData data) {
		List<RowRange> batches = new ArrayList<>();
		int batchSize = Math.max(ParallelConfig.PRE_INDEX_SIZE, cardinality / 200);
		int startIdx = 0;
		int tentativeEndIdx = startIdx + batchSize - 1;
		for (int batchCtr = 0; batchCtr * batchSize < cardinality;
			 ++batchCtr) {
			int endIdx = Math.min(cardinality - 1, tentativeEndIdx);
			while (endIdx < cardinality - 1 && data.compareRows(endIdx, endIdx + 1) == 0) {
				endIdx = Math.min(cardinality - 1, endIdx + 1);
			}
			RowRange rowRange = new RowRange(startIdx, endIdx);
			batches.add(rowRange);
			startIdx = endIdx + 1;
			tentativeEndIdx = startIdx + batchSize - 1;
			if (startIdx >= cardinality) {
				break;
			}
		}
		return batches;
	}

}
