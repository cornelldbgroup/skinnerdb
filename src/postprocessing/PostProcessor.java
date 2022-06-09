package postprocessing;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import buffer.BufferManager;
import catalog.CatalogManager;
import catalog.info.ColumnInfo;
import catalog.info.TableInfo;
import com.google.common.collect.Maps;
import com.koloboke.collect.set.IntSet;
import com.koloboke.collect.set.hash.HashIntSets;
import config.GeneralConfig;
import config.LoggingConfig;
import config.NamingConfig;
import config.ParallelConfig;
import data.ColumnData;
import data.DoubleData;
import data.IntData;
import expressions.ExpressionInfo;
import expressions.aggregates.AggInfo;
import indexing.Index;
import jni.JNIFilter;
import joining.parallel.indexing.IntPartitionIndex;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExtractExpression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import operators.*;
import operators.parallel.JNIParser;
import operators.parallel.ParallelAvgAggregate;
import operators.parallel.ParallelGroupBy;
import operators.parallel.ParallelMapRows;
import preprocessing.Context;
import print.RelationPrinter;
import query.ColumnRef;
import query.QueryInfo;
import statistics.PostStats;

import static types.SQLtype.*;

/**
 * Uses the result of the join phase as input and
 * takes care of grouping, aggregation, and
 * sorting. Produces the final result of a
 * query block.
 * 
 * @author Anonymous
 *
 */
public class PostProcessor {
	static {
		try {
			System.load(GeneralConfig.JNI_PATH);
		} catch (UnsatisfiedLinkError e) {
			System.err.println("Native code library failed to load.\n" + e);
			System.exit(1);
		}
	}
	/**
	 * Assigns each join result row to a group ID if the query
	 * specifies a group by clause. Stores results in context.
	 * 
	 * @param query			query to process
	 * @param context		query processing context
	 * @throws Exception
	 */
	static void groupBy(QueryInfo query, Context context) throws Exception {
		// Check whether query has group by clause
		if (query.groupByExpressions.isEmpty()) {
			return;
		}
		// Create table to contain groups
		String groupTbl = NamingConfig.GROUPS_TBL_NAME;
		TableInfo groupTblInfo = new TableInfo(groupTbl, true);
		CatalogManager.currentDB.nameToTable.put(groupTbl, groupTblInfo);
		// ID to assign to next group by column (only used for
		// columns whose content needs to be generated).
		int groupSrcID = 0;
		// Will contain group-by columns
		List<ColumnRef> sourceRefs = new ArrayList<>();
		for (ExpressionInfo groupExpr : query.groupByExpressions) {
			// Is it raw group by column?
			if (groupExpr.finalExpression instanceof Column) {
				// Simply add referenced column
				ColumnRef queryRef = groupExpr.columnsMentioned.iterator().next();
				ColumnRef dbRef = context.columnMapping.get(queryRef);
				sourceRefs.add(dbRef);
			} else {
				// Composite expression - need to execute map
				String sourceRel = NamingConfig.JOINED_NAME;
				String targetCol = NamingConfig.GROUPS_SRC_COL_PRE + groupSrcID;
				++groupSrcID;
				ColumnRef targetRef = new ColumnRef(groupTbl, targetCol);
				ColumnInfo targetInfo = new ColumnInfo(targetCol, 
						groupExpr.resultType, false, false, false, false);
				groupTblInfo.addColumn(targetInfo);
				MapRows.execute(sourceRel, groupExpr,
						context.columnMapping,
						null, null, -1, targetRef);
				sourceRefs.add(targetRef);
			}
		}
		// Execute group by
		String targetCol = NamingConfig.GROUPS_COL_NAME;
		ColumnRef targetRef = new ColumnRef(groupTbl, targetCol);
		// Update query context for following steps
		context.groupRef = targetRef;
		long timer1 = System.currentTimeMillis();
//		context.nrGroups = GroupBy.execute(sourceRefs, targetRef);
//		context.nrGroups = ParallelGroupBy.execute(sourceRefs, targetRef, query);
		context.nrGroups = ParallelGroupBy.executeJNI(sourceRefs, targetRef, query);
//		context.nrGroups = ParallelGroupBy.executeByIndex(sourceRefs, targetRef, query);
//		context.nrGroups = ParallelGroupBy.executeBySimpleIndex(sourceRefs, targetRef, query, context);
		long timer2 = System.currentTimeMillis();
		System.out.println("Group: " + (timer2 - timer1));
		// TODO: need to replace references to columns in GROUP BY clause
	}

	static void groupBySingleIndex(QueryInfo query, Context context, Index index) throws Exception {
		// Check whether query has group by clause
		if (query.groupByExpressions.isEmpty()) {
			return;
		}
		// Create table to contain groups
		String groupTbl = NamingConfig.GROUPS_TBL_NAME;
		TableInfo groupTblInfo = new TableInfo(groupTbl, true);
		CatalogManager.currentDB.nameToTable.put(groupTbl, groupTblInfo);
		// ID to assign to next group by column (only used for
		// columns whose content needs to be generated).
		int groupSrcID = 0;
		// Will contain group-by columns
		List<ColumnRef> sourceRefs = new ArrayList<>();
		for (ExpressionInfo groupExpr : query.groupByExpressions) {
			// Is it raw group by column?
			if (groupExpr.finalExpression instanceof Column) {
				// Simply add referenced column
				ColumnRef queryRef = groupExpr.columnsMentioned.iterator().next();
				ColumnRef dbRef = context.columnMapping.get(queryRef);
				sourceRefs.add(dbRef);
			} else {
				// Composite expression - need to execute map
				String sourceRel = NamingConfig.JOINED_NAME;
				String targetCol = NamingConfig.GROUPS_SRC_COL_PRE + groupSrcID;
				++groupSrcID;
				ColumnRef targetRef = new ColumnRef(groupTbl, targetCol);
				ColumnInfo targetInfo = new ColumnInfo(targetCol,
						groupExpr.resultType, false, false, false, false);
				groupTblInfo.addColumn(targetInfo);
				MapRows.parallelExecute(sourceRel, groupExpr,
						context.columnMapping,
						null, null, -1, targetRef);
				sourceRefs.add(targetRef);
			}
		}
		// Execute group by
		String targetCol = NamingConfig.GROUPS_COL_NAME;
		ColumnRef targetRef = new ColumnRef(groupTbl, targetCol);
		// Update query context for following steps
		context.groupRef = targetRef;
		long timer1 = System.currentTimeMillis();
		context.nrGroups = ParallelGroupBy.executeBySingleIndex(sourceRefs, targetRef, query, index);
//		context.nrGroups = index.groupIds.length;
		long timer2 = System.currentTimeMillis();
		System.out.println("Group: " + (timer2 - timer1));
		// TODO: need to replace references to columns in GROUP BY clause
	}
	/**
	 * Process aggregates that appear in the query. Stores
	 * references to aggregation results in query context.
	 * Uses results of a prior group by stage if any.
	 *
	 * @param queryInfo		query to process
	 * @param context		query processing context
	 * @throws Exception
	 */
	static void applyIndex(QueryInfo queryInfo, Context context, String targetRel,
						   boolean tempResult, Index index)
			throws Exception {
		// Generate table for holding aggregation input
		String aggSrcTbl = NamingConfig.AGG_SRC_TBL_NAME;
		TableInfo aggSrcTblInfo = new TableInfo(aggSrcTbl, true);
		CatalogManager.currentDB.nameToTable.put(aggSrcTbl, aggSrcTblInfo);
		// Generate table for holding aggregation results
		String aggTbl = NamingConfig.AGG_TBL_NAME;
		TableInfo aggTblInfo = new TableInfo(aggTbl, true);
		CatalogManager.currentDB.nameToTable.put(aggTbl, aggTblInfo);
		// Iterate over aggregates for processing
		int aggInputCtr = 0;
		// Generate table holding result
		TableInfo targetInfo = new TableInfo(targetRel, tempResult);
		CatalogManager.currentDB.addTable(targetInfo);
		PostStats.groupByMillis = 0;;
		PostStats.havingMillis = 0;
		long aggStart = System.currentTimeMillis();
		for (ExpressionInfo selInfo : queryInfo.selectExpressions) {
			String colName = queryInfo.selectToAlias.get(selInfo);
			String resultName = targetInfo.name;
			// Generate result reference
			ColumnRef resultRef = new ColumnRef(resultName, colName);
			// Is it a previously calculated aggregate?
			ColumnRef columnRef = selInfo.columnsMentioned.iterator().next();
			if (selInfo.aggregates.isEmpty()) {
				// Need to generate select item data -
				// selector must be based on group-by columns.
				String srcRel = NamingConfig.JOINED_NAME;
				ParallelMapRows.executeIndex(srcRel, selInfo, index, resultRef);
			} else {
				// Need to generate data - selector is
				// complex expression based on previously
				// calculated per-group aggregates.
				AggInfo aggInfo = queryInfo.aggregates.iterator().next();
				ExpressionInfo aggInput = aggInfo.aggInput;
				ColumnRef sourceRef = null;
				if (aggInput.finalExpression instanceof Column) {
					// No need to regenerate base column
					ColumnRef queryRef = aggInput.columnsMentioned.iterator().next();
					sourceRef = context.columnMapping.get(queryRef);

				} else {
					// Input is complex expression - generate data
					String joinRel = NamingConfig.JOINED_NAME;
					String sourceCol = NamingConfig.AGG_SRC_COL_PRE + aggInputCtr;
					sourceRef = new ColumnRef(aggSrcTbl, sourceCol);
					++aggInputCtr;
					ParallelMapRows.execute(joinRel, aggInput,
							context.columnMapping, null,
							null, null, -1, sourceRef);
				}
				long timer0 = System.currentTimeMillis();
				// Process aggregate
				switch (aggInfo.aggFunction) {
					case SUM:
//                        ParallelSumAggregate.executeIndex(sourceRef, index,
//                                groupRef, targetRef);
						break;
					case MIN:
//                        ParallelMinMaxAggregate.executeIndex(sourceRef, nrGroups,
//                                groupRef, false, targetRef);
						break;
					case MAX:
//                        ParallelMinMaxAggregate.execute(sourceRef, nrGroups,
//                                groupRef, true, targetRef);
						break;
					case AVG:
						ParallelAvgAggregate.executeIndex(sourceRef, index, resultRef, selInfo);
						break;
					default:
						throw new Exception("Error - aggregate " + aggInfo +
								" should have been rewritten");
				}
				long timer1 = System.currentTimeMillis();
				System.out.println("AggColumn: " + aggInfo + "\t" + (timer1 - timer0));
			}
		}
		long aggEnd = System.currentTimeMillis();
		PostStats.aggMillis = aggEnd - aggStart;
		// Update statistics on result table
		CatalogManager.updateStats(targetRel);

		// Print out aggregation table if activated
		if (LoggingConfig.PRINT_INTERMEDIATES) {
			RelationPrinter.print(aggTbl);
		}
	}
	/**
	 * Process aggregates that appear in the query. Stores
	 * references to aggregation results in query context.
	 * Uses results of a prior group by stage if any.
	 * 
	 * @param queryInfo		query to process
	 * @param context		query processing context
	 * @throws Exception
	 */
	static void aggregate(QueryInfo queryInfo, Context context) 
			throws Exception {
		// Generate table for holding aggregation input
		String aggSrcTbl = NamingConfig.AGG_SRC_TBL_NAME;
		TableInfo aggSrcTblInfo = new TableInfo(aggSrcTbl, true);
		CatalogManager.currentDB.nameToTable.put(aggSrcTbl, aggSrcTblInfo);
		// Generate table for holding aggregation results
		String aggTbl = NamingConfig.AGG_TBL_NAME;
		TableInfo aggTblInfo = new TableInfo(aggTbl, true);
		CatalogManager.currentDB.nameToTable.put(aggTbl, aggTblInfo);
		// Iterate over aggregates for processing
		int aggInputCtr = 0;
		int aggCtr = 0;
		for (AggInfo aggInfo : queryInfo.aggregates) {
			// Debugging output
			log("Treating aggregate nr. " + aggCtr + ": " + aggInfo);
			// Retrieve or generate source data
			ColumnRef sourceRef = null;
			ExpressionInfo aggInput = aggInfo.aggInput;
			if (aggInput.finalExpression instanceof Column) {
				// No need to regenerate base column
				ColumnRef queryRef = aggInput.columnsMentioned.iterator().next();
				sourceRef = context.columnMapping.get(queryRef);
			} else {
				// Input is complex expression - generate data
				String joinRel = NamingConfig.JOINED_NAME;
				String sourceCol = NamingConfig.AGG_SRC_COL_PRE + aggInputCtr;
				sourceRef = new ColumnRef(aggSrcTbl, sourceCol);
				++aggInputCtr;
//				MapRows.execute(joinRel, aggInput,
//						context.columnMapping, null,
//						null, -1, sourceRef);
				MapRows.parallelExecute(joinRel, aggInput,
						context.columnMapping, null,
						null, -1, sourceRef);
			}
			log("Source column: " + sourceRef);
			// Determine target column for aggregation
			String targetCol = NamingConfig.AGG_COL_PRE + aggCtr;
			++aggCtr;
			ColumnRef targetRef = new ColumnRef(aggTbl, targetCol);
			log("Target column: " + targetRef);
			// Update context
			context.aggToData.put(aggInfo.toString(), targetRef);
			context.columnMapping.put(targetRef, targetRef);
			// Process aggregate
			int nrGroups = context.nrGroups;
			ColumnRef groupRef = context.groupRef;
			switch (aggInfo.aggFunction) {
			case SUM:
				SumAggregate.execute(sourceRef, nrGroups,
						groupRef, targetRef);
//				SumAggregate.parallelExecute(sourceRef, nrGroups,
//						groupRef, targetRef, queryInfo);
				break;
			case MIN:
//				MinMaxAggregate.execute(sourceRef, nrGroups,
//						groupRef, false, targetRef);
				MinMaxAggregate.parallelExecute(sourceRef, nrGroups,
						groupRef, false, targetRef);
				break;
			case MAX:
				MinMaxAggregate.execute(sourceRef, nrGroups, 
						groupRef, true, targetRef);
				break;
			case AVG:
//				AvgAggregate.execute(sourceRef, nrGroups,
//						groupRef, targetRef);
				AvgAggregate.parallelExecute(sourceRef, nrGroups,
						groupRef, targetRef);
				break;
			default:
				throw new Exception("Error - aggregate " + aggInfo +
						" should have been rewritten");
			}
		}
		// Print out aggregation table if activated
		if (LoggingConfig.PRINT_INTERMEDIATES) {
			RelationPrinter.print(aggTbl);
		}
	}
	/**
	 * Adds a column that contains one element for each element
	 * in a source relation. The content of the column to add is
	 * described by an expression - we simply reuse column data
	 * if the expression simply references a source column.
	 * Otherwise, we evaluate the expression for each row in
	 * the source relation.
	 * 
	 * @param query		query to evaluate
	 * @param context	query processing context
	 * @param srcRel	determines cardinality of column to add
	 * @param expr		defines content of column to add
	 * @param result	table to which column is added
	 * @param colName	name of result column to add
	 * @throws Exception
	 */
	static void addPerRowCol(QueryInfo query, Context context, 
			String srcRel, ExpressionInfo expr, TableInfo result,
			String colName) throws Exception {
		// Get name of result relation
		String resultName = result.name;
		// Generate reference to result column to create
		ColumnRef resultRef = new ColumnRef(resultName, colName);
		// Is it an existing column?
		if (expr.finalExpression instanceof Column) {
			// Update catalog by adding result column
			ColumnInfo resultColInfo = new ColumnInfo(colName, 
					expr.resultType, false, false, false, false);
			result.addColumn(resultColInfo);
			// Re-use existing data as column in final result table
			ColumnRef queryRef = expr.columnsMentioned.iterator().next();
			ColumnRef dbRef = context.columnMapping.get(queryRef);
			// Associate new column with data in buffer manager
			ColumnData colData = BufferManager.getData(dbRef);
			BufferManager.colToData.put(resultRef, colData);
		} else {
			// No possibility to reuse existing data - generate new
//			MapRows.execute(srcRel, expr, context.columnMapping,
//					null, null, -1, resultRef);

			ParallelMapRows.execute(srcRel, expr, context.columnMapping,
					null, null, null, 0, resultRef);
		}
	}
	/**
	 * Do post-processing for a query without any aggregates
	 * (or group by clauses).
	 * 
	 * @param query				query to process
	 * @param context			query processing context
	 * @param resultRelName		name of final result relation
	 * @param tempResult		whether the result relation is temporary
	 * @throws Exception
	 */
	static void treatNoAggregatesQuery(QueryInfo query, 
			Context context, String resultRelName, 
			boolean tempResult) throws Exception {
		// Create relation containing query result
		String resultTbl = resultRelName;
		TableInfo result = new TableInfo(resultTbl, tempResult);
		CatalogManager.currentDB.nameToTable.put(resultTbl, result);
		// Name of source relation
		String joinRel = NamingConfig.JOINED_NAME;
		// Name of result relation
		String resultRel = result.name;
		PostStats.groupByMillis = 0;
		PostStats.havingMillis = 0;
		PostStats.orderMillis = 0;
		long aggStart = System.currentTimeMillis();
		// Iterate over expressions in SELECT clause
		for (ExpressionInfo expr : query.selectExpressions) {
			// Add corresponding result column
			String colName = query.selectToAlias.get(expr);
			addPerRowCol(query, context, joinRel, expr, result, colName);
		}
		long aggEnd = System.currentTimeMillis();
		PostStats.aggMillis = aggEnd - aggStart;
		// Update statistics on result
		CatalogManager.updateStats(resultRel);
		// Does query have an ORDER BY clause?
		if (!query.orderByExpressions.isEmpty()) {
			// Generate table holding order by columns
			String orderTbl = NamingConfig.ORDER_NAME;
			TableInfo orderInfo = new TableInfo(orderTbl, true);
			CatalogManager.currentDB.nameToTable.put(orderTbl, orderInfo);
			// Iterate over order by expressions
			int nrOrderCols = 0;
			for (ExpressionInfo expr : query.orderByExpressions) {
				// Add corresponding result column
				String colName = "orderby" + nrOrderCols;
				++nrOrderCols;
				addPerRowCol(query, context, joinRel, expr, orderInfo, colName);				
			}
			// Collect columns to sort
			List<ColumnRef> orderRefs = new ArrayList<>();
			for (int orderCtr=0; orderCtr<nrOrderCols; ++orderCtr) {
				orderRefs.add(new ColumnRef(orderTbl, "orderby" + orderCtr));
			}
			// Sort result table
			OrderBy.execute(orderRefs, query.orderByAsc, resultRel);
		}
		long orderEnd = System.currentTimeMillis();
		PostStats.orderMillis = orderEnd - aggEnd;
	}
	/**
	 * Treat queries that have aggregates but no group-by clauses.
	 * 
	 * @param query			query to process
	 * @param context		query processing context
	 * @param resultRelName	name of result relation
	 * @param tempResult	whether result relation is temporary
	 * @throws Exception
	 */
	static void treatAllRowsAggQuery(QueryInfo query, 
			Context context, String resultRelName, 
			boolean tempResult) throws Exception {
		// Create relation containing query result
		String resultTbl = resultRelName;
		TableInfo result = new TableInfo(resultTbl, tempResult);
		CatalogManager.currentDB.nameToTable.put(resultTbl, result);
		long aggStart = System.currentTimeMillis();
		// Calculate aggregates
		aggregate(query, context);
		// Get name of result relation
		String resultName = result.name;
		// Iterate over expressions in SELECT clause
		for (ExpressionInfo expr : query.selectExpressions) {
			// Update catalog by adding result column
			String colName = query.selectToAlias.get(expr);
			ColumnRef resultRef = new ColumnRef(resultName, colName);
			// Is it a previously calculated aggregate?
			String exprSQL = expr.finalExpression.toString();
			if (context.aggToData.containsKey(exprSQL)) {
				// Update catalog
				ColumnInfo resultColInfo = new ColumnInfo(colName, 
						expr.resultType, false, false, false, false);
				result.addColumn(resultColInfo);
				// Select item data was previously generated
				ColumnRef aggRef = context.aggToData.get(exprSQL);
				ColumnData aggData = BufferManager.getData(aggRef);
				BufferManager.colToData.put(resultRef, aggData);
			} else {
				// Need to generate select item data
				String srcRel = NamingConfig.AGG_TBL_NAME;
				MapRows.execute(srcRel, expr, context.columnMapping,
						context.aggToData, null, -1, resultRef);
			}
		}
		long aggEnd = System.currentTimeMillis();
		PostStats.groupByMillis = 0;
		PostStats.havingMillis = 0;
		PostStats.orderMillis = 0;
		PostStats.aggMillis = aggEnd - aggStart;
	}
	/**
	 * Add a column that has one row for each group. Either
	 * reuses previously evaluated aggregates for the column
	 * content or generates content on the fly.
	 * 
	 * @param query		query to process
	 * @param context	query processing context
	 * @param expr		defines column content
	 * @param groupRef	associates each source row with a group
	 * @param nrGroups	number of groups
	 * @param result	add column to this relation
	 * @param colName	name of column to add
	 * @throws Exception
	 */
	static void addPerGroupCol(QueryInfo query, Context context, 
			ExpressionInfo expr, ColumnRef groupRef, int nrGroups, 
			TableInfo result, String colName) throws Exception {
		String resultName = result.name;
		// Generate result reference
		ColumnRef resultRef = new ColumnRef(resultName, colName);
		// Is it a previously calculated aggregate?
		String exprSQL = expr.finalExpression.toString();
		if (context.aggToData.containsKey(exprSQL)) {
			// Update catalog by adding result column
			ColumnInfo resultColInfo = new ColumnInfo(colName, 
					expr.resultType, false, false, false, false);
			result.addColumn(resultColInfo);
			// Select item data was previously generated
			ColumnRef aggRef = context.aggToData.get(exprSQL);
			ColumnData aggData = BufferManager.getData(aggRef);
			BufferManager.colToData.put(resultRef, aggData);
		} else if (expr.aggregates.isEmpty()) {
			// Need to generate select item data -
			// selector must be based on group-by columns.
			String srcRel = NamingConfig.JOINED_NAME;
//			MapRows.parallelExecute(srcRel, expr, context.columnMapping,
//					context.aggToData, groupRef, nrGroups, resultRef);
			MapRows.execute(srcRel, expr, context.columnMapping,
					context.aggToData, groupRef, nrGroups, resultRef);
		} else {
			// Need to generate data - selector is
			// complex expression based on previously
			// calculated per-group aggregates.
			String srcRel = NamingConfig.AGG_TBL_NAME;
//			MapRows.parallelExecute(srcRel, expr, context.columnMapping,
//					context.aggToData, null, -1, resultRef);
			MapRows.execute(srcRel, expr, context.columnMapping,
					context.aggToData, null, -1, resultRef);
		}
	}
	/**
	 * Adds newly created relation containing for each item in
	 * the SELECT clause of the input query a new column.
	 * 
	 * @param query			treat SELECT clause of this query			
	 * @param context		execution context specifying mappings
	 * @param targetRel		name of target relation to create
	 * @param tempResult	whether target relation is temporary
	 * @throws Exception
	 */
	static void addPerGroupSelTbl(QueryInfo query, Context context, 
			String targetRel, boolean tempResult) throws Exception {
		// Retrieve information on groups
		ColumnRef groupRef = context.groupRef;
		int nrGroups = context.nrGroups;
		// Generate table holding result
		TableInfo targetInfo = new TableInfo(targetRel, tempResult);
		CatalogManager.currentDB.addTable(targetInfo);
		// Treat items in SELECT clause
		for (ExpressionInfo selInfo : query.selectExpressions) {
			String colName = query.selectToAlias.get(selInfo);
			addPerGroupCol(query, context, selInfo, groupRef, 
					nrGroups, targetInfo, colName);
		}
		// Update statistics on result table
		CatalogManager.updateStats(targetRel);
	}
	/**
	 * Adds table containing for each item in the input query's
	 * ORDER BY clause a column containing corresponding per-group
	 * values.
	 * 
	 * @param query			treat ORDER BY clause of this query
	 * @param context		execution context specifying mappings
	 * @param targetRel		name of target relation to create
	 * @param tempResult	whether target relation is temporary
	 * @throws Exception
	 */
	static void addPerGroupOrderTbl(QueryInfo query, Context context, 
			String targetRel, boolean tempResult) throws Exception {
		// Retrieve information on groups
		ColumnRef groupRef = context.groupRef;
		int nrGroups = context.nrGroups;
		// Generate table holding order by columns
		TableInfo orderInfo = new TableInfo(targetRel, true);
		CatalogManager.currentDB.addTable(orderInfo);
		// Iterate over order by expressions
		int nrOrderCols = 0;
		for (ExpressionInfo expr : query.orderByExpressions) {
			// Add corresponding result column
			String colName = "orderby" + nrOrderCols;
			++nrOrderCols;
			addPerGroupCol(query, context, expr, groupRef, 
					nrGroups, orderInfo, colName);
		}
	}
	/**
	 * Returns indices of groups (which is at the same time the
	 * indices of rows containing corresponding results) that
	 * satisfy the condition in the HAVING clause.
	 * 
	 * @param query			query whose HAVING clause to process
	 * @param context		execution context containing column mappings
	 * @return				indices of rows satisfying HAVING clause
	 * @throws Exception
	 */
	static List<Integer> havingRows(QueryInfo query,
			Context context) throws Exception {
		ExpressionInfo havingExpr = query.havingExpression;
		// Generate table containing result of having expression
		String havingTbl = NamingConfig.HAVING_TBL_NAME;
		TableInfo havingInfo = new TableInfo(havingTbl, true);
		CatalogManager.currentDB.addTable(havingInfo);
		addPerGroupCol(query, context, havingExpr, context.groupRef, 
				context.nrGroups, havingInfo, NamingConfig.HAVING_COL_NAME);
		ColumnRef havingRef = new ColumnRef(havingTbl, 
				NamingConfig.HAVING_COL_NAME);
		// Collect indices of group passing the having predicate
		int[] groupHaving = ((IntData)BufferManager.getData(havingRef)).data;

		List<Integer> havingGroups = new ArrayList<>(groupHaving.length);
		for (int groupCtr = 0; groupCtr < groupHaving.length; ++groupCtr) {
			if (groupHaving[groupCtr]>0) {
				havingGroups.add(groupCtr);
			}
		}
		return havingGroups;
	}
	/**
	 * Treat a query that aggregates over groups of rows.
	 * 
	 * @param query			query to process
	 * @param context		query processing context
	 * @param resultRelName	name of final result relation
	 * @param tempResult	whether result relation is temporary
	 * @throws Exception
	 */
	static void treatGroupAggQuery(QueryInfo query, 
			Context context, String resultRelName, 
			boolean tempResult) throws Exception {
		// Use index
		int joinCard = CatalogManager.getCardinality(NamingConfig.JOINED_NAME);
		Index index = query.groupByExpressions.size() == 1 ?
				BufferManager.colToIndex.get(query.groupByExpressions.iterator().next().
						columnsMentioned.iterator().next()) : null;

		// Determine whether query has HAVING clause
		ExpressionInfo havingExpr = query.havingExpression;
		boolean hasHaving = havingExpr!=null;

		// Determine whether query has ORDER BY clause
		boolean hasOrder = !query.orderByExpressions.isEmpty();

		if (index != null && joinCard == index.cardinality && !hasHaving) {
			applyIndex(query, context, resultRelName, tempResult, index);
		}
		else {
			long groupStart = System.currentTimeMillis();
			boolean hasCase = query.selectExpressions.stream().filter(expressionInfo ->
					expressionInfo.originalExpression.toString().contains("CASE WHEN"))
					.findFirst().orElse(null) != null;
			// Execute group by
			if (index != null && joinCard == index.cardinality && !index.sorted) {
				groupBySingleIndex(query, context, index);
			} else if (hasCase) {
				groupBy(query, context);
			}
			else {
				List<String[]> selectASTNodes = new ArrayList<>();
				String havingNode = "";
				JNIParser parser = new JNIParser(query, context.columnMapping);
				parser.isSelect = true;
				int nrIntSelects = 0;
				int nrDoubleSelects = 0;
				int count = 0;
				List<ColumnRef> intColRefs = new ArrayList<>();
				List<ColumnRef> doubleColRefs = new ArrayList<>();
				List<ExpressionInfo> selectExpressions = new ArrayList<>(query.selectExpressions);

				ExpressionInfo havingInfo = null;
				String havingCol = null;
				if (hasHaving) {
					for (Function agg: query.havingExpression.aggregates) {
						ExpressionInfo exp = selectExpressions.stream().filter(s ->
								s.finalExpression.toString().equals(agg.toString())).findFirst().orElse(null);
						if (exp == null) {
							try {
								havingInfo = new ExpressionInfo(query, agg);
								selectExpressions.add(havingInfo);

							} catch (Exception e) {
								e.printStackTrace();
							}
						}
						else {
							havingInfo = exp;
						}
					}
				}
				int nrSelects = selectExpressions.size();
				boolean[] isIntSelects = new boolean[nrSelects];
				for (ExpressionInfo select : selectExpressions) {
					Expression checkExpression = select.originalExpression.toString().startsWith("count") ?
							select.originalExpression : select.finalExpression;
					checkExpression.accept(parser);
					String colName = query.selectToAlias.get(select);
					if (colName == null) {
						colName = "tmp";
					}
					ColumnRef resultRef = new ColumnRef(resultRelName, colName);

					if (select.resultType != DOUBLE) {
						isIntSelects[count] = true;
						if (select == havingInfo) {
							havingCol = "IT(" + nrIntSelects + ")";
						}
						nrIntSelects++;
						intColRefs.add(resultRef);
					}
					else {
						if (select == havingInfo) {
							havingCol = "DT(" + nrDoubleSelects + ")";
						}
						nrDoubleSelects++;
						doubleColRefs.add(resultRef);
					}
					if (parser.jniAST.isEmpty()) {
						String[] columns = new String[]{parser.extractedIndex.pop()};
						selectASTNodes.add(columns);
					}
					else {
						selectASTNodes.add(parser.jniAST.toArray(new String[0]));
					}
					parser.nodeIndexes.clear();
					parser.extractedIndex.clear();
					parser.extractedNumbers.clear();
					parser.extractedConstants.clear();
					parser.jniAST.clear();
					parser.isDouble = false;
					count++;
				}
				// Check having expression

				// Add group by columns
				List<Integer> groupIntCols = new ArrayList<>();
				List<Integer> groupDoubleCols = new ArrayList<>();
				int groupCol = -1;
				int maxKeys = -1;
				List<String> groupIntMapCols = new ArrayList<>();
				List<String> groupDoubleMapCols = new ArrayList<>();
				boolean needSort = true;
				for (ExpressionInfo group : query.groupByExpressions) {
					group.finalExpression.accept(parser);
					String column = group.finalExpression instanceof Column ?
							parser.extractedIndex.pop() : parser.jniAST.get(0).split("-")[0];
					int columnIdx = Integer.parseInt(column.substring(3, column.length() - 1));
					ColumnRef mentionedCol = group.columnsMentioned.iterator().next();
					Index mentionedIndex = BufferManager.colToIndex.get(mentionedCol);
					ColumnData data = BufferManager.colToData.get(mentionedCol);
					if (group.finalExpression instanceof ExtractExpression) {
						int nrKeys = 30;
						if (nrKeys > maxKeys) {
							groupCol = columnIdx;
							maxKeys = nrKeys;
						}
						if (column.charAt(0) == 'I') {
							groupIntMapCols.add(parser.jniAST.get(0));
						} else {
							groupDoubleMapCols.add(parser.jniAST.get(0));
						}
					}
					else if (mentionedIndex instanceof IntPartitionIndex) {
						int nrKeys = ((IntPartitionIndex) mentionedIndex).nrKeys;
						if (nrKeys > maxKeys) {
							groupCol = columnIdx;
							maxKeys = nrKeys;
							needSort = index == null || joinCard != index.cardinality || (!mentionedIndex.sorted);
						}
					} else if (data instanceof IntData){
						// Sample 500 values
						IntData intData = (IntData) data;
						IntSet intSet = HashIntSets.newMutableSet();
						for (int rowCtr = 0; rowCtr < 500; rowCtr++) {
							intSet.add(intData.data[rowCtr]);
						}
						int nrKeysLowerBound = intSet.size();
						if (nrKeysLowerBound > maxKeys) {
							groupCol = columnIdx;
							maxKeys = nrKeysLowerBound;
						}
					}
					if (column.charAt(0) == 'I') {
						groupIntCols.add(columnIdx);
						groupIntMapCols.add("");
					} else {
						groupDoubleCols.add(columnIdx);
						groupDoubleMapCols.add("");
					}
					parser.nodeIndexes.clear();
					parser.extractedIndex.clear();
					parser.extractedNumbers.clear();
					parser.extractedConstants.clear();
					parser.jniAST.clear();
				}
				if (hasHaving) {
					parser.isHaving = true;
					parser.havingCol = havingCol;
					query.havingExpression.finalExpression.accept(parser);
					havingNode = parser.jniAST.get(0);
				}
				// All columns to be materialized
				List<int[]> intSrcCols = new ArrayList<>();
				List<double[]> doubleSrcCols = new ArrayList<>();
				List<Map.Entry<ColumnRef, String>> sortedColRefs = parser.dataIndexMap.entrySet()
						.stream().sorted(Map.Entry.comparingByValue()).collect(Collectors.toList());
				for (Map.Entry<ColumnRef, String> entry: sortedColRefs) {
					ColumnRef columnRef = entry.getKey();
					ColumnData columnData = BufferManager.getData(columnRef);
					if (columnData instanceof IntData) {
						intSrcCols.add(((IntData) columnData).data);
					}
					else {
						doubleSrcCols.add(((DoubleData) columnData).data);
					}
				}

				int[][] intTargetCols = new int[nrIntSelects][];
				double[][] doubleTargetCols = new double[nrSelects - nrIntSelects][];
				long group_start = System.currentTimeMillis();
				JNIFilter.postprocessing(intSrcCols.toArray(new int[0][0]), doubleSrcCols.toArray(new double[0][0]),
						groupIntCols.stream().mapToInt(i->i).toArray(),
						groupDoubleCols.stream().mapToInt(i->i).toArray(),
						groupIntMapCols.toArray(new String[0]), groupDoubleMapCols.toArray(new String[0]),
						groupCol, maxKeys,
						selectASTNodes.toArray(new String[0][0]), isIntSelects, nrIntSelects,
						havingNode, needSort,
						intTargetCols, doubleTargetCols, joinCard,
						ParallelConfig.EXE_THREADS);
				long groupEnd = System.currentTimeMillis() - group_start;
				// Update catalog, inserting materialized table
				TableInfo resultTable = new TableInfo(resultRelName, true);
				CatalogManager.currentDB.addTable(resultTable);
				for (ExpressionInfo selInfo : query.selectExpressions) {
					String colName = query.selectToAlias.get(selInfo);
					// Update catalog by adding result column
					ColumnInfo resultColInfo = new ColumnInfo(colName,
							selInfo.resultType, false, false, false, false);
					resultTable.addColumn(resultColInfo);
				}
				for (int intColCtr = 0; intColCtr < nrIntSelects; intColCtr++) {
					int[] target = intTargetCols[intColCtr];
					IntData intData = new IntData(target);
					BufferManager.colToData.put(intColRefs.get(intColCtr), intData);
				}
				for (int doubleColCtr = 0; doubleColCtr < (nrSelects - nrIntSelects); doubleColCtr++) {
					double[] target = doubleTargetCols[doubleColCtr];
					DoubleData doubleData = new DoubleData(target);
					BufferManager.colToData.put(doubleColRefs.get(doubleColCtr), doubleData);
				}
				// Update statistics in catalog
				CatalogManager.updateStats(resultRelName);
				System.out.println("JNI post: " + groupEnd);
				if (hasOrder) {
					TableInfo orderInfo = CatalogManager.getTable(resultRelName);
					List<ColumnRef> orderRefs = new ArrayList<>();
					for (ExpressionInfo orderExpInfo: query.orderByExpressions) {
						String columnName = orderExpInfo.originalExpression.toString();
						orderRefs.add(new ColumnRef(resultRelName, columnName));
					}

					OrderBy.execute(orderRefs, query.orderByAsc, resultRelName);
				}
				return;
				//				groupBy(query, context);
			}
			long groupEnd = System.currentTimeMillis();
			PostStats.groupByMillis = groupEnd - groupStart;
			// Calculate aggregates
			aggregate(query, context);
			long aggEnd = System.currentTimeMillis();
			PostStats.aggMillis = aggEnd - groupEnd;
			System.out.println("Agg: " + (aggEnd - groupEnd));
			// Different treatment for queries with/without HAVING
			if (hasHaving) {
				// Having clause specified - insertinto intermediate result table
				addPerGroupSelTbl(query, context,
						NamingConfig.RESULT_NO_HAVING, true);
				// Get groups satisfying HAVING clause
				List<Integer> havingGroups = havingRows(query, context);
				// Prepare sorting if ORDER BY clause is specified
				if (hasOrder) {
					addPerGroupOrderTbl(query, context,
							NamingConfig.ORDER_NO_HAVING, true);
				}
				// Filter result to having groups
				TableInfo noHavingResInfo = CatalogManager.getTable(
						NamingConfig.RESULT_NO_HAVING);
				Materialize.execute(NamingConfig.RESULT_NO_HAVING,
						noHavingResInfo.columnNames, havingGroups,
						null, resultRelName, tempResult);
				// Filter order table to having groups if applicable
				if (hasOrder) {
					TableInfo noHavingOrderInfo = CatalogManager.getTable(
							NamingConfig.ORDER_NO_HAVING);
					Materialize.execute(NamingConfig.ORDER_NO_HAVING,
							noHavingOrderInfo.columnNames, havingGroups,
							null, NamingConfig.ORDER_NAME, true);
				}
			} else {
				// No having clause specified - insert into final result table
				addPerGroupSelTbl(query, context, resultRelName, tempResult);
				// Prepare sorting if ORDER BY clause is specified
				if (hasOrder) {
					// Add table containing values for order-by items
					addPerGroupOrderTbl(query, context,
							NamingConfig.ORDER_NAME, true);
				}
			}
			long havingEnd = System.currentTimeMillis();
			PostStats.havingMillis = havingEnd - aggEnd;
			System.out.println("Having: " + (havingEnd - aggEnd));
		}
		long orderStart = System.currentTimeMillis();
		// Sort result table if applicable
		if (hasOrder) {
			String orderTbl = NamingConfig.ORDER_NAME;
			TableInfo orderInfo = CatalogManager.getTable(orderTbl);
			List<ColumnRef> orderRefs = new ArrayList<>();
			for (String orderCol : orderInfo.columnNames) {
				orderRefs.add(new ColumnRef(orderTbl, orderCol));
			}
			OrderBy.execute(orderRefs, query.orderByAsc, resultRelName);			
		}
		long orderEnd = System.currentTimeMillis();
		PostStats.orderMillis = orderEnd - orderStart;
	}
	/**
	 * Generate debugging output if activated.
	 * 
	 * @param logEntry	entry to output
	 */
	static void log(String logEntry) {
		if (LoggingConfig.POST_PROCESSING_VERBOSE) {
			System.out.println(logEntry);
		}
	}
	/**
	 * Do post-processing (including aggregation, grouping, or sorting)
	 * for given query and store final query result in result table.
	 * 
	 * @param query			query to process
	 * @param context		query processing context
	 * @param resultRel		name of final result relation
	 * @param tempResult	whether the result relation is temporary
	 */
	public static void process(QueryInfo query, Context context,
			String resultRel, boolean tempResult) throws Exception {
		// Start counter
		long startMillis = System.currentTimeMillis();
		// Store full result in preliminary table if limit specified 
		boolean hasLimit = query.limit!=-1;
		String preLimitResult = hasLimit?NamingConfig.PRE_LIMIT_TBL:resultRel;
		boolean preLimitTemp = hasLimit?true:tempResult;
		// Distinguish type of query
		switch (query.aggregationType) {
		case NONE:
			treatNoAggregatesQuery(query, context, 
					preLimitResult, preLimitTemp);
			break;
		case ALL_ROWS:
			treatAllRowsAggQuery(query, context, 
					preLimitResult, preLimitTemp);
			break;
		case GROUPS:
			treatGroupAggQuery(query, context, 
					preLimitResult, preLimitTemp);
			break;
		}
		// Apply LIMIT clause if any
		if (hasLimit) {
			// Add final result table in catalog
			TableInfo preLimitInfo = CatalogManager.getTable(preLimitResult);
			CatalogManager.updateStats(preLimitResult);
			int preLimitCard = CatalogManager.getCardinality(preLimitResult);
			// Fill with subset of pre-limit result rows
			List<Integer> limitRows = new ArrayList<>();
			int limit = Math.min(query.limit, preLimitCard);
			for (int rowCtr=0; rowCtr<limit; ++rowCtr) {
				limitRows.add(rowCtr);
			}
			operators.Materialize.execute(preLimitResult, 
					preLimitInfo.columnNames, limitRows, null, 
					resultRel, true);
		}
		// Update result table statistics
		CatalogManager.updateStats(resultRel);
		// Measure time and store as statistics
		PostStats.postMillis = System.currentTimeMillis() - startMillis;
	}
}
