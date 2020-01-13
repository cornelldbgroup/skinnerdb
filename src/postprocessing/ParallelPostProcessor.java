package postprocessing;

import buffer.BufferManager;
import catalog.CatalogManager;
import catalog.info.ColumnInfo;
import catalog.info.TableInfo;
import config.LoggingConfig;
import config.NamingConfig;
import config.ParallelConfig;
import data.ColumnData;
import data.IntData;
import expressions.ExpressionInfo;
import expressions.aggregates.AggInfo;
import indexing.Index;
import net.sf.jsqlparser.schema.Column;
import operators.*;
import operators.parallel.*;
import preprocessing.Context;
import print.RelationPrinter;
import query.ColumnRef;
import query.QueryInfo;
import statistics.PostStats;

import java.util.ArrayList;
import java.util.List;
/**
 * Uses the result of the join phase as input and
 * takes care of grouping, aggregation, and
 * sorting. Produces the final result of a
 * query block in parallel.
 *
 * @author Ziyun Wei
 *
 */
public class ParallelPostProcessor {

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
                                       Context context,
                                       String resultRelName,
                                       boolean tempResult) throws Exception {
        // Create relation containing query result
        String resultTbl = resultRelName;
        TableInfo result = new TableInfo(resultTbl, tempResult);
        CatalogManager.currentDB.nameToTable.put(resultTbl, result);
        // Name of source relation
        String joinRel = NamingConfig.JOINED_NAME;
        // Name of result relation
        String resultRel = result.name;
        // Iterate over expressions in SELECT clause
        for (ExpressionInfo expr : query.selectExpressions) {
            // Add corresponding result column
            String colName = query.selectToAlias.get(expr);
            long timer0 = System.currentTimeMillis();
            addPerRowCol(query, context, joinRel, expr, result, colName);
            long timer1 = System.currentTimeMillis();
//            System.out.println("Column: " + colName + "\t" + (timer1 - timer0));
        }
        // Update statistics on result
        CatalogManager.updateStats(resultRel);
        // Does query have an ORDER BY clause?
        // TODO: Not Finished
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
     * @param expr		defines content of column to add
     * @param result	table to which column is added
     * @param colName	name of result column to add
     * @throws Exception
     */
    static void addPerRowCol(QueryInfo query, Context context, String srcRel,
                             ExpressionInfo expr, TableInfo result,
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
            // Re-use existing data as column in final result table
            ColumnRef queryRef = expr.columnsMentioned.iterator().next();
            int srcIndex = query.aliasToIndex.get(queryRef.aliasName);
            // No possibility to reuse existing data - generate new
            ParallelMapRows.execute(srcRel, expr, context.columnMapping,
                    null, null, null, -1, resultRef);
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
    static void groupBy(QueryInfo query, Context context, Index index) throws Exception {
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
        int maxSize = 1;
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
                ParallelMapRows.execute(sourceRel, groupExpr,
                        context.columnMapping,
                        null, null, null, -1, targetRef);
                sourceRefs.add(targetRef);
            }
        }
        // Execute group by
        String targetCol = NamingConfig.GROUPS_COL_NAME;
        ColumnRef targetRef = new ColumnRef(groupTbl, targetCol);
        // Update query context for following steps
        context.groupRef = targetRef;
//        context.nrGroups = GroupBy.execute(sourceRefs, targetRef);
        if (index != null) {
            context.groupsToIndex = ParallelGroupBy.executeIndex(sourceRefs, targetRef, index);
            context.nrGroups = index.groupIds.length;
        }
        else {
            context.groupsToIndex = ParallelGroupBy.execute(sourceRefs, targetRef, maxSize);
            context.nrGroups = context.groupsToIndex.size();
        }
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
                ParallelMapRows.execute(joinRel, aggInput,
                        context.columnMapping, null,
                        null, null, -1, sourceRef);
            }
            // Determine target column for aggregation
            String targetCol = NamingConfig.AGG_COL_PRE + aggCtr;
            ++aggCtr;
            ColumnRef targetRef = new ColumnRef(aggTbl, targetCol);
            // Update context
            context.aggToData.put(aggInfo.toString(), targetRef);
            context.columnMapping.put(targetRef, targetRef);
            // Process aggregate
            int nrGroups = context.nrGroups;
            ColumnRef groupRef = context.groupRef;
//            long timer0 = System.currentTimeMillis();
            switch (aggInfo.aggFunction) {
                case SUM:
                    if (context.groupsToIndex != null && nrGroups < ParallelConfig.PRE_BATCH_SIZE)
                        ParallelSumAggregate.execute(sourceRef, nrGroups, context.groupsToIndex,
                            groupRef, targetRef);
                    else
                        SumAggregate.execute(sourceRef, nrGroups, groupRef, targetRef);
                    break;
                case MIN:
                    ParallelMinMaxAggregate.execute(sourceRef, nrGroups,
                            groupRef, false, targetRef);
//                    MinMaxAggregate.execute(sourceRef, nrGroups, groupRef, false, targetRef);
                    break;
                case MAX:
                    ParallelMinMaxAggregate.execute(sourceRef, nrGroups,
                            groupRef, true, targetRef);
                    break;
                case AVG:
                    if (context.groupsToIndex != null && nrGroups < ParallelConfig.PRE_BATCH_SIZE)
                        ParallelAvgAggregate.execute(sourceRef, nrGroups, context.groupsToIndex,
                            groupRef, targetRef);
                    else
                        AvgAggregate.execute(sourceRef, nrGroups, groupRef, targetRef);
                    break;
                default:
                    throw new Exception("Error - aggregate " + aggInfo +
                            " should have been rewritten");
            }
//            long timer1 = System.currentTimeMillis();
//            System.out.println("AggColumn: " + aggInfo + "\t" + (timer1 - timer0));
        }
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
    static void applyIndex(QueryInfo queryInfo, Context context, String targetRel, boolean tempResult, Index index)
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
                if (index != null) {
                    // Determine target column for aggregation
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
                }
                else {
                    throw new RuntimeException("No index is available!");
                }
                long timer1 = System.currentTimeMillis();
                System.out.println("AggColumn: " + aggInfo + "\t" + (timer1 - timer0));
            }
        }
        // Update statistics on result table
        CatalogManager.updateStats(targetRel);

        // Print out aggregation table if activated
        if (LoggingConfig.PRINT_INTERMEDIATES) {
            RelationPrinter.print(aggTbl);
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
//            System.out.println("Column: " + colName + "\t" + (timer1 - timer0));
        }
        // Update statistics on result table
        CatalogManager.updateStats(targetRel);
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
            if (context.groupsToIndex != null && nrGroups < ParallelConfig.PRE_BATCH_SIZE)
                ParallelMapRows.execute(srcRel, expr, context.columnMapping,
                    context.aggToData, context.groupsToIndex, groupRef, nrGroups, resultRef);
            else
                MapRows.execute(srcRel, expr, context.columnMapping,
                        context.aggToData, groupRef, nrGroups, resultRef);
        } else {
            // Need to generate data - selector is
            // complex expression based on previously
            // calculated per-group aggregates.
            String srcRel = NamingConfig.AGG_TBL_NAME;
            if (context.groupsToIndex != null && nrGroups < ParallelConfig.PRE_BATCH_SIZE)
                ParallelMapRows.execute(srcRel, expr, context.columnMapping,
                    context.aggToData, context.groupsToIndex,null, -1, resultRef);
            else
                MapRows.execute(srcRel, expr, context.columnMapping,
                        context.aggToData, null, -1, resultRef);
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
        List<Integer> havingGroups = new ArrayList<>();
        for (int groupCtr=0; groupCtr<context.nrGroups; ++groupCtr) {
            if (groupHaving[groupCtr]>0) {
                havingGroups.add(groupCtr);
            }
        }
        return havingGroups;
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
            long timer0 = System.currentTimeMillis();
            String colName = "orderby" + nrOrderCols;
            ++nrOrderCols;
            addPerGroupCol(query, context, expr, groupRef,
                    nrGroups, orderInfo, colName);
            long timer1 = System.currentTimeMillis();
//            System.out.println("Ordering Column: " + colName + " " + query.selectToAlias.get(expr) + "\t" + (timer1 - timer0));
        }
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
                                   Context context,
                                   String resultRelName,
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
            if (index != null && joinCard == index.cardinality) {
                groupBy(query, context, index);
            }
            else {
                groupBy(query, context, null);
            }
            // Calculate aggregates
            aggregate(query, context);
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
                long timerx = System.currentTimeMillis();
                if (hasOrder) {
                    // Add table containing values for order-by items
                    addPerGroupOrderTbl(query, context,
                            NamingConfig.ORDER_NAME, true);
                }
                long timery = System.currentTimeMillis();
//            System.out.println("Adding column: " + (timerx - timer2) + "\tOrdering: " + (timery - timerx));

            }
        }
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
                ParallelMapRows.execute(srcRel, expr, context.columnMapping,
                        context.aggToData, null, null, -1, resultRef);
            }
        }
    }

    public static void process(QueryInfo query, Context context,
                               String resultRel, boolean tempResult) throws Exception {
        // Start counter
        long startMillis = System.currentTimeMillis();
        // Store full result in preliminary table if limit specified
        boolean hasLimit = query.limit!=-1;
        String preLimitResult = hasLimit? NamingConfig.PRE_LIMIT_TBL:resultRel;
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
        PostStats.subPostMillis.add(PostStats.postMillis);
    }
}
