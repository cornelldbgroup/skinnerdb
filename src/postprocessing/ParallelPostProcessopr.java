package postprocessing;

import buffer.BufferManager;
import catalog.CatalogManager;
import catalog.info.ColumnInfo;
import catalog.info.TableInfo;
import config.LoggingConfig;
import config.NamingConfig;
import data.ColumnData;
import data.IntData;
import expressions.ExpressionInfo;
import expressions.aggregates.AggInfo;
import joining.result.ResultTuple;
import net.sf.jsqlparser.schema.Column;
import operators.MapRows;
import operators.MinMaxAggregate;
import operators.MinOperator;
import preprocessing.Context;
import query.ColumnRef;
import query.QueryInfo;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;

/**
 * Uses the result of the join phase as input and
 * takes care of grouping, aggregation, and
 * sorting in parallel. Produces the final result of a
 * query block.
 *
 * @author Ziyun Wei
 *
 */
public class ParallelPostProcessopr {
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

    }
    /**
     * Treat queries that have aggregates but no group-by clauses.
     *
     * @param query			query to process
     * @param context		query processing context
     * @throws Exception
     */
    static void treatAllRowsAggQuery(QueryInfo query,
                                     Context context) throws Exception {
        // Create relation containing query result
        String resultTbl = NamingConfig.FINAL_RESULT_NAME;
        TableInfo result = new TableInfo(resultTbl, true);
        CatalogManager.currentDB.nameToTable.put(resultTbl, result);

        ForkJoinPool pool = new ForkJoinPool();

        MinOperator postTask = new MinOperator(query, context, 0, context.results.length);
        ForkJoinTask<ResultTuple> joinTask = pool.submit(postTask);

        ResultTuple resultTuple = joinTask.get();


        // Materialize
        String resultName = result.name;
        // Iterate over expressions in SELECT clause
        for (ExpressionInfo expr : query.selectExpressions) {
            // Update catalog by adding result column
            String colName = query.selectToAlias.get(expr);
            ColumnRef resultRef = new ColumnRef(resultName, colName);
            // Is it a previously calculated aggregate?
            ColumnRef queryRef = expr.columnsMentioned.iterator().next();;

            if (context.columnMapping.containsKey(queryRef)) {
                ColumnRef sourceRef = context.columnMapping.get(queryRef);
                int index = context.colToIndex.get(sourceRef);
                int value = resultTuple.baseIndices[index];
                IntData aggData = new IntData(1);
                aggData.data[0] = value;
                BufferManager.colToData.put(resultRef, aggData);
            } else {
                // Need to generate select item data
                String srcRel = NamingConfig.AGG_TBL_NAME;
                MapRows.execute(srcRel, expr, context.columnMapping,
                        context.aggToData, null, -1, resultRef);
            }
        }
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
     * @param query		query to process
     * @param context	query processing context
     */
    public static void process(QueryInfo query, Context context) throws Exception {
        // Distinguish type of query
        switch (query.aggregationType) {
//            case NONE:
//                treatNoAggregatesQuery(query, context);
//                break;
            case ALL_ROWS:
                treatAllRowsAggQuery(query, context);
                break;
//            case GROUPS:
//                treatGroupAggQuery(query, context);
//                break;
        }
        // Apply LIMIT clause

    }
}
