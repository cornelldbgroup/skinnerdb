package operators;

import buffer.BufferManager;
import catalog.CatalogManager;
import catalog.info.ColumnInfo;
import catalog.info.TableInfo;
import config.NamingConfig;
import config.ParallelConfig;
import data.ColumnData;
import data.IntData;
import expressions.ExpressionInfo;
import expressions.aggregates.AggInfo;
import joining.result.ResultTuple;
import net.sf.jsqlparser.schema.Column;
import preprocessing.Context;
import query.ColumnRef;
import query.QueryInfo;
import types.SQLtype;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.RecursiveTask;


public class MinOperator extends RecursiveTask<ResultTuple> {
    /**
     * Start index of results lists.
     */
    private int start;
    /**
     * End index of results list.
     */
    private int end;
    /**
     * Query to process.
     */
    private QueryInfo queryInfo;
    /**
     * Query processing context
     */
    private Context context;

    /**
     *
     */
    public MinOperator(QueryInfo queryInfo, Context context, int start, int end) {
        this.start = start;
        this.end = end;
        this.queryInfo = queryInfo;
        this.context = context;
    }


    @Override
    protected ResultTuple compute() {
        if (end - start < ParallelConfig.POST_SIZES) {
            int[] minResult = new int[queryInfo.aggregates.size()];
            // Iterate over aggregates for processing
            int aggInputCtr = 0;
            for (AggInfo aggInfo : queryInfo.aggregates) {
                // Retrieve or generate source data
                ColumnRef sourceRef;
                ExpressionInfo aggInput = aggInfo.aggInput;
                if (aggInput.finalExpression instanceof Column) {
                    // No need to regenerate base column
                    ColumnRef queryRef = aggInput.columnsMentioned.iterator().next();
                    sourceRef = context.columnMapping.get(queryRef);
                }
                // TODO: What are these used for?
                else {
                    // Input is complex expression - generate data
                    String joinRel = NamingConfig.JOINED_NAME;
                    String sourceCol = NamingConfig.AGG_SRC_COL_PRE + aggInputCtr;
                    String aggTbl = "";
                    sourceRef = new ColumnRef(aggTbl, sourceCol);
                    ++aggInputCtr;
                    try {
                        MapRows.execute(joinRel, aggInput,
                                context.columnMapping, null,
                                null, -1, sourceRef);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
//                    log("Source column: " + sourceRef);
                // Process aggregate
                int index = context.colToIndex.get(sourceRef);
                IntData srcData = (IntData) context.results[index];
                int last = Math.min(end, srcData.cardinality);
                // Get information about source column
                ColumnRef groupRef = context.groupRef;
                // Do we aggregate per group or overall?
                if (groupRef == null) {
                    // No grouping
                    int resultRow = -1;
                    for (int row = start; row < last; ++row) {
                        // Only consider non-NULL values
                        if (!srcData.isNull.get(row)) {
                            // Is this the first row?
                            if (resultRow == -1) {
                                resultRow = row;
                            } else {
                                int cmp = srcData.compareRows(resultRow, row);
                                if (cmp == 1) {
                                    resultRow = row;
                                }
                            }
                        }
                    }

                    minResult[index] = srcData.data[resultRow];
                }
                // TODO: need to fix it!
                else {
                    int nrGroups = context.nrGroups;
                    // Aggregate by group
                    // Each group will be associated with row index
                    // where minimal/maximal group value is located.
                    int[] rowForGroup = new int[nrGroups];
                    Arrays.fill(rowForGroup, -1);
                    // Iterate over source rows and update groups
                    int[] groups = new int[0];
                    try {
                        groups = ((IntData) BufferManager.getData(groupRef)).data;
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    for (int row = start; row < last; ++row) {
                        // Only consider non-NULL values
                        if (!srcData.isNull.get(row)) {
                            int group = groups[row];
                            if (rowForGroup[group] == -1) {
                                // First considered row for group
                                rowForGroup[group] = row;
                            } else {
                                // Need to compare against prior minimum/maximum
                                int priorRow = rowForGroup[group];
                                // Need to replace prior min/max?
                                int cmp = srcData.compareRows(priorRow, row);
                                if (cmp > 0) {
                                    rowForGroup[group] = row;
                                }
                            }
                        }
                    }
                    // Collect result rows
                    for (int group = 0; group < nrGroups; ++group) {
                        int row = rowForGroup[group];
//                        resultRows.add(row);
                    }
                }
            }

            return new ResultTuple(minResult);
        } else {
            int middle = (end + start) / 2;
            MinOperator taskLeft = new MinOperator(queryInfo, context, start, middle);
            MinOperator taskRight = new MinOperator(queryInfo, context, middle, end);
            invokeAll(taskLeft, taskRight);
            return minResult(taskLeft.join(), taskRight.join());
        }
    }

    private ResultTuple minResult(ResultTuple left, ResultTuple right) {
        int[] minResult = new int[queryInfo.aggregates.size()];
        for (int i = 0; i < minResult.length; i++) {
            int leftIndex = left.baseIndices[i];
            int rightIndex = right.baseIndices[i];
            minResult[i] = Math.min(leftIndex, rightIndex);
        }
        return new ResultTuple(minResult);
    }
}
