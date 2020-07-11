package operators;

import buffer.BufferManager;
import catalog.CatalogManager;
import catalog.info.ColumnInfo;
import catalog.info.TableInfo;
import data.DoubleData;
import data.IntData;
import data.LongData;
import data.StringData;
import expressions.ExpressionInfo;
import expressions.compilation.*;
import indexing.DefaultIntIndex;
import indexing.DoubleIndex;
import indexing.Index;
import indexing.IntIndex;
import query.ColumnRef;
import types.JavaType;
import types.SQLtype;
import types.TypeUtil;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * Evaluates a given expression on each row of a source table and
 * stores results in newly created column.
 *
 * @author Ziyun Wei
 */
public class ParallelMapRows {
    /**
     * Creates new column that contains one value for
     * each row in the source relation.
     *
     * @param sourceRel     we iterate over rows of this source relation
     * @param expression    each source row is mapped using this expression
     * @param columnMapping maps query to database columns
     * @param aggMapping    maps SQL aggregate expressions to columns
     * @param groupRef      optionally assign each row to a group -
     *                      result contains one row per group if specified.
     * @param nrGroups      specifies the number of groups (not used if
     *                      no group reference is specified).
     * @param targetRef     store results in this target column
     * @throws Exception
     */
    public static void execute(String sourceRel,
                               ExpressionInfo expression,
                               Map<ColumnRef, ColumnRef> columnMapping,
                               Map<String, ColumnRef> aggMapping,
                               Map<Group, Group> groupMapping,
                               ColumnRef groupRef,
                               int nrGroups,
                               ColumnRef targetRef) throws Exception {
        // Do we map to groups?
        boolean groupBy = groupRef != null;
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
        int outCard = groupBy ? nrGroups : inCard;

        // Create result data and load into buffer
        switch (jResultType) {
            case INT: {
                // Compile mapping expression
                ExpressionCompiler unaryCompiler = new ExpressionCompiler(
                        expression, columnMapping, null, aggMapping,
                        EvaluatorType.UNARY_INT);
                expression.finalExpression.accept(unaryCompiler);
                // Generate result data and store in buffer
                IntData intResult = new IntData(outCard);
                if (groupBy && outCard < 0) {
                    intResult.isNull.set(0, outCard - 1);
                }
                BufferManager.colToData.put(targetRef, intResult);
                // Iterate over source table and store results
                UnaryIntEval unaryIntEval = unaryCompiler.getUnaryIntEval();
                List<RowRange> batches = OperatorUtils.split(inCard);
                int nrBatches = batches.size();
                IntStream.range(0, nrBatches).parallel().forEach(b -> {
                    RowRange range = batches.get(b);
                    int start = range.firstTuple;
                    int last = range.lastTuple;
                    int[] rowResult = new int[1];
                    for (int rid = start; rid <= last; rid++) {
                        boolean notNull = unaryIntEval.evaluate(rid, rowResult);
                        intResult.data[rid] = rowResult[0];
                    }
                });
            }
            break;
            case LONG: {
                // Compile mapping expression
                ExpressionCompiler unaryCompiler = new ExpressionCompiler(
                        expression, columnMapping, null, aggMapping,
                        EvaluatorType.UNARY_LONG);
                expression.finalExpression.accept(unaryCompiler);
                UnaryLongEval unaryLongEval = unaryCompiler.getUnaryLongEval();
                // Generate result data and store in buffer
                LongData longResult = new LongData(outCard);
                if (groupBy && outCard < 0) {
                    longResult.isNull.set(0, outCard - 1);
                }
                BufferManager.colToData.put(targetRef, longResult);
                // Iterate over source table and store results
                List<RowRange> batches = OperatorUtils.split(inCard);
                int nrBatches = batches.size();
                IntStream.range(0, inCard).parallel().forEach(e -> {
                    long[] rowResult = new long[1];
                    boolean notNull = unaryLongEval.evaluate(e, rowResult);
                    longResult.data[e] = rowResult[0];
                });
            }
            break;
            case DOUBLE: {
                // Compile mapping expression
                ExpressionCompiler unaryCompiler = new ExpressionCompiler(
                        expression, columnMapping, null, aggMapping,
                        EvaluatorType.UNARY_DOUBLE);
                expression.finalExpression.accept(unaryCompiler);
                UnaryDoubleEval unaryDoubleEval = unaryCompiler.getUnaryDoubleEval();
                // Generate result data and store in buffer
                DoubleData doubleResult = new DoubleData(outCard);
                if (groupBy && outCard < 0) {
                    doubleResult.isNull.set(0, outCard - 1);
                }
                BufferManager.colToData.put(targetRef, doubleResult);
                // Iterate over source table and store results
                List<RowRange> batches = OperatorUtils.split(inCard);
                int nrBatches = batches.size();
                IntStream.range(0, nrBatches).parallel().forEach(b -> {
                    RowRange range = batches.get(b);
                    int start = range.firstTuple;
                    int last = range.lastTuple;
                    double[] rowResult = new double[1];
                    for (int rid = start; rid <= last; rid++) {
                        boolean notNull = unaryDoubleEval.evaluate(rid, rowResult);
                        doubleResult.data[rid] = rowResult[0];
                    }
                });
            }
            break;
            case STRING: {
                // Compile mapping expression
                ExpressionCompiler unaryCompiler = new ExpressionCompiler(
                        expression, columnMapping, null, aggMapping,
                        EvaluatorType.UNARY_STRING);
                expression.finalExpression.accept(unaryCompiler);
                UnaryStringEval unaryStringEval = unaryCompiler.getUnaryStringEval();
                // Generate result data and store in buffer
                StringData stringResult = new StringData(outCard);
                if (groupBy && outCard < 0) {
                    stringResult.isNull.set(0, outCard - 1);
                }
                BufferManager.colToData.put(targetRef, stringResult);
                // Iterate over source table and store results
                List<RowRange> batches = OperatorUtils.split(inCard);
                int nrBatches = batches.size();
                IntStream.range(0, inCard).parallel().forEach(e -> {
                    String[] rowResult = new String[1];
                    boolean notNull = unaryStringEval.evaluate(e, rowResult);
                    stringResult.data[e] = rowResult[0];
                });
            }
            break;
        }
        // Update catalog statistics
        CatalogManager.updateStats(targetTable);
    }
    /**
     * Creates new column that contains one value for
     * each row in the source relation.
     *
     * @param sourceRel     we iterate over rows of this source relation
     * @param expression    each source row is mapped using this expression
     *                      no group reference is specified).
     * @param targetRef     store results in this target column
     * @throws Exception
     */
    public static void executeWithIndex(String sourceRel, ExpressionInfo expression,
                                        Index index, ColumnRef targetRef) throws Exception {
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
        OperationTest operationTest = new OperationTest();
        expression.finalExpression.accept(operationTest);
        OperationNode operationNode = operationTest.operationNodes.pop();
        // Check more mappings
        OperationNode evaluator = operationNode.operator == Operator.Variable ? null : operationNode;
        int[] positions = index.positions;
        int[] gids = index.groupForRows;
        int outCard = gids.length;
        // Create result data and load into buffer
        switch (jResultType) {
            case INT: {
                // Generate result data and store in buffer
                IntData intResult = new IntData(outCard);
//                intResult.isNull.set(0, outCard - 1);
                BufferManager.colToData.put(targetRef, intResult);
                IntStream.range(0, gids.length).parallel().forEach(gid -> {
                    int pos = index.groupForRows[gid];
                    int rid = positions[pos + 1];
                    int data = ((DefaultIntIndex)index).intData.data[rid];
                    if (evaluator != null) {
                        data = (int) evaluator.evaluate(data);
                    }
                    intResult.data[gid] = data;
                });
            }
            break;
            case DOUBLE: {
                // Generate result data and store in buffer
                DoubleData doubleResult = new DoubleData(outCard);
                doubleResult.isNull.set(0, outCard - 1);
                BufferManager.colToData.put(targetRef, doubleResult);
                IntStream.range(0, gids.length).parallel().forEach(gid -> {
                    int pos = index.groupForRows[gid];
                    int rid = positions[pos + 1];
                    double data = ((DoubleIndex)index).doubleData.data[rid];
                    if (evaluator != null) {
                        data = evaluator.evaluate(data);
                    }
                    doubleResult.data[gid] = data;
                });
            }
            break;
        }
        // Update catalog statistics
        CatalogManager.updateStats(targetTable);
    }
}
