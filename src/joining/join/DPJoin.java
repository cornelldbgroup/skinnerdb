package joining.join;

import catalog.CatalogManager;
import config.LoggingConfig;
import expressions.ExpressionInfo;
import expressions.compilation.EvaluatorType;
import expressions.compilation.ExpressionCompiler;
import expressions.compilation.KnaryBoolEval;
import joining.progress.State;
import joining.result.JoinResult;
import net.sf.jsqlparser.expression.Expression;
import preprocessing.Context;
import query.QueryInfo;
import statistics.StatsInstance;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class DPJoin {
    /**
     * Number of steps per episode.
     */
    public int budget;
    /**
     * The query for which join orders are evaluated.
     */
    protected final QueryInfo query;
    /**
     * Number of tables joined by query.
     */
    protected final int nrJoined;
    /**
     * At i-th position: cardinality of i-th joined table
     * (after pre-processing).
     */
    public final int[] cardinalities;
    /**
     * Summarizes pre-processing steps.
     */
    protected final Context preSummary;
    /**
     * Maps non-equi join predicates to compiled evaluators.
     */
    protected final Map<Expression, KnaryBoolEval> predToEval;
    /**
     * Collects result tuples and contains
     * finally a complete result.
     */
    public final JoinResult result;
    /**
     * Number of working threads.
     */
    public final int nrThreads;
    /**
     * ID of the join operator.
     */
    public final int tid;
    /**
     * Instance of statistics records.
     */
    public StatsInstance statsInstance;
    /**
     * Last state after a episode.
     */
    public State lastState;
    /**
     * Last split table.
     */
    public int lastTable;
    /**
     * A list of logs.
     */
    public List<String> logs;
    /**
     * Initializes join operator for given query
     * and initialize new join result.
     *
     * @param query			query to process
     * @param preSummary	summarizes pre-processing steps
     */
    public DPJoin(QueryInfo query, Context preSummary, int budget, int nrThreads, int tid) throws Exception {
        this.query = query;
        this.nrJoined = query.nrJoined;
        this.preSummary = preSummary;
        this.budget = budget;
        // Retrieve table cardinalities
        this.cardinalities = new int[nrJoined];
        this.nrThreads = nrThreads;
        this.tid = tid;
        this.statsInstance = new StatsInstance();
        this.logs = new ArrayList<>();
        for (Map.Entry<String,Integer> entry :
                query.aliasToIndex.entrySet()) {
            String alias = entry.getKey();
            String table = preSummary.aliasToFiltered.get(alias);
            int index = entry.getValue();
            int cardinality = CatalogManager.getCardinality(table);
            cardinalities[index] = cardinality;
        }
        this.result = new JoinResult(nrJoined);
        // Compile predicates
        predToEval = new HashMap<>();
//        for (ExpressionInfo predInfo : query.wherePredicates) {
//            // Log predicate compilation if enabled
//            if (LoggingConfig.MAX_JOIN_LOGS>0) {
//                System.out.println("Compiling predicate " + predInfo + " ...");
//            }
//            // Compile predicate and store in lookup table
//            Expression pred = predInfo.finalExpression;
//            ExpressionCompiler compiler = new ExpressionCompiler(predInfo,
//                    preSummary.columnMapping, query.aliasToIndex, null,
//                    EvaluatorType.KARY_BOOLEAN);
//            predInfo.finalExpression.accept(compiler);
//            KnaryBoolEval boolEval = (KnaryBoolEval)compiler.getBoolEval();
//            predToEval.put(pred, boolEval);
//        }
    }
    /**
     * Executes given join order for a given number of steps.
     *
     * @param order		execute this join order
     * @return			reward (higher reward means faster progress)
     * @throws Exception
     */
    public abstract double execute(int[] order, int splitTable, int roundCtr) throws Exception;
    /**
     * Returns true iff a complete join result was generated.
     *
     * @return	true iff query processing is finished
     */
    public abstract boolean isFinished();

    /**
     * Get the first table that has more than 1 row.
     * @param order     Join order.
     * @return
     */
    public int getFirstLargeTable(int[] order) {
        for (int table : order) {
            if (cardinalities[table] > 1) {
                return table;
            }
        }
        return order[0];
    }

    public void writeLog(String line) {
        if (LoggingConfig.PARALLEL_JOIN_VERBOSE) {
            logs.add(line);
        }
    }
}
