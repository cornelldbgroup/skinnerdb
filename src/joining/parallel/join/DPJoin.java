package joining.parallel.join;

import catalog.CatalogManager;
import config.LoggingConfig;
import config.PreConfig;
import expressions.ExpressionInfo;
import expressions.compilation.EvaluatorType;
import expressions.compilation.ExpressionCompiler;
import expressions.compilation.KnaryBoolEval;
import joining.progress.State;
import joining.result.JoinResult;
import net.sf.jsqlparser.expression.Expression;
import joining.parallel.statistics.StatsInstance;
import predicate.NonEquiNode;
import preprocessing.Context;
import query.QueryInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A multi-way join operator that executes data parallelization joins in small
 * episodes, using for each episode a newly specified join
 * order. Collects result tuples over different episodes
 * and contains finally a complete join result.
 *
 * @author Ziyun Wei
 */
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
    protected final Map<Expression, NonEquiNode> predToEval;
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
     * Last large table.
     */
    public int largeTable;
    /**
     * Whether we have progress on the split table.
     */
    public boolean noProgressOnSplit;
    /**
     * Whether the thread is slowest one.
     */
    public boolean slowest;
    /**
     * Whether the thread is sharing plans.
     */
    public boolean isShared;
    /**
     * A list of logs.
     */
    public List<String> logs;
    /**
     * Number of episode.
     */
    public long roundCtr;
    /**
     * Slowest threads for each split table.
     */
    public int[] slowThreads;

    ExpressionCompiler compiler;
    KnaryBoolEval boolEval;


    /**
     * Initializes join operator for given query
     * and initialize new join result.
     *
     * @param query      query to process
     * @param preSummary summarizes pre-processing steps
     */
    public DPJoin(QueryInfo query, Context preSummary, int budget, int nrThreads, int tid,
                  Map<Expression, NonEquiNode> predToEval, Map<Expression, KnaryBoolEval> predToComp) throws Exception {
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
        for (Map.Entry<String, Integer> entry :
                query.aliasToIndex.entrySet()) {
            String alias = entry.getKey();
            String table = preSummary.aliasToFiltered.get(alias);
            int index = entry.getValue();
            int cardinality = CatalogManager.getCardinality(table);
            cardinalities[index] = cardinality;
        }
        this.result = new JoinResult(nrJoined);
        this.predToEval = predToEval;

//        ExpressionInfo predInfo = query.nonEquiJoinPreds.get(0);
//        compiler = new ExpressionCompiler(predInfo,
//                preSummary.columnMapping, query.aliasToIndex, null,
//                EvaluatorType.KARY_BOOLEAN);
//        predInfo.finalExpression.accept(compiler);
//        boolEval = (KnaryBoolEval)compiler.getBoolEval();

        if (!PreConfig.FILTER) {
            for (ExpressionInfo predInfo : query.wherePredicates) {
                // Compile predicate and store in lookup table
                if (predInfo.aliasIdxMentioned.size()==1) {
                    Expression pred = predInfo.finalExpression;
                    ExpressionCompiler compiler = new ExpressionCompiler(predInfo,
                            preSummary.columnMapping, query.aliasToIndex, null,
                            EvaluatorType.KARY_BOOLEAN);
                    predInfo.finalExpression.accept(compiler);
                    KnaryBoolEval boolEval = (KnaryBoolEval) compiler.getBoolEval();
                    predToComp.put(pred, boolEval);
                }
            }
        }
    }

    /**
     * Executes given join order for a given number of steps.
     *
     * @param order execute this join order
     * @throws Exception
     * @return reward (higher reward means faster progress)
     */
    public abstract double execute(int[] order, int splitTable, int roundCtr) throws Exception;

    /**
     * Returns true iff a complete join result was generated.
     *
     * @return true iff query processing is finished
     */
    public abstract boolean isFinished();

    /**
     * Get the first table that has more than 1 row.
     *
     * @param order Join order.
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

    /**
     * Put a log sentence into a list of logs.
     *
     * @param line log candidate
     */
    public void writeLog(String line) {
        if (LoggingConfig.PARALLEL_JOIN_VERBOSE) {
            logs.add(line);
        }
    }
}
