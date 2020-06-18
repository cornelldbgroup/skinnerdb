package joining.join;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import catalog.CatalogManager;
import config.JoinConfig;
import config.LoggingConfig;
import expressions.ExpressionInfo;
import expressions.compilation.EvaluatorType;
import expressions.compilation.ExpressionCompiler;
import expressions.compilation.KnaryBoolEval;
import joining.result.JoinResult;
import net.sf.jsqlparser.expression.Expression;
import preprocessing.Context;
import query.QueryInfo;

/**
 * A multi-way join operator that executes joins in small
 * episodes, using for each episode a newly specified join
 * order. Collects result tuples over different episodes
 * and contains finally a complete join result.
 * 
 * @author immanueltrummer
 *
 */
public abstract class MultiWayJoin {
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
     * Contains after each invocation the delta of the tuple
     * indices when comparing start state and final state.
     */
    public final int[] tupleIndexDelta;
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
     * Number of steps per episode.
     */
    public final int budget;
    /**
     * This constructor only serves for testing purposes.
     * It initializes most field to null pointers.
     * 
     * @param query			query to test
     * @throws Exception
     */
    public MultiWayJoin(QueryInfo query) throws Exception {
    	this.query = query;
    	this.nrJoined = query.nrJoined;
    	this.preSummary = null;
    	this.cardinalities = null;
    	this.tupleIndexDelta = null;
    	this.result = null;
    	this.budget = 0;
    	predToEval = null;
    }
    /**
     * Initializes join operator for given query
     * and initialize new join result.
     * 
     * @param query			query to process
     * @param preSummary	summarizes pre-processing steps
     * @param budget		budget per episode
     */
    public MultiWayJoin(QueryInfo query, Context preSummary, int budget) throws Exception {
        this.query = query;
        this.nrJoined = query.nrJoined;
        this.preSummary = preSummary;
        this.budget = budget;
        // Retrieve table cardinalities
        this.cardinalities = new int[nrJoined];
        this.tupleIndexDelta = new int[nrJoined];
        for (Entry<String,Integer> entry : 
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
        for (ExpressionInfo predInfo : query.wherePredicates) {
    		// Log predicate compilation if enabled
    		if (LoggingConfig.MAX_JOIN_LOGS>0) {
    			System.out.println("Compiling predicate " + predInfo + " ...");
    		}
    		// Compile predicate and store in lookup table
        	Expression pred = predInfo.finalExpression;
        	ExpressionCompiler compiler = new ExpressionCompiler(predInfo, 
        			preSummary.columnMapping, query.aliasToIndex, null,
        			EvaluatorType.KARY_BOOLEAN);
        	predInfo.finalExpression.accept(compiler);
        	KnaryBoolEval boolEval = (KnaryBoolEval)compiler.getBoolEval();
        	predToEval.put(pred, boolEval);        		
        }
    }

    /**
     * Calculates reward for progress during one invocation.
     *
     * @param joinOrder			join order followed
     * @param tupleIndexDelta	difference in tuple indices
     * @param tableOffsets		table offsets (number of tuples fully processed)
     * @param nrResultTuples	number of results found in the learning sample
     * @return					reward between 0 and 1, proportional to progress
     */
    double reward(int[] joinOrder, int[] tupleIndexDelta, int[] tableOffsets, int nrResultTuples) {
        double progress = 0;
        double weight = 1;
        for (int pos = 0; pos < nrJoined; ++pos) {
            // Scale down weight by cardinality of current table
            int curTable = joinOrder[pos];
            int remainingCard = cardinalities[curTable] -
                    (tableOffsets[curTable]);
            //int remainingCard = cardinalities[curTable];
            weight *= 1.0 / remainingCard;
            // Fully processed tuples from this table
            progress += tupleIndexDelta[curTable] * weight;
        }
        return JoinConfig.INPUT_REWARD_WEIGHT * progress +
                JoinConfig.OUTPUT_REWARD_WEIGHT * nrResultTuples / (double)budget;
    }
    /**
     * Executes given join order for a given number of steps.
     * 
     * @param order		execute this join order
     * @return			reward (higher reward means faster progress)
     * @throws Exception 
     */
    public abstract double execute(int[] order) throws Exception;
    /**
     * Returns true iff a complete join result was generated.
     * 
     * @return	true iff query processing is finished
     */
    public abstract boolean isFinished();
}