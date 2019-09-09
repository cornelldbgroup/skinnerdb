package joining.join;

import catalog.CatalogManager;
import config.LoggingConfig;
import expressions.ExpressionInfo;
import expressions.compilation.EvaluatorType;
import expressions.compilation.ExpressionCompiler;
import expressions.compilation.KnaryBoolEval;
import joining.plan.JoinOrder;
import joining.plan.LeftDeepPlan;
import joining.progress.State;
import joining.result.JoinResult;
import net.sf.jsqlparser.expression.Expression;
import preprocessing.Context;
import query.QueryInfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class BatchQueryJoin {

    /**
     * The query for which join orders are evaluated.
     */
    protected final QueryInfo[] queries;

    /**
     * Summarizes pre-processing steps.
     */
    protected final Context[] preSummaries;

    /**
     * At i-th position: cardinality of i-th joined table
     * (after pre-processing).
     */
    public final int[][] cardinalities;

    /**
     * Collects result tuples and contains
     * finally a complete result.
     */
    public final JoinResult[] result;

    /**
     *
     */
    public int nrQueries;

    /**
     * Initializes join operator for given query
     * and initialize new join result.
     *
     * @param queries			query to process
     * @param preSummaries	summarizes pre-processing steps
     */
    public BatchQueryJoin(QueryInfo[] queries, Context[] preSummaries) throws Exception {
        int nrQueries = queries.length;
        this.queries = queries;
        this.preSummaries = preSummaries;
        this.nrQueries = nrQueries;
        // Retrieve table cardinalities
        this.cardinalities = new int[nrQueries][];
        this.result = new JoinResult[nrQueries];
        int i = 0;
        for(QueryInfo query : queries) {
            cardinalities[i] = new int[query.nrJoined];
            for (Map.Entry<String, Integer> entry :
                    query.aliasToIndex.entrySet()) {
                String alias = entry.getKey();
                String table = preSummaries[i].aliasToFiltered.get(alias);
                int index = entry.getValue();
                int cardinality = CatalogManager.getCardinality(table);
                cardinalities[i][index] = cardinality;
            }
            this.result[i] = new JoinResult(query.nrJoined);
            i++;
        }

        // Compile predicates

    }

    public double execute(int[][] orders, ArrayList[] batchGroups, int startQuery) throws Exception {

        for(int i = 0; i < nrQueries; i++) {
            int[] joinOrder = orders[i];
            int nrTable = joinOrder.length;
            int[] tupleIndices = new int[nrTable];

            for (int tableCtr = 0; tableCtr < nrTables; ++tableCtr) {
                tupleIndices[tableCtr] = state.tupleIndices[tableCtr];
            }



        }



        // Treat special case: at least one input relation is empty
        for (int tableCtr=0; tableCtr<nrJoined; ++tableCtr) {
            if (cardinalities[tableCtr]==0) {
                tracker.isFinished = true;
                return 1;
            }
        }
        // Lookup or generate left-deep query plan
        JoinOrder joinOrder = new JoinOrder(order);
        LeftDeepPlan plan = planCache.get(joinOrder);
        if (plan == null) {
            plan = new LeftDeepPlan(query, preSummary, predToEval, order);
            planCache.put(joinOrder, plan);
        }
        log(plan.toString());
        // Execute from starting state, save progress, return progress
        State state = tracker.continueFrom(joinOrder);
        //logger.println("Start state " + state);
        int[] offsets = tracker.tableOffset;
        executeWithBudget(plan, state, offsets);
        double reward = reward(joinOrder.order,
                tupleIndexDelta, offsets);
        tracker.updateProgress(joinOrder, state);
        return reward;
    }

}
