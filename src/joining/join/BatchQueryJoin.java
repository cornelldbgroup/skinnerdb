package joining.join;

import catalog.CatalogManager;
import config.LoggingConfig;
import config.PreConfig;
import expressions.ExpressionInfo;
import expressions.compilation.EvaluatorType;
import expressions.compilation.ExpressionCompiler;
import expressions.compilation.KnaryBoolEval;
import joining.plan.JoinOrder;
import joining.plan.LeftDeepPlan;
import joining.result.JoinResult;
import net.sf.jsqlparser.expression.Expression;
import preprocessing.Context;
import query.QueryInfo;
import statistics.JoinStats;

import java.util.*;

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
     * Avoids redundant planning work by storing left deep plans.
     */
    public final Map<JoinOrder, LeftDeepPlan>[] planCache;

    /**
     * Maps non-equi join predicates to compiled evaluators.
     */
    protected final Map<Expression, KnaryBoolEval>[] predToEvals;

    /**
     * Associates each table index with unary predicates.
     */
    final KnaryBoolEval[][] unaryPreds;

    /**
     *
     */
    public int nrQueries;

    public final static int limit = 1000;

    /**
     * Initializes join operator for given query
     * and initialize new join result.
     *
     * @param queries      query to process
     * @param preSummaries summarizes pre-processing steps
     */
    public BatchQueryJoin(QueryInfo[] queries, Context[] preSummaries) throws Exception {
        int nrQueries = queries.length;
        this.queries = queries;
        this.preSummaries = preSummaries;
        this.nrQueries = nrQueries;
        // Retrieve table cardinalities
        this.cardinalities = new int[nrQueries][];
        this.result = new JoinResult[nrQueries];
        this.planCache = new Map[nrQueries];
        this.predToEvals = new HashMap[nrQueries];
        this.unaryPreds = new KnaryBoolEval[nrQueries][];
        int i = 0;
        for (QueryInfo query : queries) {
            planCache[i] = new HashMap<JoinOrder, LeftDeepPlan>();
            cardinalities[i] = new int[query.nrJoined];
            predToEvals[i] = new HashMap<>();
            for (Map.Entry<String, Integer> entry :
                    query.aliasToIndex.entrySet()) {
                String alias = entry.getKey();
                String table = preSummaries[i].aliasToFiltered.get(alias);
                int index = entry.getValue();
                int cardinality = CatalogManager.getCardinality(table);
                cardinalities[i][index] = cardinality;
            }

            for (ExpressionInfo predInfo : query.wherePredicates) {
                // Log predicate compilation if enabled
                if (LoggingConfig.MAX_JOIN_LOGS > 0) {
                    System.out.println("Compiling predicate " + predInfo + " ...");
                }
                // Compile predicate and store in lookup table
                Expression pred = predInfo.finalExpression;
                ExpressionCompiler compiler = new ExpressionCompiler(predInfo,
                        preSummaries[i].columnMapping, query.aliasToIndex, null,
                        EvaluatorType.KARY_BOOLEAN);
                predInfo.finalExpression.accept(compiler);
                KnaryBoolEval boolEval = (KnaryBoolEval) compiler.getBoolEval();
                predToEvals[i] = new HashMap<>();
                predToEvals[i].put(pred, boolEval);
            }

            this.unaryPreds[i] = new KnaryBoolEval[query.nrJoined];
            for (ExpressionInfo unaryExpr : query.wherePredicates) {
                // Is it a unary predicate?
                if (unaryExpr.aliasIdxMentioned.size() == 1) {
                    // (Exactly one table mentioned for unary predicates)
                    int aliasIdx = unaryExpr.aliasIdxMentioned.iterator().next();
                    KnaryBoolEval eval = predToEvals[i].get(unaryExpr.finalExpression);
                    unaryPreds[i][aliasIdx] = eval;
                }
            }

            this.result[i] = new JoinResult(query.nrJoined);
            i++;
        }

        // Compile predicates
    }

    /**
     * Evaluates list of given predicates on current tuple
     * indices and returns true iff all predicates evaluate
     * to true.
     *
     * @param preds        predicates to evaluate
     * @param tupleIndices (partial) tuples
     * @return true iff all predicates evaluate to true
     */
    boolean evaluateAll(List<KnaryBoolEval> preds, int[] tupleIndices) {
        for (KnaryBoolEval pred : preds) {
            if (pred.evaluate(tupleIndices) <= 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * Propose next tuple index to consider, based on a set of
     * indices on the join column.
     *
     * @param indexWrappers list of join index wrappers
     * @param tupleIndices  current tuple indices
     * @return next proposed tuple index
     */
    int proposeNext(int query, List<JoinIndexWrapper> indexWrappers,
                    int curTable, int[] tupleIndices) {
        if (indexWrappers.isEmpty()) {
            return tupleIndices[curTable] + 1;
        }
        int max = -1;
        for (JoinIndexWrapper wrapper : indexWrappers) {
            int nextRaw = wrapper.nextIndex(tupleIndices);
            int next = nextRaw < 0 ? cardinalities[query][curTable] : nextRaw;
            max = Math.max(max, next);
        }
        if (max < 0) {
            System.out.println(Arrays.toString(tupleIndices));
            System.out.println(indexWrappers.toString());
        }
        return max;
    }

    public double execute(int[][] orders, ArrayList[] batchGroup, int startQuery) throws Exception {
        HashMap<Integer, List<Integer>>[] batchGroups = new HashMap[nrQueries];
        LeftDeepPlan[] plans = new LeftDeepPlan[nrQueries];
        for (int i = 0; i < nrQueries; i++) {
            int[] order = orders[i];
            JoinOrder joinOrder = new JoinOrder(order);
            LeftDeepPlan plan = planCache[i].get(joinOrder);
            if (plan == null) {
                plan = new LeftDeepPlan(queries[i], preSummaries[i], predToEvals[i], order);
                planCache[i].put(joinOrder, plan);
            }
            plans[i] = plan;
        }

        for (int i = 0; i < nrQueries; i++) {
            int[] order = orders[i];
            int nrTable = order.length;
            int[] tupleIndices = new int[nrTable];
            int[] offsets = new int[nrTable];
            LeftDeepPlan plan = plans[i];
            List<List<KnaryBoolEval>> applicablePreds = plan.applicablePreds;
            List<List<JoinIndexWrapper>> joinIndices = plan.joinIndices;
            HashMap<Integer, List<Integer>> currentBatchGroup = batchGroups[i];
            for (int start = 0; start < nrTable; start++) {
                //should recovery table offsets
                int joinIndex = 0;
                while (offsets[0] < limit) {
                    //next table
                    int nextTable = plan.joinOrder.order[joinIndex];
                    int nextCardinality = cardinalities[i][nextTable];
                    //System.out.println("index:"+joinIndex+", next table:"+nextTable);
                    // Integrate table offset
                    tupleIndices[nextTable] = Math.max(
                            offsets[nextTable], tupleIndices[nextTable]);
                    // Evaluate all applicable predicates on joined tuples
                    KnaryBoolEval unaryPred = unaryPreds[i][nextTable];
                    if ((PreConfig.PRE_FILTER || unaryPred == null ||
                            unaryPred.evaluate(tupleIndices) > 0) &&
                            evaluateAll(applicablePreds.get(joinIndex), tupleIndices)) {
                        ++JoinStats.nrTuples;
                        // Do we have a complete result row?
                        if (joinIndex == plan.joinOrder.order.length - 1) {
                            // Complete result row -> add to result
                            result[i].add(tupleIndices);
                            tupleIndices[nextTable] = proposeNext(i, joinIndices.get(joinIndex), nextTable, tupleIndices);
                            // Have reached end of current table? -> we backtrack.
                            while (tupleIndices[nextTable] >= nextCardinality) {
                                tupleIndices[nextTable] = 0;
                                --joinIndex;
                                if (joinIndex < 0) {
                                    break;
                                }
                                nextTable = plan.joinOrder.order[joinIndex];
                                nextCardinality = cardinalities[i][nextTable];
                                tupleIndices[nextTable] += 1;
                            }
                        } else {
                            // No complete result row -> complete further
                            joinIndex++;
                            //System.out.println("Current Join Index2:"+ joinIndex);
                        }
                    } else {
                        // At least one of applicable predicates evaluates to false -
                        // try next tuple in same table.
                        tupleIndices[nextTable] = proposeNext(i, joinIndices.get(joinIndex), nextTable, tupleIndices);
                        // Have reached end of current table? -> we backtrack.
                        while (tupleIndices[nextTable] >= nextCardinality) {
                            tupleIndices[nextTable] = 0;
                            --joinIndex;
                            if (joinIndex < 0) {
                                break;
                            }
                            nextTable = plan.joinOrder.order[joinIndex];
                            nextCardinality = cardinalities[i][nextTable];
                            tupleIndices[nextTable] += 1;
                        }
                    }
                }
            }
        }
        return 0;
    }

}
