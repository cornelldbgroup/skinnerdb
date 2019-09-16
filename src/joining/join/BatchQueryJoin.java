package joining.join;

import catalog.CatalogManager;
import config.LoggingConfig;
import config.PreConfig;
import expressions.ExpressionInfo;
import expressions.compilation.EvaluatorType;
import expressions.compilation.ExpressionCompiler;
import expressions.compilation.KnaryBoolEval;
import joining.plan.LeftDeepPlan;
import joining.result.JoinResult;
import multiquery.GlobalContext;
import net.sf.jsqlparser.expression.Expression;
import preprocessing.Context;
import query.QueryInfo;
import statistics.JoinStats;
import utils.Pair;

import java.awt.*;
import java.util.*;
import java.util.List;

public class BatchQueryJoin {

    public final static float limitPrecent = 0.05f;
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
    public final Map<int[], LeftDeepPlan>[] planCache;
    /**
     * The query for which join orders are evaluated.
     */
    protected final QueryInfo[] queries;
    /**
     * Summarizes pre-processing steps.
     */
    protected final Context[] preSummaries;
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

    public int[][] orders;

    public HashMap<Integer, List<Integer>>[] batchGroups;

    public HashMap<Integer, Integer>[] progress;


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
        this.progress = new HashMap[nrQueries];
        this.unaryPreds = new KnaryBoolEval[nrQueries][];
        int i = 0;
        for (QueryInfo query : queries) {
            planCache[i] = new HashMap<int[], LeftDeepPlan>();
            cardinalities[i] = new int[query.nrJoined];
            predToEvals[i] = new HashMap<>();
            progress[i] = new HashMap<>();
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
        System.out.println("ssss" + preds.size());
        for (KnaryBoolEval pred : preds) {
            System.out.println("pred" + pred.toString());
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
    int proposeNext(int[] cardinality, List<JoinIndexWrapper> indexWrappers,
                    int curTable, int[] tupleIndices) {
        if (indexWrappers.isEmpty()) {
            return tupleIndices[curTable] + 1;
        }
        int max = -1;
        for (JoinIndexWrapper wrapper : indexWrappers) {
            int nextRaw = wrapper.nextIndex(tupleIndices);
            int next = nextRaw < 0 ? cardinality[curTable] : nextRaw;
            max = Math.max(max, next);
        }
        if (max < 0) {
            System.out.println(Arrays.toString(tupleIndices));
            System.out.println(indexWrappers.toString());
        }
        return max;
    }

    public double[] execute(int[][] orders, HashMap<Integer, List<Integer>>[] batchGroups, int startQuery) throws Exception {
        //LeftDeepPlan[] plans = new LeftDeepPlan[nrQueries];
        for (int i = 0; i < nrQueries; i++) {
            int realIdx = (startQuery + i) % nrQueries;
            int[] order = orders[realIdx];
            //JoinOrder joinOrder = new JoinOrder(order);
            LeftDeepPlan plan = planCache[realIdx].get(order);
            if (plan == null) {
                plan = new LeftDeepPlan(queries[realIdx], preSummaries[realIdx], predToEvals[realIdx], order);
                planCache[realIdx].put(order, plan);
            }
            //plans[i] = plan;
        }

        this.orders = orders;
        this.batchGroups = batchGroups;
        boolean[] isVisit = new boolean[nrQueries];
        int[] nrTuples = new int[nrQueries];
        for (int i = 0; i < batchGroups.length; i++) {
            int executedQuery = (startQuery + i) % nrQueries;
            //if(batchGroup != null) {
            if (!isVisit[executedQuery]) {
                int[] order = orders[executedQuery];
                int nrTable = order.length;
                int[] tupleIndices = new int[nrTable];
                int firstTable = order[0];
                int firstTableStartIdx = progress[executedQuery].containsKey(firstTable) ? progress[executedQuery].get(firstTable) : 0;
                tupleIndices[firstTable] = firstTableStartIdx;
                reuseJoin(executedQuery, 0, nrTable, tupleIndices, firstTableStartIdx, isVisit, nrTuples);
            }
            //}
        }
        for (int i = 0; i < nrQueries; i++) {
            int firstTable = orders[i][0];
            int currentProgress = (int) (cardinalities[i][firstTable] * limitPrecent);
            if (progress[i].containsKey(firstTable)) {
                int totalProgress = progress[i].get(firstTable) + currentProgress;
                progress[i].put(firstTable, totalProgress);
                if (totalProgress >= cardinalities[i][firstTable])
                    GlobalContext.queryStatus[i] = true;
            } else
                progress[i].put(firstTable, currentProgress);
        }

        double[] rewards = new double[nrQueries];
        for (int i = 0; i < nrQueries; i++) {
            rewards[i] = rewardFun(nrTuples[i], i, orders[i].length);
        }

        return rewards;
    }

    double rewardFun(int nrTuple, int queryNum, int nrTable) {
        long totalCard = 0;
        for (int i = 0; i < nrTable; i++) {
            totalCard += cardinalities[queryNum][i];
        }
        return totalCard / nrTuple;
    }

    private void reuseJoin(int queryNum, int startIdx, int endIdx, int[] tupleIndices, int firstTableStartIdx, boolean[] isVisit, int[] nrTuples) {
        //join table
        isVisit[queryNum] = true;
        int[] cardinality = this.cardinalities[queryNum];
        int[] order = orders[queryNum];
        //int[] offsets = null;
        //int[] tupleIndices = new int[order.length];
        HashMap<Integer, Integer> progressCurrentQuery = progress[queryNum];

        LeftDeepPlan plan = planCache[queryNum].get(order);
        List<List<KnaryBoolEval>> applicablePreds = plan.applicablePreds;
        List<List<JoinIndexWrapper>> joinIndices = plan.joinIndices;
        KnaryBoolEval[] unarys = unaryPreds[queryNum];
        HashMap<Integer, List<Integer>> currentBatchGroup = batchGroups[queryNum];

//        int firstTable = order[0];
//        int firstTableStartIdx = progressCurrentQuery.containsKey(firstTable) ? progressCurrentQuery.get(firstTable) : 0;
//        tupleIndices[firstTable] = firstTableStartIdx;

        int firstTableEndIdx = Math.min(firstTableStartIdx + (int) (cardinality[order[0]] * limitPrecent), cardinality[order[0]]);


        //next table
        int joinIndex = startIdx;
        int[] commonPrefixReward = new int[order.length];
        while (joinIndex >= startIdx && joinIndex < endIdx && tupleIndices[0] < firstTableEndIdx) {

            commonPrefixReward[joinIndex]++;
            nrTuples[queryNum]++;

            int nextTable = order[joinIndex];
            int nextCardinality = cardinality[nextTable];
            //System.out.println("index:"+joinIndex+", next table:"+nextTable);
            // Integrate table offset
            //tupleIndices[nextTable] = Math.max(offsets[nextTable], tupleIndices[nextTable]);

            // Evaluate all applicable predicates on joined tuples
            KnaryBoolEval unaryPred = unarys[nextTable];
            System.out.println("queryNum:" + queryNum);
            System.out.println("card:" + nextCardinality);
            System.out.println("join len:" + joinIndex);
            System.out.println("tuples" + Arrays.toString(tupleIndices));
            System.out.println("apps size" + applicablePreds.size());
            if ((PreConfig.PRE_FILTER || unaryPred == null ||
                    unaryPred.evaluate(tupleIndices) > 0) &&
                    evaluateAll(applicablePreds.get(joinIndex), tupleIndices)) {
                //++JoinStats.nrTuples;

                if (currentBatchGroup != null && currentBatchGroup.containsKey(joinIndex)) {
                    //int[] reusedQueries = currentBatchGroup.get(joinIndex).getSecond();
                    for (Integer reusedQuery : currentBatchGroup.get(joinIndex)) {
                        int[] reusedQueryOrder = orders[reusedQuery];
                        int nrReuseQueryTable = reusedQueryOrder.length;
                        //System.out.println(Arrays.toString(reusedQueryOrder));
                        int[] reusedTupleIndices = new int[nrReuseQueryTable];
                        for (int i = 0; i < joinIndex; i++) {
                            int reusedTable = reusedQueryOrder[i];
                            reusedTupleIndices[reusedTable] = tupleIndices[order[i]];
                            nrTuples[reusedTable] += commonPrefixReward[i];
                        }

                        //System.arraycopy(tupleIndices, 0, reusedTupleIndices, 0, joinIndex);

                        //                    LeftDeepPlan plan = planCache[reusedQuery].get(reusedQueryOrder);
                        //                    List<List<KnaryBoolEval>> reuseApplicablePreds = plan.applicablePreds;
                        //                    List<List<JoinIndexWrapper>> reuseJoinIndices = plan.joinIndices;
                        //                    reuseJoin(reusedQueryOrder, cardinalities[reusedQuery], offsets, reusedTupleIndices, reuseJoinIndices, reuseApplicablePreds
                        //                            , unaryPreds[reusedQuery], reuseJoinIndex, nrReuseQueryTable, batchGroups[reusedQuery]);


                        reuseJoin(reusedQuery, joinIndex, nrReuseQueryTable, reusedTupleIndices, firstTableStartIdx, isVisit, nrTuples);
                    }
                }

                // Do we have a complete result row?
                if (joinIndex == endIdx - 1) {
                    // Complete result row -> add to result
                    result[queryNum].add(tupleIndices);

                    tupleIndices[nextTable] = proposeNext(cardinality, joinIndices.get(joinIndex), nextTable, tupleIndices);
                    // Have reached end of current table? -> we backtrack.
                    while (tupleIndices[nextTable] >= nextCardinality) {
                        tupleIndices[nextTable] = 0;
                        --joinIndex;
                        if (joinIndex < 0) {
                            break;
                        }
                        nextTable = order[joinIndex];
                        nextCardinality = cardinality[nextTable];
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
                tupleIndices[nextTable] = proposeNext(cardinality, joinIndices.get(joinIndex), nextTable, tupleIndices);
                // Have reached end of current table? -> we backtrack.
                while (tupleIndices[nextTable] >= nextCardinality) {
                    tupleIndices[nextTable] = 0;
                    --joinIndex;
                    if (joinIndex < 0) {
                        break;
                    }
                    nextTable = order[joinIndex];
                    nextCardinality = cardinality[nextTable];
                    tupleIndices[nextTable] += 1;
                }
            }

        }
    }

}
