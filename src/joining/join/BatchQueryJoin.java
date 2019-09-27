package joining.join;

import catalog.CatalogManager;
import config.GeneralConfig;
import config.LoggingConfig;
import config.PreConfig;
import expressions.ExpressionInfo;
import expressions.compilation.EvaluatorType;
import expressions.compilation.ExpressionCompiler;
import expressions.compilation.KnaryBoolEval;
import joining.plan.JoinOrder;
import joining.plan.LeftDeepPlan;
import joining.progress.ProgressTracker;
import joining.progress.State;
import joining.result.JoinResult;
import multiquery.GlobalContext;
import net.sf.jsqlparser.expression.Expression;
import preprocessing.Context;
import query.QueryInfo;

import java.util.*;
import java.util.List;

public class BatchQueryJoin {

    //public final static float limitPrecent = 0.05f;
    //public final static int limit = 3;

    /**
     * Number of steps per episode.
     */
    public final int budget = 500;

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
    final Map<JoinOrder, LeftDeepPlan>[] planCache;
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

    public final long[] totalCardinality;

    public final int[] budgets;

    public final ProgressTracker[] tracker;

    private final int[][] startIndices;

    private final int[][] endIndices;

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
        this.totalCardinality = new long[nrQueries];
        this.budgets = new int[nrQueries];
        this.tracker = new ProgressTracker[nrQueries];
        this.startIndices = new int[nrQueries][];
        this.endIndices = new int[nrQueries][];
        int i = 0;
        for (QueryInfo query : queries) {
            planCache[i] = new HashMap<JoinOrder, LeftDeepPlan>();
            cardinalities[i] = new int[query.nrJoined];
            predToEvals[i] = new HashMap<>();
            progress[i] = new HashMap<>();
            totalCardinality[i] = 1;
            for (Map.Entry<String, Integer> entry :
                    query.aliasToIndex.entrySet()) {
                String alias = entry.getKey();
                String table = preSummaries[i].aliasToFiltered.get(alias);
                int index = entry.getValue();
                int cardinality = CatalogManager.getCardinality(table);
                cardinalities[i][index] = cardinality;
                totalCardinality[i] *= cardinality;
                if(totalCardinality[i] < 0)
                    totalCardinality[i] = 1000000000l;
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
            this.tracker[i] = new ProgressTracker(query.nrJoined, cardinalities[i]);
            this.startIndices[i] = new int[query.nrJoined];
            this.endIndices[i] = new int[query.nrJoined];
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
        //System.out.println("ssss" + preds.size());
        for (KnaryBoolEval pred : preds) {
            //System.out.println("pred" + pred.toString());
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
//        if(max > cardinality[curTable]) {
//            System.out.println("error");
//        }
//        if (max < 0) {
//            System.out.println(Arrays.toString(tupleIndices));
//            System.out.println(indexWrappers.toString());
//        }
        return max;
    }

    public double[] execute(int[][] orders, HashMap<Integer, List<Integer>>[] batchGroups, int startQuery) throws Exception {
        //LeftDeepPlan[] plans = new LeftDeepPlan[nrQueries];
        State[] states = new State[nrQueries];
        for (int i = 0; i < nrQueries; i++) {
            int realIdx = (startQuery + i) % nrQueries;
            int[] order = orders[realIdx];
            if(GlobalContext.queryStatus[realIdx])
                continue;
            //JoinOrder joinOrder = new JoinOrder(order);
            LeftDeepPlan plan = planCache[realIdx].get(new JoinOrder(order));
            if (plan == null) {
                plan = new LeftDeepPlan(queries[realIdx], preSummaries[realIdx], predToEvals[realIdx], order);
                planCache[realIdx].put(new JoinOrder(order), plan);
            }
            //plans[i] = plan;
            this.budgets[realIdx] = budget;
            states[realIdx] = tracker[realIdx].continueFrom(new JoinOrder(order));
            this.startIndices[realIdx] = Arrays.copyOf(states[realIdx].tupleIndices, states[realIdx].tupleIndices.length);
            this.endIndices[realIdx] = new int[order.length];
        }

        this.orders = orders;
        this.batchGroups = batchGroups;
        boolean[] isVisit = new boolean[nrQueries];
        int[] nrTuples = new int[nrQueries];
        for (int i = 0; i < batchGroups.length; i++) {
            int executedQuery = (startQuery + i) % nrQueries;
            //if(batchGroup != null) {
            if (!isVisit[executedQuery] && !GlobalContext.queryStatus[executedQuery]) {
                //int currentBudget = budget;
                int[] order = orders[executedQuery];
                int nrTable = order.length;
                //int[] tupleIndices = new int[nrTable];
                //int firstTable = order[0];
                //int firstTableStartIdx = progress[executedQuery].getOrDefault(firstTable, 0);
                //tupleIndices[firstTable] = firstTableStartIdx;

                reuseJoin(executedQuery, 0, nrTable, states[executedQuery].tupleIndices, isVisit, nrTuples, 0, states[executedQuery]);
            }
            //}
        }

//        for (int i = 0; i < nrQueries; i++) {
//            if(GlobalContext.queryStatus[i])
//                continue;
//            int firstTable = orders[i][0];
//            //int currentProgress = (int) Math.ceil((double) cardinalities[i][firstTable] * limitPrecent);
//            //int currentProgress = limit;
//            if (progress[i].containsKey(firstTable)) {
//                int totalProgress = progress[i].get(firstTable) + currentProgress;
//                progress[i].put(firstTable, totalProgress);
//                if (totalProgress >= cardinalities[i][firstTable])
//                    GlobalContext.queryStatus[i] = true;
//            } else
//                progress[i].put(firstTable, currentProgress);
//        }

        double[] rewards = new double[nrQueries];
        for (int i = 0; i < nrQueries; i++) {
            if(GlobalContext.queryStatus[i])
                continue;
//            System.out.println("startIndices:"+ Arrays.toString(startIndices));
//            System.out.println("endIndices:" + Arrays.toString(endIndices));
            GlobalContext.queryStatus[i] = tracker[i].isFinished;
            //rewards[i] = nrTuples[i] > 0 ? 1d / (double) nrTuples[i] : 0;
                    //(double) totalCardinality[i];
            //rewards[i] = rewardFun(nrTuples[i], i, orders[i].length);
            rewards[i] = reward(orders[i], startIndices[i], endIndices[i], cardinalities[i]);
        }
//        System.out.println(Arrays.toString(rewards));
        return rewards;
    }

//    double rewardFun(int nrTuple, int queryNum, int nrTable) {
//        long totalCard = 0;
//        for (int i = 0; i < nrTable; i++) {
//            totalCard += cardinalities[queryNum][i];
//        }
//        return totalCard / nrTuple;
//    }

    double reward(int[] joinOrder, int[] startIdx, int[] endIdx, int[] cardinality) {
        double progress = 0;
        double weight = 1;
        for (int pos=0; pos<joinOrder.length; ++pos) {
            // Scale down weight by cardinality of current table
            int curTable = joinOrder[pos];
            int remainingCard = cardinality[curTable] -
                    (endIdx[curTable]);
            //int remainingCard = cardinalities[curTable];
            weight *= 1.0 / remainingCard;
            // Fully processed tuples from this table
            progress += (endIdx[curTable] - startIdx[curTable]) * weight;
        }
        return progress;
    }

    private boolean reuseJoin(int queryNum, int startIdx, int endIdx, int[] tupleIndices, boolean[] isVisit, int[] nrTuples, int reuseBudget, State state) {
//        System.out.println("queryNum:" + queryNum + ", join order:" + Arrays.toString(orders[queryNum]));
        //join table
        isVisit[queryNum] = true;
        int[] cardinality = this.cardinalities[queryNum];
        int[] order = orders[queryNum];

//        System.out.println("Cardinality:" + Arrays.toString(cardinality));

        //int[] offsets = null;
        //int[] tupleIndices = new int[order.length];
        //HashMap<Integer, Integer> progressCurrentQuery = progress[queryNum];

        LeftDeepPlan plan = planCache[queryNum].get(new JoinOrder(order));
        List<List<KnaryBoolEval>> applicablePreds = plan.applicablePreds;
        List<List<JoinIndexWrapper>> joinIndices = plan.joinIndices;
        KnaryBoolEval[] unarys = unaryPreds[queryNum];
        HashMap<Integer, List<Integer>> currentBatchGroup = batchGroups[queryNum];

        int[] currentStartIndices = Arrays.copyOf(tupleIndices, tupleIndices.length);

//        int firstTable = order[0];
//        int firstTableStartIdx = progressCurrentQuery.containsKey(firstTable) ? progressCurrentQuery.get(firstTable) : 0;
//        tupleIndices[firstTable] = firstTableStartIdx;

        //int firstTableEndIdx = Math.min(firstTableStartIdx + (int) Math.ceil((double) cardinality[order[0]] * limitPrecent), cardinality[order[0]]);
        //int firstTableEndIdx = Math.max(firstTableStartIdx + limit, cardinality[order[0]]);

        //next table
        int joinIndex = startIdx;
        int[] reuseNrTuples = new int[order.length];
        //boolean reuseFinish = true;
        boolean[] reuseFinish = new boolean[order.length];
        Arrays.fill(reuseFinish, true);
        //int minReuseSize = 0;
//        int[] budgetEachTable = new int[order.length];
        while (joinIndex >= startIdx && joinIndex < endIdx && reuseFinish[joinIndex] && this.budgets[queryNum] > 0) {

            reuseNrTuples[joinIndex]++;
            nrTuples[queryNum]++;

            int nextTable = order[joinIndex];
            int nextCardinality = cardinality[nextTable];
//            System.out.println("index:"+joinIndex+", next table:"+nextTable);
            // Integrate table offset
            //tupleIndices[nextTable] = Math.max(offsets[nextTable], tupleIndices[nextTable]);

//            budgetEachTable[nextTable]++;

            // Evaluate all applicable predicates on joined tuples
            KnaryBoolEval unaryPred = unarys[nextTable];
//            System.out.println("queryNum:" + queryNum);
//            System.out.println("card:" + nextCardinality);
//            System.out.println("join len:" + joinIndex);
//            System.out.println("tuples" + Arrays.toString(tupleIndices));
//            System.out.println("apps size" + applicablePreds.size());
            if ((PreConfig.PRE_FILTER || unaryPred == null ||
                    unaryPred.evaluate(tupleIndices) > 0) &&
                    evaluateAll(applicablePreds.get(joinIndex), tupleIndices)) {
                //++JoinStats.nrTuples;

                if (currentBatchGroup != null && currentBatchGroup.containsKey(joinIndex + 1)) {
                    //int[] reusedQueries = currentBatchGroup.get(joinIndex).getSecond();
                    //minReuseSize = Math.min(minReuseSize, joinIndex);
                    for (Integer reusedQuery : currentBatchGroup.get(joinIndex + 1)) {
                        int[] reusedQueryOrder = orders[reusedQuery];
                        int nrReuseQueryTable = reusedQueryOrder.length;
                        //System.out.println(Arrays.toString(reusedQueryOrder));
                        State reuseState = tracker[reusedQuery].continueFrom(new JoinOrder(reusedQueryOrder));
                        int[] reusedTupleIndices = new int[nrReuseQueryTable];
                        int[] recoveryTupleIndices = reuseState.tupleIndices;
                        int totalReuseTuples = 0;
                        //reuseState.tupleIndices;

                        if(queryNum == GeneralConfig.testQuery || reusedQuery == GeneralConfig.testQuery) {
                            System.out.println("============");
                            System.out.println("parent query:" + queryNum);
                            System.out.println("reuse query:" + reusedQuery);
                            System.out.println("parent order:" + Arrays.toString(order));
                            System.out.println("child order:" + Arrays.toString(reusedQueryOrder));
                            System.out.println("parent indices:" + Arrays.toString(tupleIndices));
                            System.out.println("reused before indices:" + Arrays.toString(recoveryTupleIndices));
                        }

                        boolean tryReuse = true;
                        boolean isSameProgress = true;
                        //test whether we can reuse index
                        for (int i = 0; i <= joinIndex; i++) {
                            int reusedTable = reusedQueryOrder[i];
                            int reusedTableOffset = tupleIndices[order[i]];
                            isSameProgress &= (reusedTableOffset == recoveryTupleIndices[reusedTable]);
                            if(reusedTableOffset > recoveryTupleIndices[reusedTable]) {
                                break;
                            } else if(reusedTableOffset < recoveryTupleIndices[reusedTable]) {
                                tryReuse = false;
                                break;
                            }
                        }

                        if(tryReuse) {

                            if (isSameProgress) {
                                reusedTupleIndices = recoveryTupleIndices;
                            } else {
                                for (int i = 0; i <= joinIndex; i++) {
                                    int reusedTable = reusedQueryOrder[i];
                                    reusedTupleIndices[reusedTable] = tupleIndices[order[i]];
                                    nrTuples[reusedTable] += reuseNrTuples[i];
                                    totalReuseTuples += reuseNrTuples[i];
                                }
                            }


                            this.budgets[reusedQuery] -= (reuseBudget + totalReuseTuples);
                            reuseFinish[joinIndex] &= reuseJoin(reusedQuery, joinIndex + 1, nrReuseQueryTable, reusedTupleIndices, isVisit, nrTuples, reuseBudget + totalReuseTuples, reuseState);

                        }

//                        for (int i = 0; i <= joinIndex; i++) {
//                            int reusedTable = reusedQueryOrder[i];
//                            int reusedTableOffset = tupleIndices[order[i]];
////                            System.out.println("share idx:" + joinIndex);
////                            System.out.println("re order:" + Arrays.toString(reusedQueryOrder));
////                            System.out.println("table old:" + order[i]);
////                            System.out.println("reuseTable:" + reusedTable);
////                            System.out.println("tupleIndices: " + tupleIndices[order[i]]);
////                            System.out.println("reuseTableIdx: " + reusedTupleIndices[reusedTable]);
//                            if(reusedTableOffset < recoveryTupleIndices[reusedTable]) {
//                                tryReuse = false;
//                                break;
//                            }
//
//                            isSameProgress &= (reusedTableOffset == recoveryTupleIndices[reusedTable]);
//                            reusedTupleIndices[reusedTable] = reusedTableOffset;
//                            nrTuples[reusedTable] += reuseNrTuples[i];
//                            totalReuseTuples += reuseNrTuples[i];
//                        }

                        //System.arraycopy(tupleIndices, 0, reusedTupleIndices, 0, joinIndex);

                        //                    LeftDeepPlan plan = planCache[reusedQuery].get(reusedQueryOrder);
                        //                    List<List<KnaryBoolEval>> reuseApplicablePreds = plan.applicablePreds;
                        //                    List<List<JoinIndexWrapper>> reuseJoinIndices = plan.joinIndices;
                        //                    reuseJoin(reusedQueryOrder, cardinalities[reusedQuery], offsets, reusedTupleIndices, reuseJoinIndices, reuseApplicablePreds
                        //                            , unaryPreds[reusedQuery], reuseJoinIndex, nrReuseQueryTable, batchGroups[reusedQuery]);

                        if(queryNum == GeneralConfig.testQuery || reusedQuery == GeneralConfig.testQuery) {
                            System.out.println("reused after indices:" + Arrays.toString(reusedTupleIndices));
                            System.out.println("reused:" + tryReuse);
                            System.out.println("============");
                        }

//                        if(isSameProgress) {
//                            reusedTupleIndices = recoveryTupleIndices;
//                        }

                        //State reuseState = tracker[reusedQuery].continueFrom(new JoinOrder(reusedQueryOrder));
//                        if(tryReuse) {
//                            reuseFinish[joinIndex] &= reuseJoin(reusedQuery, joinIndex + 1, nrReuseQueryTable, reusedTupleIndices, isVisit, nrTuples, reuseBudget + totalReuseTuples, reuseState);
//                            this.budgets[reusedQuery] -= (reuseBudget + totalReuseTuples);
//                        }
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
                        if (joinIndex < startIdx || !reuseFinish[joinIndex]) {
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

                --this.budgets[queryNum];

            } else {

                // At least one of applicable predicates evaluates to false -
                // try next tuple in same table.
                tupleIndices[nextTable] = proposeNext(cardinality, joinIndices.get(joinIndex), nextTable, tupleIndices);
                // Have reached end of current table? -> we backtrack.
                while (tupleIndices[nextTable] >= nextCardinality) {
                    tupleIndices[nextTable] = 0;
                    --joinIndex;
                    if (joinIndex < startIdx || !reuseFinish[joinIndex]) {
                        break;
                    }
                    nextTable = order[joinIndex];
                    nextCardinality = cardinality[nextTable];
                    tupleIndices[nextTable] += 1;
                }
            }
        }

        // Save final state
//        if(joinIndex < 0) {
//            System.out.println("Q" + queryNum);
//            state.lastIndex = joinIndex;
//        }



        //reuse. why not work
        if(startIdx > 0 && joinIndex < startIdx) {
            for (int i = startIdx; i < endIdx; i++) {
                int curTable = order[i];
                tupleIndices[curTable] = cardinality[curTable] - 1;
            }
        }

        if(queryNum == GeneralConfig.testQuery) {
            System.out.println("stat================");
            System.out.println("Query:" + queryNum);
            System.out.println("Join order:" + Arrays.toString(order));
            System.out.println("join prefix:" + joinIndex);
            System.out.println("start prefix:" + startIdx);
            System.out.println("end prefix:" + endIdx);
            System.out.println("start table indices:" + Arrays.toString(currentStartIndices));
            System.out.println("end table indices:" + Arrays.toString(tupleIndices));
            System.out.println("stat================");
        }

        state.lastIndex = joinIndex;
        for (int tableCtr = 0; tableCtr < order.length; ++tableCtr) {
            state.tupleIndices[tableCtr] = tupleIndices[tableCtr];
            endIndices[queryNum][tableCtr] = tupleIndices[tableCtr];
        }

        tracker[queryNum].updateProgress(new JoinOrder(order), state);
        return (joinIndex < startIdx);
        //return (this.budgets[queryNum] > 0) && reuseFinish;

    }

}
