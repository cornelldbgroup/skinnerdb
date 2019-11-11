package joining.parallel.join;

import config.PreConfig;
import expressions.ExpressionInfo;
import expressions.compilation.KnaryBoolEval;
import joining.plan.JoinOrder;
import joining.progress.State;
import joining.parallel.plan.LeftDeepPartitionPlan;
import joining.parallel.progress.ParallelProgressTracker;
import preprocessing.Context;
import query.QueryInfo;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ModJoin extends DPJoin {
    /**
     * Re-initialized in each invocation:
     * stores the remaining budget for
     * the current iteration.
     */
    public int remainingBudget;
    /**
     * Number of completed tuples produced
     * during last invocation.
     */
    public int nrResultTuples;
    /**
     * Avoids redundant planning work by storing left deep plans.
     */
    final Map<Integer, LeftDeepPartitionPlan> planCache;
    /**
     * Avoids redundant evaluation work by tracking evaluation progress.
     */
    public ParallelProgressTracker tracker;
    /**
     * Associates each table index with unary predicates.
     */
    final KnaryBoolEval[] unaryPreds;
    /**
     * Contains after each invocation the delta of the tuple
     * indices when comparing start state and final state.
     */
    public final int[] tupleIndexDelta;
    /**
     * Counts number of log entries made.
     */
    int logCtr = 0;
    /**
     * Whether the join phase is terminated
     */
    public boolean isFinished = false;
    /**
     * Initializes join algorithm for given input query.
     *
     * @param query			query to process
     * @param preSummary	summary of pre-processing
     * @param budget		budget per episode
     */
    public ModJoin(QueryInfo query, Context preSummary,
                   int budget, int nrThreads, int tid) throws Exception {
        super(query, preSummary, budget, nrThreads, tid);
        this.planCache = new HashMap<>();
        // Collect unary predicates
        this.unaryPreds = new KnaryBoolEval[nrJoined];
        for (ExpressionInfo unaryExpr : query.wherePredicates) {
            // Is it a unary predicate?
            if (unaryExpr.aliasIdxMentioned.size()==1) {
                // (Exactly one table mentioned for unary predicates)
                int aliasIdx = unaryExpr.aliasIdxMentioned.iterator().next();
                KnaryBoolEval eval = predToEval.get(unaryExpr.finalExpression);
                unaryPreds[aliasIdx] = eval;
            }
        }
        this.tupleIndexDelta = new int[nrJoined];
    }
    /**
     * Calculates reward for progress during one invocation.
     *
     * @param joinOrder			join order followed
     * @param tupleIndexDelta	difference in tuple indices
     * @param tableOffsets		table offsets (number of tuples fully processed)
     * @return					reward between 0 and 1, proportional to progress
     */
    double reward(int[] joinOrder, int[] tupleIndexDelta, int[] tableOffsets) {
        double progress = 0;
        double weight = 1;
        for (int pos=0; pos<nrJoined; ++pos) {
            // Scale down weight by cardinality of current table
            int curTable = joinOrder[pos];
            int remainingCard = cardinalities[curTable] -
                    (tableOffsets[curTable]);
            //int remainingCard = cardinalities[curTable];
            weight *= 1.0 / remainingCard;
            // Fully processed tuples from this table
            progress += tupleIndexDelta[curTable] * weight;
        }
        return 0.5*progress + 0.5*nrResultTuples/(double)budget;
//        return progress;
    }
    /**
     * Executes a given join order for a given budget of steps
     * (i.e., predicate evaluations). Result tuples are added
     * to result set. Budget and result set are created during
     * the class initialization.
     *
     * @param order   table join order
     */
    @Override
    public double execute(int[] order, int splitTable, int roundCtr) throws Exception {
        // Treat special case: at least one input relation is empty
        for (int tableCtr=0; tableCtr<nrJoined; ++tableCtr) {
            if (cardinalities[tableCtr]==0) {
                isFinished = true;
                return 1;
            }
        }
        // Lookup or generate left-deep query plan
        JoinOrder joinOrder = new JoinOrder(order);
        int joinHash = joinOrder.splitHashCode(-1);
        LeftDeepPartitionPlan plan = planCache.get(joinHash);
        if (plan == null) {
            plan = new LeftDeepPartitionPlan(query, predToEval, joinOrder);
            planCache.putIfAbsent(joinHash, plan);
        }
        int splitHash = joinOrder.splitHashCode(splitTable);
        // Execute from ing state, save progress, return progress
        State state = tracker.continueFrom(joinOrder, splitHash, tid, splitTable, null);
//        writeLog("Round: " + roundCtr);
//        writeLog("Join: " + Arrays.toString(order) + "\tSplit: " + splitTable + "\tHash: " + splitHash);
        int[] offsets = tracker.tableOffset;
//        writeLog("Start: " + state.toString());
//        writeLog("Offset: " + Arrays.toString(offsets));
        executeWithBudget(plan, splitTable, state, offsets, tid);
//        writeLog("End: " + state.toString());
        double reward = reward(joinOrder.order, tupleIndexDelta, offsets);
        // Get the first table whose cardinality is larger than 1.
        int firstTable = getFirstLargeTable(order);
        tracker.updateProgress(joinOrder, splitHash, state, tid, roundCtr, splitTable, firstTable);
        lastState = state;
        lastTable = splitTable;
        return reward;
    }
    /**
     * Evaluates list of given predicates on current tuple
     * indices and returns true iff all predicates evaluate
     * to true.
     *
     * @param preds				predicates to evaluate
     * @param tupleIndices		(partial) tuples
     * @return					true iff all predicates evaluate to true
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
     * Evaluates list of given predicates on current tuple
     * indices and returns true iff all predicates evaluate
     * to true.
     *
     * @param indexWrappers
     * @param tupleIndices
     * @param splitTable
     * @param tid
     * @param nextTable
     * @return
     */
    boolean evaluateInScope(List<JoinPartitionIndexWrapper> indexWrappers, List<KnaryBoolEval> preds, int[] tupleIndices,
                            int splitTable, int nextTable, int tid) {
        boolean first = true;
        if (indexWrappers.isEmpty()) {
            if (nextTable == splitTable) {
                if (tupleIndices[nextTable] % nrThreads != tid) {
                    return false;
                }
            }
        }
        for (JoinPartitionIndexWrapper wrapper : indexWrappers) {
            if (first && splitTable == nextTable) {
                if (!wrapper.evaluateInScope(tupleIndices, tid)) {
                    return false;
                }
            }
            else {
                if (!wrapper.evaluate(tupleIndices)) {
                    return false;
                }
            }
            first = false;
        }
        // evaluate non-equi join predicates
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
     * @param indexWrappers	list of join index wrappers
     * @param tupleIndices	current tuple indices
     * @param tid	        thread id
     * @return				next join index
     */
    int proposeNextInScope(int[] joinOrder, int splitTable, List<JoinPartitionIndexWrapper> indexWrappers,
                    int curIndex, int[] tupleIndices, int tid) {
        int nextTable = joinOrder[curIndex];
        int nextCardinality = cardinalities[nextTable];
        // If there is no equi-predicates.
        if (indexWrappers.isEmpty()) {
            if (splitTable == nextTable) {
                int jump = (nrThreads + tid - tupleIndices[nextTable] % nrThreads) % nrThreads;
                tupleIndices[nextTable] += jump;
            }
            else {
                tupleIndices[nextTable]++;
            }
        }
        else {
            boolean first = true;
            for (JoinPartitionIndexWrapper wrapper : indexWrappers) {
                if (splitTable == nextTable) {
                    if (!first) {
                        if (wrapper.evaluate(tupleIndices)) {
                            continue;
                        }
                    }
                    int nextRaw = first ? wrapper.nextIndexInScope(tupleIndices, tid):
                            wrapper.nextIndex(tupleIndices);
                    if (nextRaw < 0 || nextRaw == nextCardinality) {
                        tupleIndices[nextTable] = nextCardinality;
                        break;
                    }
                    else {
                        tupleIndices[nextTable] = nextRaw;
                    }
                    first = false;
                }
                else {
                    int nextRaw = wrapper.nextIndex(tupleIndices);
                    if (!first) {
                        if (wrapper.evaluate(tupleIndices)) {
                            continue;
                        }
                    }
                    if (nextRaw < 0 || nextRaw == nextCardinality) {
                        tupleIndices[nextTable] = nextCardinality;
                        break;
                    }
                    else {
                        tupleIndices[nextTable] = nextRaw;
                    }
                    first = false;
                }
            }
        }

        // Have reached end of current table? -> we backtrack.
        while (tupleIndices[nextTable] >= nextCardinality) {
            tupleIndices[nextTable] = 0;
            --curIndex;
            if (curIndex < 0) {
                break;
            }
            nextTable = joinOrder[curIndex];
            nextCardinality = cardinalities[nextTable];
            tupleIndices[nextTable] += 1;
        }
        return curIndex;
    }
    /**
     * Executes a given join order for a given budget of steps
     * (i.e., predicate evaluations). Result tuples are added
     * to result set. Budget and result set are created during
     * the class initialization.
     *
     * @param plan    left-deep query plan fixing join order
     * @param offsets last fully treated index for each table
     * @param state   last tuple visited in each base table before start
     */
    private void executeWithBudget(LeftDeepPartitionPlan plan, int splitTable, State state, int[] offsets, int tid) {
        // Extract variables for convenient access
        int nrTables = query.nrJoined;
        int[] tupleIndices = new int[nrTables];
        List<List<JoinPartitionIndexWrapper>> joinIndices = plan.joinIndices;
        List<List<KnaryBoolEval>> applicablePreds = plan.applicablePreds;
        // Initialize state and flags to prepare budgeted execution
//        int joinIndex = state.lastIndex;
        int joinIndex = 1;
        System.arraycopy(state.tupleIndices, 0, tupleIndices, 0, nrTables);
        int remainingBudget = budget;
        // Number of completed tuples added
        nrResultTuples = 0;
        // Execute join order until budget depleted or all input finished -
        // at each iteration start, tuple indices contain next tuple
        // combination to look at.
        while (remainingBudget > 0 && joinIndex >= 0) {

//            ++statsInstance.nrIterations;
            //log("Offsets:\t" + Arrays.toString(offsets));
            //log("Indices:\t" + Arrays.toString(tupleIndices));
            // Get next table in join order
            int nextTable = plan.joinOrder.order[joinIndex];
//            writeLog("Indices: " + Arrays.toString(tupleIndices));
            // Integrate table offset
            tupleIndices[nextTable] = Math.max(
                    offsets[nextTable], tupleIndices[nextTable]);
            // Evaluate all applicable predicates on joined tuples
            KnaryBoolEval unaryPred = unaryPreds[nextTable];

            if ((PreConfig.PRE_FILTER || unaryPred == null ||
                    unaryPred.evaluate(tupleIndices)>0) &&
//                    evaluateAll(applicablePreds.get(joinIndex), tupleIndices)
                    evaluateInScope(joinIndices.get(joinIndex), applicablePreds.get(joinIndex), tupleIndices, splitTable, nextTable, tid)
            ) {
//                ++statsInstance.nrTuples;
                // Do we have a complete result row?
                if(joinIndex == plan.joinOrder.order.length - 1) {
                    // Complete result row -> add to result
                    ++nrResultTuples;
                    result.add(tupleIndices);
//                    writeLog("INFO:Bingo: " + Arrays.toString(tupleIndices));
                    joinIndex = proposeNextInScope(
                            plan.joinOrder.order, splitTable, joinIndices.get(joinIndex), joinIndex, tupleIndices, tid);
                } else {
                    // No complete result row -> complete further
                    joinIndex++;
                    //System.out.println("Current Join Index2:"+ joinIndex);
                }
            } else {
                // At least one of applicable predicates evaluates to false -
                // try next tuple in same table.
                joinIndex = proposeNextInScope(
                        plan.joinOrder.order, splitTable, joinIndices.get(joinIndex), joinIndex, tupleIndices, tid);
            }
            --remainingBudget;
        }
        // Store tuple index deltas used to calculate reward
        for (int tableCtr = 0; tableCtr < nrTables; ++tableCtr) {
            int start = Math.max(offsets[tableCtr], state.tupleIndices[tableCtr]);
            int end = Math.max(offsets[tableCtr], tupleIndices[tableCtr]);
            tupleIndexDelta[tableCtr] = end - start;
            if (joinIndex == -1 && tableCtr == plan.joinOrder.order[0] &&
                    tupleIndexDelta[tableCtr] <= 0) {
                tupleIndexDelta[tableCtr] = cardinalities[tableCtr] - start;
            }
        }
        // Save final state
        state.lastIndex = joinIndex;
        System.arraycopy(tupleIndices, 0, state.tupleIndices, 0, nrTables);
    }
    @Override
    public boolean isFinished() {
        return lastState.isFinished();
    }
}
