package joining.parallel.join;

import config.JoinConfig;
import config.LoggingConfig;
import config.ParallelConfig;
import config.PreConfig;
import expressions.compilation.KnaryBoolEval;
import joining.parallel.indexing.OffsetIndex;
import joining.parallel.plan.LeftDeepPartitionPlan;
import joining.parallel.progress.ParallelProgressTracker;
import joining.plan.HotSet;
import joining.plan.JoinOrder;
import joining.progress.ProgressTracker;
import joining.progress.State;
import joining.result.ResultTuple;
import net.sf.jsqlparser.expression.Expression;
import predicate.NonEquiNode;
import preprocessing.Context;
import query.QueryInfo;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class OldJoin extends SPJoin {
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
    public final ParallelProgressTracker tracker;
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
     * Whether the join phase is terminated
     */
    public boolean isFinished = false;
    /**
     * The deepest join index after one episode of execution .
     */
    public int lastIndex = 0;
    /**
     * Map table to index in the order.
     */
    public int[] tableIndex;
    /**
     * Offset index for each thread.
     */
    public final OffsetIndex[][] threadOffsets;
    /**
     * The table that has the largest progress.
     */
    public int quickTable = 0;
    /**
     * Atomic hash set to store
     * result tuple.
     */
    private final Set<ResultTuple> concurrentSet;
    public final List<ResultTuple> concurrentList = new LinkedList<>();
    public long insertTime = 0;

    /**
     * Initializes join algorithm for given input query.
     *
     * @param query			query to process
     * @param preSummary	summary of pre-processing
     * @param budget		budget per episode
     */
    public OldJoin(QueryInfo query, Context preSummary,
                   int budget, int nrThreads, int tid,
                   Map<Expression, NonEquiNode> predToEval,
                   OffsetIndex[][] threadOffsets,
                   Set<ResultTuple> concurrentSet) throws Exception {
        super(query, preSummary, budget, nrThreads, tid, predToEval);
        this.planCache = new HashMap<>();
        // Collect unary predicates
        this.unaryPreds = new KnaryBoolEval[nrJoined];
        this.tupleIndexDelta = new int[nrJoined];
        this.tableIndex = new int[nrJoined];
        this.tracker = new ParallelProgressTracker(nrJoined, 1, 1);
        this.threadOffsets = threadOffsets;
        this.concurrentSet = concurrentSet;
    }
    /**
     * Calculates reward for progress during one invocation.
     *
     * @param joinOrder			join order followed
     * @param tupleIndexDelta	difference in tuple indices
     * @param tableOffsets		table offsets (number of tuples fully processed)
     * @return					reward between 0 and 1, proportional to progress
     */
    double reward(int[] joinOrder, int[] tupleIndexDelta, int[] tableOffsets, int[] tupleIndices) {
        double progress = 0;
        double weight = 1;
        this.progress = 0;
        double pWeight = 1;
        for (int pos=0; pos<nrJoined; ++pos) {
            // Scale down weight by cardinality of current table
            int curTable = joinOrder[pos];
            int curCard = cardinalities[curTable];
            int remainingCard = curCard -
                    (tableOffsets[curTable]);
            //int remainingCard = cardinalities[curTable];
            weight *= 1.0 / remainingCard;
            // Fully processed tuples from this table
            progress += tupleIndexDelta[curTable] * weight;
            this.progress += pWeight * (tupleIndices[curTable] + 0.0) / curCard;
            pWeight *= 1.0 / curCard;
        }
        return JoinConfig.INPUT_REWARD_WEIGHT * progress
                + JoinConfig.OUTPUT_REWARD_WEIGHT * nrResultTuples/(double)budget;
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
    public double execute(int[] order, int roundCtr) throws Exception {
        // Treat special case: at least one input relation is empty
        for (int tableCtr=0; tableCtr<nrJoined; ++tableCtr) {
            if (cardinalities[tableCtr]==0) {
                isFinished = true;
                return 1;
            }
        }
//        order = new int[]{7, 5, 4, 3, 2, 0, 1, 6};
        // Lookup or generate left-deep query plan
        JoinOrder joinOrder = new JoinOrder(order);
        for (int i = 0; i < nrJoined; i++) {
            tableIndex[order[i]] = i;
        }
        lastIndex = 0;
        int joinHash = joinOrder.splitHashCode(-1);
        LeftDeepPartitionPlan plan = planCache.get(joinHash);
        if (plan == null) {
            plan = new LeftDeepPartitionPlan(query, predToEval, joinOrder);
            planCache.putIfAbsent(joinHash, plan);
        }
        // Execute from ing state, save progress, return progress
        int firstTable = getFirstLargeTable(order);
        State state = tracker.continueFromSP(joinOrder);
        if (LoggingConfig.PARALLEL_JOIN_VERBOSE) {
            writeLog("Round: " + roundCtr + "\t" + "Join: " + Arrays.toString(order));
        }
        int[] offsets = tracker.tableOffsetMaps[0][0];
        if (JoinConfig.OFFSETS_SHARING) {
            for (int threadCtr = 0; threadCtr < nrThreads; threadCtr++) {
                for (int tableCtr = 0; tableCtr < nrJoined; tableCtr++) {
                    offsets[tableCtr] = Math.max(offsets[tableCtr],
                            threadOffsets[threadCtr][tableCtr].index);
                }
            }
        }
        int lastFirstIndex = offsets[firstTable];
        if (LoggingConfig.PARALLEL_JOIN_VERBOSE) {
            writeLog("Start: " + state.toString() + "\t" + "Offset: " + Arrays.toString(offsets));
        }
//        writeLog("Offset: " + Arrays.toString(offsets));
        // move the indices into correct position
        boolean forward = false;
        for (int i = 0; i < nrJoined; i++) {
            int table = order[i];
            int offset = offsets[table];
            if (!forward) {
                if (state.tupleIndices[table] < offset) {
                    forward = true;
                    state.tupleIndices[table] = offset;
                }
            }
            else {
                state.tupleIndices[table] = offset;
            }
            plan.joinIndices.get(i).forEach(index ->index.reset(state.tupleIndices));
        }
        executeWithBudget(plan, state, offsets);

        // progress in the left table.
        double reward = reward(joinOrder.order, tupleIndexDelta, offsets, state.tupleIndices);
        // Get the first table whose cardinality is larger than 1.
        state.roundCtr = 0;
        for (int table: query.temporaryTables) {
            state.tupleIndices[table] = 0;
        }
        if (!state.isFinished()) {
            tracker.updateProgressSP(joinOrder, state, roundCtr, firstTable);
        }
        // Update table offset considering last fully treated tuple
        int lastTreatedTuple = state.tupleIndices[firstTable] - 1;
        if (lastTreatedTuple > lastFirstIndex) {
            tracker.tableOffsetMaps[0][0][firstTable] = lastTreatedTuple;
            if (JoinConfig.OFFSETS_SHARING) {
                threadOffsets[tid][firstTable].index = lastTreatedTuple;
            }
        }
        // Retrieve the quick table
        double maxProgress = 0;
        for (int table = 0; table < nrJoined; table++) {
            int offsetIndex = offsets[table];
            int cardinality = cardinalities[table];
            double progress = (offsetIndex + 0.0) / cardinality;
            if (progress > maxProgress) {
                maxProgress = progress;
                quickTable = table;
            }
        }
        lastState = state;
        if (LoggingConfig.PARALLEL_JOIN_VERBOSE) {
            writeLog("End: " + state + "\tReward: " + reward + "\t" + nrResultTuples);
        }
//        long timer5 = System.currentTimeMillis();
//        writeLog((timer5 - timer1) + "\t" + (timer2 - timer1) + "\t" + (timer3 - timer2)
//                + "\t" + (timer4 - timer3) + "\t" + (timer5 - timer4));
        this.roundCtr = roundCtr;
        return reward;
    }

    public double executeWorker(int[] order, int roundCtr, LeftDeepPartitionPlan plan) throws Exception {
        // Treat special case: at least one input relation is empty
        for (int tableCtr=0; tableCtr<nrJoined; ++tableCtr) {
            if (cardinalities[tableCtr]==0) {
                isFinished = true;
                return 1;
            }
        }
//        order = new int[]{3, 0, 1, 2, 4, 5};
        // Lookup or generate left-deep query plan
        JoinOrder joinOrder = new JoinOrder(order);
        for (int i = 0; i < nrJoined; i++) {
            tableIndex[order[i]] = i;
        }
        lastIndex = 0;
        // Execute from ing state, save progress, return progress
        int firstTable = getFirstLargeTable(order);
        State state = tracker.continueFromSP(joinOrder);
        long startEpisode = System.currentTimeMillis();
        if (LoggingConfig.PARALLEL_JOIN_VERBOSE) {
            writeLog("Round: " + roundCtr + "\t"
                    + "Join: " + Arrays.toString(order) + "\t"
                    + startEpisode);
        }
        int[] offsets = tracker.tableOffsetMaps[0][0];
        if (JoinConfig.OFFSETS_SHARING) {
            for (int threadCtr = 0; threadCtr < nrThreads; threadCtr++) {
                for (int tableCtr = 0; tableCtr < nrJoined; tableCtr++) {
                    offsets[tableCtr] = Math.max(offsets[tableCtr],
                            threadOffsets[threadCtr][tableCtr].index);
                }
            }
        }
        int lastFirstIndex = offsets[firstTable];
        if (LoggingConfig.PARALLEL_JOIN_VERBOSE) {
            writeLog("Start: " + state.toString() + "\t" + "Offset: " + Arrays.toString(offsets));
        }
//        writeLog("Offset: " + Arrays.toString(offsets));
        // move the indices into correct position
        boolean forward = false;
        for (int i = 0; i < nrJoined; i++) {
            int table = order[i];
            int offset = offsets[table];
            if (!forward) {
                if (state.tupleIndices[table] < offset) {
                    forward = true;
                    state.tupleIndices[table] = offset;
                }
            }
            else {
                state.tupleIndices[table] = offset;
            }
            plan.joinIndices.get(i).forEach(index ->index.reset(state.tupleIndices));
        }
        executeWithBudgetWorker(plan, state, offsets);

        // progress in the left table.
        double reward = reward(joinOrder.order, tupleIndexDelta, offsets, state.tupleIndices);
        // Get the first table whose cardinality is larger than 1.
        state.roundCtr = 0;
        for (int table: query.temporaryTables) {
            state.tupleIndices[table] = 0;
        }
        if (!state.isFinished()) {
            tracker.updateProgressSP(joinOrder, state, roundCtr, firstTable);
        }
        // Update table offset considering last fully treated tuple
        int lastTreatedTuple = state.tupleIndices[firstTable] - 1;
        if (lastTreatedTuple > lastFirstIndex) {
            tracker.tableOffsetMaps[0][0][firstTable] = lastTreatedTuple;
            if (JoinConfig.OFFSETS_SHARING) {
                threadOffsets[tid][firstTable].index = lastTreatedTuple;
            }
        }
        // Retrieve the quick table
        double maxProgress = 0;
        for (int table = 0; table < nrJoined; table++) {
            int offsetIndex = offsets[table];
            int cardinality = cardinalities[table];
            double progress = (offsetIndex + 0.0) / cardinality;
            if (progress > maxProgress) {
                maxProgress = progress;
                quickTable = table;
            }
        }
        lastState = state;
        if (LoggingConfig.PARALLEL_JOIN_VERBOSE) {
            writeLog("End: " + state );
        }
//        long timer5 = System.currentTimeMillis();
//        writeLog((timer5 - timer1) + "\t" + (timer2 - timer1) + "\t" + (timer3 - timer2)
//                + "\t" + (timer4 - timer3) + "\t" + (timer5 - timer4));
        this.roundCtr = roundCtr;
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
     * @return
     */
    boolean evaluateInScope(List<JoinPartitionIndexWrapper> indexWrappers, List<NonEquiNode> preds,
                            int[] tupleIndices, int nextTable) {
        if (indexWrappers.isEmpty() && preds.isEmpty()) {
            return true;
        }
        for (JoinPartitionIndexWrapper wrapper : indexWrappers) {
            if (!wrapper.evaluate(tupleIndices)) {
                return false;
            }
        }
        // evaluate non-equi join predicates
        for (NonEquiNode pred : preds) {
            if (!pred.evaluate(tupleIndices, nextTable, cardinalities[nextTable])) {
                return false;
            }
        }
        return true;
    }

    private int nrIndexed(int[] joinOrder, List<JoinPartitionIndexWrapper> indexWrappers,
                          int curIndex, int[] tupleIndices) {
        int nextTable = joinOrder[curIndex];
        if (indexWrappers.isEmpty()) {
            return cardinalities[nextTable];
        }
        else {
            int size = Integer.MAX_VALUE;
            for (JoinPartitionIndexWrapper wrapper : indexWrappers) {
                size = Math.min(wrapper.nrIndexed(tupleIndices), size);
            }
            return size;
        }
    }

    /**
     * Propose next tuple index to consider, based on a set of
     * indices on the join column.
     *
     * @param indexWrappersList	list of join index wrappers
     * @param tupleIndices	current tuple indices
     * @return				next join index
     */
    int proposeNextInScope(int[] joinOrder, List<List<JoinPartitionIndexWrapper>> indexWrappersList,
                           int curIndex, int[] tupleIndices, boolean eval) {
        int nextTable = joinOrder[curIndex];
        int nextCardinality = cardinalities[nextTable];
        List<JoinPartitionIndexWrapper> indexWrappers = indexWrappersList.get(curIndex);
        int priorTable = -1;
        // If there is no equi-predicates.
        if (indexWrappers.isEmpty()) {
            tupleIndices[nextTable]++;
            if (query.temporaryTables.contains(nextTable) && !eval) {
                priorTable = query.temporaryConnection.get(nextTable);
            }
        }
        else {
            boolean first = true;
            for (JoinPartitionIndexWrapper wrapper : indexWrappers) {
                if (!first) {
                    if (wrapper.evaluate(tupleIndices)) {
                        continue;
                    }
                }
                int nextRaw = wrapper.nextIndex(tupleIndices, null);
//                int nextRaw = wrapper.nextIndexFromLast(tupleIndices, null, tid);
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
        // Have reached end of current table? -> we backtrack.
        while (tupleIndices[nextTable] >= nextCardinality) {
            if (priorTable >= 0) {
                while (nextTable != priorTable) {
                    tupleIndices[nextTable] = 0;
                    --curIndex;
                    nextTable = joinOrder[curIndex];
                    nextCardinality = cardinalities[nextTable];
                }
                tupleIndices[nextTable] += 1;
                priorTable = -1;
            }
            else {
                tupleIndices[nextTable] = 0;
                --curIndex;
                if (curIndex < 0) {
                    break;
                }
                nextTable = joinOrder[curIndex];
                nextCardinality = cardinalities[nextTable];
                tupleIndices[nextTable] += 1;
            }
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
    private void executeWithBudget(LeftDeepPartitionPlan plan, State state, int[] offsets) {
        // Extract variables for convenient access
        int nrTables = query.nrJoined;
        int[] tupleIndices = new int[nrTables];
        List<List<JoinPartitionIndexWrapper>> joinIndices = plan.joinIndices;
//        List<List<KnaryBoolEval>> applicablePreds = plan.applicablePreds;
        List<List<NonEquiNode>> applicablePreds = plan.nonEquiNodes;
        // Initialize state and flags to prepare budgeted execution
//        int joinIndex = state.lastIndex;
        int joinIndex = 1;
        System.arraycopy(state.tupleIndices, 0, tupleIndices, 0, nrTables);
        int remainingBudget = budget;
        for (int joinCtr = 0; joinCtr < nrTables; joinCtr++) {
            int table = plan.joinOrder.order[joinCtr];
            joinIndex = joinCtr;
            remainingBudget--;
            if (query.temporaryTables.contains(table)) {
                tupleIndices[table] = offsets[table];
            }
            if (!evaluateInScope(joinIndices.get(joinCtr), applicablePreds.get(joinCtr),
                    tupleIndices, table)) {
                for (int back = joinIndex + 1; back < nrTables; back++) {
                    int backTable = plan.joinOrder.order[back];
                    tupleIndices[backTable] = offsets[backTable];
                }
                joinIndex = proposeNextInScope(
                        plan.joinOrder.order, joinIndices, joinIndex, tupleIndices, true);
                break;
            }
            else if (joinIndex == nrTables - 1){
                ++nrResultTuples;
//                long add1 = System.currentTimeMillis();
                concurrentList.add(new ResultTuple(tupleIndices));
//                concurrentSet.add(new ResultTuple(tupleIndices));
//                long add2 = System.currentTimeMillis();
//                insertTime = Math.max(insertTime, (add2 - add1));
                joinIndex = proposeNextInScope(
                        plan.joinOrder.order, joinIndices, joinIndex, tupleIndices, false);
//                writeLog("INFO:Bingo: " + Arrays.toString(tupleIndices));
            }
        }

        // Number of completed tuples added
        Arrays.fill(nrVisited, 0);
        nrResultTuples = 0;
        // Execute join order until budget depleted or all input finished -
        // at each iteration start, tuple indices contain next tuple
        // combination to look at.
        while (remainingBudget > 0 && joinIndex >= 0) {
            //log("Offsets:\t" + Arrays.toString(offsets));
            //log("Indices:\t" + Arrays.toString(tupleIndices));
            // Get next table in join order
            int nextTable = plan.joinOrder.order[joinIndex];
            nrVisited[nextTable]++;
            // Integrate table offset
            int offset = offsets[nextTable];
            nrVisited[nextTable] += (offset > tupleIndices[nextTable] ? offset - tupleIndices[nextTable] : 0);
            tupleIndices[nextTable] = Math.max(offset, tupleIndices[nextTable]);
            // Evaluate all applicable predicates on joined tuples
            KnaryBoolEval unaryPred = unaryPreds[nextTable];
            lastIndex = Math.max(lastIndex, joinIndex);
            if ((PreConfig.PRE_FILTER || unaryPred == null ||
                    unaryPred.evaluate(tupleIndices)>0) &&
//                    evaluateAll(applicablePreds.get(joinIndex), tupleIndices)
                    evaluateInScope(joinIndices.get(joinIndex), applicablePreds.get(joinIndex), tupleIndices, nextTable)
            ) {
                ++statsInstance.nrTuples;
                // Do we have a complete result row?
                if(joinIndex == plan.joinOrder.order.length - 1) {
                    // Complete result row -> add to result
                    ++nrResultTuples;
//                    long add1 = System.currentTimeMillis();
                    concurrentList.add(new ResultTuple(tupleIndices));
//                    concurrentSet.add(new ResultTuple(tupleIndices));
//                    long add2 = System.currentTimeMillis();
//                    insertTime = Math.max(insertTime, (add2 - add1));
//                    writeLog("INFO:Bingo: " + Arrays.toString(tupleIndices));
                    joinIndex = proposeNextInScope(
                            plan.joinOrder.order, joinIndices, joinIndex, tupleIndices, true);
                } else {
                    // No complete result row -> complete further
                    joinIndex++;
                    //System.out.println("Current Join Index2:"+ joinIndex);
                }
            } else {
                // At least one of applicable predicates evaluates to false -
                // try next tuple in same table.
                joinIndex = proposeNextInScope(
                        plan.joinOrder.order, joinIndices, joinIndex, tupleIndices, false);
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


    private void executeWithBudgetWorker(LeftDeepPartitionPlan plan, State state, int[] offsets) {
        // Extract variables for convenient access
        int nrTables = query.nrJoined;
        int[] tupleIndices = new int[nrTables];
        List<List<JoinPartitionIndexWrapper>> joinIndices = plan.joinIndices;
//        List<List<KnaryBoolEval>> applicablePreds = plan.applicablePreds;
        List<List<NonEquiNode>> applicablePreds = plan.nonEquiNodes;
        // Initialize state and flags to prepare budgeted execution
//        int joinIndex = state.lastIndex;
        int joinIndex = 1;
        System.arraycopy(state.tupleIndices, 0, tupleIndices, 0, nrTables);
        int remainingBudget = budget;
        for (int joinCtr = 0; joinCtr < nrTables; joinCtr++) {
            int table = plan.joinOrder.order[joinCtr];
            joinIndex = joinCtr;
            remainingBudget--;
            if (query.temporaryTables.contains(table)) {
                tupleIndices[table] = offsets[table];
            }
            if (!evaluateInScope(joinIndices.get(joinCtr), applicablePreds.get(joinCtr),
                    tupleIndices, table)) {
                for (int back = joinIndex + 1; back < nrTables; back++) {
                    int backTable = plan.joinOrder.order[back];
                    tupleIndices[backTable] = offsets[backTable];
                }
                joinIndex = proposeNextInScope(
                        plan.joinOrder.order, joinIndices, joinIndex, tupleIndices, true);
                break;
            }
            else if (joinIndex == nrTables - 1){
                ++nrResultTuples;
//                long add1 = System.currentTimeMillis();
//                result.add(tupleIndices);
                concurrentSet.add(new ResultTuple(tupleIndices));
//                concurrentList.add(new ResultTuple(tupleIndices));
//                long add2 = System.currentTimeMillis();
//                insertTime = Math.max(insertTime, (add2 - add1));
                joinIndex = proposeNextInScope(
                        plan.joinOrder.order, joinIndices, joinIndex, tupleIndices, false);
//                writeLog("INFO:Bingo: " + Arrays.toString(tupleIndices));
            }
        }

        // Number of completed tuples added
        Arrays.fill(nrVisited, 0);
        nrResultTuples = 0;
        // Execute join order until budget depleted or all input finished -
        // at each iteration start, tuple indices contain next tuple
        // combination to look at.
        while (remainingBudget > 0 && joinIndex >= 0) {
            //log("Offsets:\t" + Arrays.toString(offsets));
            //log("Indices:\t" + Arrays.toString(tupleIndices));
            // Get next table in join order
            int nextTable = plan.joinOrder.order[joinIndex];
            nrVisited[nextTable]++;
            // Integrate table offset
            int offset = offsets[nextTable];
            nrVisited[nextTable] += (offset > tupleIndices[nextTable] ? offset - tupleIndices[nextTable] : 0);
            tupleIndices[nextTable] = Math.max(offset, tupleIndices[nextTable]);
            // Evaluate all applicable predicates on joined tuples
            KnaryBoolEval unaryPred = unaryPreds[nextTable];
            lastIndex = Math.max(lastIndex, joinIndex);
            if ((PreConfig.PRE_FILTER || unaryPred == null ||
                    unaryPred.evaluate(tupleIndices)>0) &&
//                    evaluateAll(applicablePreds.get(joinIndex), tupleIndices)
                    evaluateInScope(joinIndices.get(joinIndex), applicablePreds.get(joinIndex), tupleIndices, nextTable)
            ) {
                ++statsInstance.nrTuples;
                // Do we have a complete result row?
                if(joinIndex == plan.joinOrder.order.length - 1) {
                    // Complete result row -> add to result
                    ++nrResultTuples;
//                    long add1 = System.currentTimeMillis();
//                    result.add(tupleIndices);
                    concurrentSet.add(new ResultTuple(tupleIndices));
//                    concurrentList.add(new ResultTuple(tupleIndices));
//                    long add2 = System.currentTimeMillis();
//                    insertTime = Math.max(insertTime, (add2 - add1));
//                    writeLog("INFO:Bingo: " + Arrays.toString(tupleIndices));
                    joinIndex = proposeNextInScope(
                            plan.joinOrder.order, joinIndices, joinIndex, tupleIndices, true);
                } else {
                    // No complete result row -> complete further
                    joinIndex++;
                    //System.out.println("Current Join Index2:"+ joinIndex);
                }
            } else {
                // At least one of applicable predicates evaluates to false -
                // try next tuple in same table.
                joinIndex = proposeNextInScope(
                        plan.joinOrder.order, joinIndices, joinIndex, tupleIndices, false);
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
        return isFinished || (lastState != null && lastState.isFinished());
    }
}
