package joining.parallel.join;

import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.set.IntSet;
import config.JoinConfig;
import config.LoggingConfig;
import config.ParallelConfig;
import config.PreConfig;
import expressions.ExpressionInfo;
import expressions.compilation.KnaryBoolEval;
import joining.parallel.indexing.OffsetIndex;
import joining.parallel.plan.LeftDeepPartitionPlan;
import joining.parallel.progress.ParallelProgressTracker;
import joining.plan.JoinOrder;
import joining.progress.State;
import net.sf.jsqlparser.expression.Expression;
import predicate.NonEquiNode;
import preprocessing.Context;
import query.QueryInfo;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class HybridJoin extends DPJoin {
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
     * Associates each table index with unary predicates.
     */
    final KnaryBoolEval[] unaryPreds;
    /**
     * Contains after each invocation the delta of the tuple
     * indices when comparing start state and final state.
     */
    public final int[] tupleIndexDelta;
    /**
     * The number of visits for each table during one sample.
     */
    public final int[] nrVisits;
    /**
     * Counts number of log entries made.
     */
    int logCtr = 0;
    /**
     * Whether the join phase is terminated
     */
    public boolean isFinished = false;
    /**
     * Offset progress that has been finished by certain left-most table.
     */
    public final int[] offsets;
    /**
     * The number of down operations for each table
     */
    public final double[] downOps;
    /**
     * The number of down operations for each table
     */
    public final double[] upOps;
    /**
     * Progress of current state indices.
     */
    public double progress;
    /**
     * Offset index for each thread.
     */
    public final OffsetIndex[][] threadOffsets;
    /**
     * Initializes join algorithm for given input query.
     *
     * @param query			query to process
     * @param preSummary	summary of pre-processing
     * @param budget		budget per episode
     */
    public HybridJoin(QueryInfo query, Context preSummary,
                      int budget, int nrThreads, int tid,
                      Map<Expression, NonEquiNode> predToEval,
                      Map<Expression, KnaryBoolEval> predToComp,
                      Map<Integer, LeftDeepPartitionPlan> planCache,
                      OffsetIndex[][] threadOffsets) throws Exception {
        super(query, preSummary, budget, nrThreads, tid, predToEval, predToComp);
//        this.planCache = new HashMap<>();
        this.planCache = planCache;
        // Collect unary predicates
        this.unaryPreds = new KnaryBoolEval[nrJoined];
        if (!PreConfig.FILTER) {
            for (ExpressionInfo unaryExpr : query.wherePredicates) {
                // Is it a unary predicate?
                if (unaryExpr.aliasIdxMentioned.size()==1) {
                    // (Exactly one table mentioned for unary predicates)
                    int aliasIdx = unaryExpr.aliasIdxMentioned.iterator().next();
                    KnaryBoolEval eval = predToComp.get(unaryExpr.finalExpression);
                    unaryPreds[aliasIdx] = eval;
                }
            }
        }
        this.tupleIndexDelta = new int[nrJoined];
        this.nrVisits = new int[nrJoined];
        this.offsets = new int[nrJoined];
        this.downOps = new double[nrJoined];
        this.upOps = new double[nrJoined];
        this.tracker = new ParallelProgressTracker(nrJoined, 1, 1);
        this.threadOffsets = threadOffsets;
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
        for (int pos = 0; pos < nrJoined; ++pos) {
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
    }
    /**
     * Executes a given join order for a given budget of steps
     0 shared: [0] splitting 0
     * (i.e., predicate evaluations). Result tuples are added
     * to result set. Budget and result set are created during
     * the class initialization.
     *
     * Specifically, only search threads will call this function.
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
//        order = new int[]{3, 0, 1, 2, 4, 5};
        // Lookup or generate left-deep query plan
        JoinOrder joinOrder = new JoinOrder(order);
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
            writeLog("Search Round: " + roundCtr + "\t" + "Join: " + Arrays.toString(order));
        }
        int[] offsets = tracker.tableOffsetMaps[0][0];
//        for (int threadCtr = 0; threadCtr < nrThreads; threadCtr++) {
//            for (int tableCtr = 0; tableCtr < nrJoined; tableCtr++) {
//                offsets[tableCtr] = Math.max(offsets[tableCtr],
//                        threadOffsets[threadCtr][tableCtr].index);
//            }
//        }
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
            plan.joinIndices.get(i).forEach(index -> index.reset(state.tupleIndices));
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
//            threadOffsets[tid][firstTable].index = lastTreatedTuple;
        }
        // Retrieve the quick table
//        double maxProgress = 0;
//        for (int table = 0; table < nrJoined; table++) {
//            int offsetIndex = offsets[table];
//            int cardinality = cardinalities[table];
//            double progress = (offsetIndex + 0.0) / cardinality;
//            if (progress > maxProgress) {
//                maxProgress = progress;
//                quickTable = table;
//            }
//        }
        lastState = state;
        if (LoggingConfig.PARALLEL_JOIN_VERBOSE) {
            writeLog("End: " + state + "\tReward: " + reward + "\tTime: " + System.nanoTime());
        }
//        long timer5 = System.currentTimeMillis();
//        writeLog((timer5 - timer1) + "\t" + (timer2 - timer1) + "\t" + (timer3 - timer2)
//                + "\t" + (timer4 - timer3) + "\t" + (timer5 - timer4));
        this.roundCtr = roundCtr;
        return reward;
    }
    /**
     * Execution algorithm for hybrid data parallelization.
     *
     * Specifically, only data threads will call this function.
     *
     * @param order
     * @param splitTable
     * @param roundCtr
     * @param slowState
     * @return
     * @throws Exception
     */
    public double execute(int[] order, int splitTable, int roundCtr, int nrDPThreads,
                          State slowState, LeftDeepPartitionPlan plan) throws Exception {
//        long timer0 = System.currentTimeMillis();
        // Treat special case: at least one input relation is empty
        for (int tableCtr=0; tableCtr<nrJoined; ++tableCtr) {
            if (cardinalities[tableCtr]==0) {
                isFinished = true;
                return 1;
            }
        }
//        System.arraycopy(new int[]{4, 5, 0, 1, 2, 3}, 0, order, 0, nrJoined);
//        splitTable = 2;
        this.roundCtr = roundCtr;
        // Lookup or generate left-deep query plan
        JoinOrder joinOrder = new JoinOrder(order);
        int splitHash = nrDPThreads == 1 ? 0 : plan.splitStrategies[splitTable];
        // Execute from ing state, save progress, return progress
        State state = threadTracker.continueFrom(joinOrder, splitHash);
//        System.arraycopy(tracker.tableOffset, 0, offsets, 0, nrJoined);
        int firstTable = getFirstLargeTable(order);
        int tableOffset = slowState == null ? 0 : (slowState.tupleIndices[firstTable] - 1);
        offsets[firstTable] = Math.max(offsets[firstTable], tableOffset);
        if (slowState != null && state.isAhead(order, slowState, nrJoined)) {
            System.arraycopy(slowState.tupleIndices, 0, state.tupleIndices, 0, nrJoined);
        }
        if (LoggingConfig.PARALLEL_JOIN_VERBOSE) {
            writeLog("Data Round: " + roundCtr + "\tJoin Order: " +
                    Arrays.toString(order) + "\tSplit: " + splitTable);
            writeLog("Start progress: " + state + " Offset: " + Arrays.toString(offsets));
        }
//        long timer2 = System.currentTimeMillis();
        boolean forward = false;
        for (int i = 0; i < nrJoined; i++) {
            int table = order[i];
            if (!forward) {
                if (state.tupleIndices[table] < offsets[table]) {
                    state.tupleIndices[table] = offsets[table];
                    forward = true;
                    state.lastIndex = Math.min(state.lastIndex, i);
                }
            }
            else {
                state.tupleIndices[table] = Math.max(0, offsets[table]);
            }
        }
        int shiftID = nrThreads - tid - 1;
        executeWithBudget(plan, splitTable, state, offsets, shiftID, nrDPThreads);
//        long timer3 = System.currentTimeMillis();
//        writeLog((timer2 - timer1) + "\t" + (timer3 - timer2));
        largeTable = splitTable;

        // Estimate sequential statistics
        int firstEligibleCtr = 0;
        for (int joinCtr = 0; joinCtr < nrJoined; joinCtr++) {
            int table = order[joinCtr];
            int sizeForSeq = nrVisits[table];
            if (sizeForSeq > nrDPThreads &&
                    !query.temporaryTables.contains(table)) {
                firstEligibleCtr = joinCtr;
                break;
            }
        }

        double progress = Integer.MIN_VALUE;
        StringBuilder progressLog = new StringBuilder();
        int largeIndex = 0;
        for (int joinCtr = 0; joinCtr < nrJoined; joinCtr++) {
            int table = order[joinCtr];
            if (cardinalities[table] >= nrDPThreads &&
                    !query.temporaryTables.contains(table)
                    && nrVisits[table] > nrDPThreads) {
                double tableProgress = getSplitTableReward(order, joinCtr, firstEligibleCtr);
                progressLog.append("|").append(table).append("|").append(tableProgress);
                if (tableProgress > progress) {
                    progress = tableProgress;
                    largeTable = table;
                    largeIndex = joinCtr;
                }
            }
        }

        double reward = reward(joinOrder.order, tupleIndexDelta, offsets, state.tupleIndices);
        // Get the first table whose cardinality is larger than 1.
//        int firstTable = order[0];
        if (!state.isFinished()) {
            state.roundCtr = 0;
            threadTracker.updateProgress(joinOrder, splitHash, state,
                    roundCtr, firstTable);
            state.lastIndex = largeTable;
        }
        lastState = state;
        state.tid = tid;
        lastTable = splitTable;
        noProgressOnSplit = nrVisits[splitTable] == 0;
        if (LoggingConfig.PARALLEL_JOIN_VERBOSE) {
            writeLog("End: " + state + "\tReward: " + reward + "\tLevel: " + deepIndex
                    + "\tTime: " + System.nanoTime());
        }
        return reward;
    }

    /**
     * Execution algorithm for hybrid data parallelization.
     *
     * Specifically, only data threads will call this function.
     *
     * @param order
     * @param splitTable
     * @param roundCtr
     * @param slowState
     * @return
     * @throws Exception
     */
    public double execute(int[] order, int splitTable, int roundCtr, int nrDPThreads,
                          State slowState, LeftDeepPartitionPlan plan, IntIntMap threadMap) throws Exception {
        // Treat special case: at least one input relation is empty
        for (int tableCtr=0; tableCtr<nrJoined; ++tableCtr) {
            if (cardinalities[tableCtr]==0) {
                isFinished = true;
                return 1;
            }
        }
//        System.arraycopy(new int[]{4, 5, 0, 1, 2, 3}, 0, order, 0, nrJoined);
//        splitTable = 2;
        this.roundCtr = roundCtr;
        // Lookup or generate left-deep query plan
        JoinOrder joinOrder = new JoinOrder(order);
        int splitHash = nrDPThreads == 1 ? 0 : plan.splitStrategies[splitTable];
        int shiftID = threadMap.get(tid);
        // Execute from ing state, save progress, return progress
        State state = threadTracker.continueFrom(joinOrder, splitHash);
//        System.arraycopy(tracker.tableOffset, 0, offsets, 0, nrJoined);
        int firstTable = getFirstLargeTable(order);
        int tableOffset = slowState == null ? 0 : (slowState.tupleIndices[firstTable] - 1);
        offsets[firstTable] = Math.max(offsets[firstTable], tableOffset);
        if (slowState != null && state.isAhead(order, slowState, nrJoined)) {
            System.arraycopy(slowState.tupleIndices, 0, state.tupleIndices, 0, nrJoined);
        }
        if (LoggingConfig.PARALLEL_JOIN_VERBOSE) {
            writeLog("Data Round: " + roundCtr + "\tJoin Order: " +
                    Arrays.toString(order) + "\tSplit: " + splitTable + "\tTid: " + shiftID);
            writeLog("Start progress: " + state + " Slow: " + slowState);
        }
        long timer2 = System.currentTimeMillis();
        boolean forward = false;
        for (int i = 0; i < nrJoined; i++) {
            int table = order[i];
            if (!forward) {
                if (state.tupleIndices[table] < offsets[table]) {
                    state.tupleIndices[table] = offsets[table];
                    forward = true;
                    state.lastIndex = Math.min(state.lastIndex, i);
                }
            }
            else {
                state.tupleIndices[table] = Math.max(0, offsets[table]);
            }
        }
        executeWithBudget(plan, splitTable, state, offsets, shiftID, nrDPThreads);
        largeTable = splitTable;

        // Estimate sequential statistics
        int firstEligibleCtr = 0;
        for (int joinCtr = 0; joinCtr < nrJoined; joinCtr++) {
            int table = order[joinCtr];
            int sizeForSeq = nrVisits[table];
            if (sizeForSeq > nrDPThreads &&
                    !query.temporaryTables.contains(table)) {
                firstEligibleCtr = joinCtr;
                break;
            }
        }

        double progress = Integer.MIN_VALUE;
        StringBuilder progressLog = new StringBuilder();
        int largeIndex = 0;
        for (int joinCtr = 0; joinCtr < nrJoined; joinCtr++) {
            int table = order[joinCtr];
            if (cardinalities[table] >= nrDPThreads &&
                    !query.temporaryTables.contains(table)
                    && nrVisits[table] > nrDPThreads) {
                double tableProgress = getSplitTableReward(order, joinCtr, firstEligibleCtr);
                progressLog.append("|").append(table).append("|").append(tableProgress);
                if (tableProgress > progress) {
                    progress = tableProgress;
                    largeTable = table;
                    largeIndex = joinCtr;
                }
            }
        }

        double reward = reward(joinOrder.order, tupleIndexDelta, offsets, state.tupleIndices);
        // Get the first table whose cardinality is larger than 1.
//        int firstTable = order[0];
        if (!state.isFinished()) {
            state.roundCtr = 0;
            threadTracker.updateProgress(joinOrder, splitHash, state,
                    roundCtr, firstTable);
            state.lastIndex = largeTable;
        }
        lastState = state;
        state.tid = tid;
        lastTable = splitTable;
        noProgressOnSplit = nrVisits[splitTable] == 0;
        if (LoggingConfig.PARALLEL_JOIN_VERBOSE) {
            writeLog("Visit: " + Arrays.toString(nrVisits) + "\tLarge: " + largeTable + "\t" + progressLog +
                    "\t" + Arrays.toString(downOps) + "\t" + Arrays.toString(upOps));
            writeLog("End: " + state + "\tReward: " + reward + "\tLevel: " + deepIndex
                    + "\tTime: " + System.nanoTime());
        }
        return reward;
    }

    public double execute(int[] order, int splitTable, int roundCtr,
                          boolean[][] finishFlags, State slowState) throws Exception {
        return 0;
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
    boolean evaluateInScope(List<JoinPartitionIndexWrapper> indexWrappers, List<NonEquiNode> preds,
                            int[] tupleIndices, int splitTable,
                            int nextTable, int tid, int nrDPThreads) {
        boolean first = true;
        boolean isSplit = nextTable == splitTable;
//        long start = System.currentTimeMillis();
        if (indexWrappers.isEmpty()) {
            if (isSplit) {
                int tuple = tupleIndices[nextTable];
                int hashedTid = (tuple / nrDPThreads + tid) % nrDPThreads;
                if (tuple % nrDPThreads != hashedTid) {
//                    long end = System.currentTimeMillis();
//                    writeLog("Join Eval: " + (end - start));
                    return false;
                }
            }
        }
        else {
            for (JoinPartitionIndexWrapper wrapper : indexWrappers) {
                if (first && isSplit) {
                    if (!wrapper.evaluateInScope(tupleIndices, tid, nrDPThreads)) {
//                        long end = System.currentTimeMillis();
//                        writeLog("Join Eval: " + (end - start));
                        return false;
                    }
                }
                else {
                    if (!wrapper.evaluate(tupleIndices)) {
//                        long end = System.currentTimeMillis();
//                        writeLog("Join Eval: " + (end - start));
                        return false;
                    }
                }
                first = false;
            }
        }
        // evaluate non-equi join predicates
        boolean nonEquiResults = true;
        for (NonEquiNode pred : preds) {
            if (!pred.evaluate(tupleIndices, nextTable, cardinalities[nextTable])) {
                nonEquiResults = false;
                break;
            }
        }
//        long end = System.currentTimeMillis();
//        writeLog("Non Eval: " + (end - start));
//        if (!preds.isEmpty()) {
//            boolean another = boolEval.evaluate(tupleIndices) > 0;
//            if (another != nonEquiResults) {
//                try {
//                    Materialize.materializeTupleIndices(preSummary.columnMapping, tupleIndices, query);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//                System.out.println("Wrong");
//            }
//        }
        return nonEquiResults;
    }

    boolean evaluate(List<JoinPartitionIndexWrapper> indexWrappers, List<NonEquiNode> preds,
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

    int nextTuple(int[] joinOrder, int splitTable, List<JoinPartitionIndexWrapper> indexWrappers,
                  int curIndex, int[] tupleIndices, int tid, int nrDPThreads) {
        int nextTable = joinOrder[curIndex];
        int nextCardinality = cardinalities[nextTable];
        int tuple = tupleIndices[nextTable];
        // If there is no equi-predicates.
        boolean isSplit = nextTable == splitTable;
        if (indexWrappers.isEmpty()) {
            if (isSplit) {
                int bucket = tuple / nrDPThreads;
                int hashedTid = (bucket + tid) % nrDPThreads;
                int mod = tuple % nrDPThreads;
                if (hashedTid > mod) {
                    int jump = hashedTid - mod;
                    tuple += jump;
                }
                else {
                    hashedTid = (hashedTid + 1) % nrDPThreads;
                    tuple = (bucket + 1) * nrDPThreads + hashedTid;
                }
            }
            else {
                tuple += 1;
            }
        }
        else {
            boolean first = true;
            for (JoinPartitionIndexWrapper wrapper : indexWrappers) {
                if (isSplit) {
                    if (!first) {
                        if (wrapper.evaluate(tupleIndices)) {
                            continue;
                        }
                    }
                    int nextRaw = first ? wrapper.nextIndexInScope(tupleIndices, tid, nrDPThreads, this.nrVisits):
                            wrapper.nextIndex(tupleIndices, this.nrVisits);
                    if (nextRaw < 0 || nextRaw == nextCardinality) {
                        tuple = nextCardinality;
                        break;
                    }
                    else {
                        tuple = nextRaw;
                    }
                    first = false;
                }
                else {
                    if (!first) {
                        if (wrapper.evaluate(tupleIndices)) {
                            continue;
                        }
                    }
                    int nextRaw = wrapper.nextIndex(tupleIndices, this.nrVisits);
                    if (nextRaw < 0 || nextRaw == nextCardinality) {
                        tuple = nextCardinality;
                        break;
                    }
                    else {
                        tuple = nextRaw;
                    }
                    first = false;
                }
            }
        }
        return tuple;
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
                    int curIndex, int[] tupleIndices, int tid, int nrDPThreads,
                           List<List<JoinPartitionIndexWrapper>> joinIndices) {
        int nextTable = joinOrder[curIndex];
        int nextCardinality = cardinalities[nextTable];
        // If there is no equi-predicates.
        boolean isSplit = nextTable == splitTable;
        if (indexWrappers.isEmpty()) {
            if (isSplit) {
                int tuple = tupleIndices[nextTable];
                int bucket = tuple / nrDPThreads;
                int hashedTid = (bucket + tid) % nrDPThreads;
                int mod = tuple % nrDPThreads;
                if (hashedTid > mod) {
                    int jump = hashedTid - mod;
                    tupleIndices[nextTable] += jump;
                }
                else {
                    hashedTid = (hashedTid + 1) % nrDPThreads;
                    tupleIndices[nextTable] = (bucket + 1) * nrDPThreads + hashedTid;
                }
            }
            else {
                tupleIndices[nextTable]++;
            }
            this.nrVisits[nextTable] = cardinalities[nextTable];
        }
        else {
            boolean first = true;
            int preSize = Integer.MAX_VALUE;
            for (JoinPartitionIndexWrapper wrapper : indexWrappers) {
                if (isSplit) {
                    if (!first) {
                        if (wrapper.evaluate(tupleIndices)) {
                            preSize = Math.min(wrapper.nrIndexed(tupleIndices), preSize);
                            continue;
                        }
                    }
                    int nextRaw = first ? wrapper.nextIndexInScope(tupleIndices, tid, nrDPThreads, this.nrVisits):
                            wrapper.nextIndex(tupleIndices, this.nrVisits);
                    preSize = Math.min(this.nrVisits[nextTable], preSize);
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
                    if (!first) {
                        if (wrapper.evaluate(tupleIndices)) {
                            preSize = Math.min(wrapper.nrIndexed(tupleIndices), preSize);
                            continue;
                        }
                    }
                    int nextRaw = wrapper.nextIndex(tupleIndices, this.nrVisits);
                    preSize = Math.min(this.nrVisits[nextTable], preSize);
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
            this.nrVisits[nextTable] = preSize;
        }
        downOps[nextTable]++;

        // Have reached end of current table? -> we backtrack.
        while (tupleIndices[nextTable] >= nextCardinality) {
            tupleIndices[nextTable] = 0;
            upOps[nextTable]++;
            --curIndex;
            if (curIndex < 0) {
                break;
            }
            nextTable = joinOrder[curIndex];
            nextCardinality = cardinalities[nextTable];
            tupleIndices[nextTable] += 1;
//            tupleIndices[nextTable] = nextTuple(joinOrder, splitTable,
//                    joinIndices.get(curIndex), curIndex, tupleIndices, tid, nrDPThreads);
            // Update statistics
            this.nrVisits[nextTable] = cardinalities[nextTable];
            downOps[nextTable]++;
        }
        return curIndex;
    }

    /**
     * Propose next tuple index to consider, based on a set of
     * indices on the join column.
     *
     * @param indexWrappersList	list of join index wrappers
     * @param tupleIndices	current tuple indices
     * @return				next join index
     */
    int proposeNext(int[] joinOrder, List<List<JoinPartitionIndexWrapper>> indexWrappersList,
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
    private void executeWithBudget(LeftDeepPartitionPlan plan, int splitTable,
                                   State state, int[] offsets, int tid, int nrDPThreads) {
        // Extract variables for convenient access
        int nrTables = query.nrJoined;
        int[] tupleIndices = new int[nrTables];
        List<List<JoinPartitionIndexWrapper>> joinIndices = plan.joinIndices;
//        List<List<KnaryBoolEval>> applicablePreds = plan.applicablePreds;
        List<List<NonEquiNode>> applicablePreds = plan.nonEquiNodes;
        // Initialize state and flags to prepare budgeted execution
//        int joinIndex = state.lastIndex;
        System.arraycopy(state.tupleIndices, 0, tupleIndices, 0, nrTables);
        int joinIndex = 0;
        int lastCheck = nrTables - 1;
        for (int i = 0; i <= lastCheck; i++) {
            int table = plan.joinOrder.order[i];
            joinIndex = i;
//            writeLog("Pre Check: " + Arrays.toString(tupleIndices));
            if (query.temporaryTables.contains(table)) {
                tupleIndices[table] = offsets[table];
            }
            if (!evaluateInScope(joinIndices.get(i), applicablePreds.get(i),
                    tupleIndices, splitTable, table, tid, nrDPThreads)) {
                for (int back = joinIndex + 1; back < nrTables; back++) {
                    int backTable = plan.joinOrder.order[back];
                    tupleIndices[backTable] = 0;
                }
                joinIndex = proposeNextInScope(
                        plan.joinOrder.order, splitTable, joinIndices.get(joinIndex),
                        joinIndex, tupleIndices, tid, nrDPThreads, joinIndices);
//                writeLog("Pre Propose Next: " + Arrays.toString(tupleIndices));
                break;
            }
            else if (joinIndex == nrTables - 1){
                ++nrResultTuples;
                if (uniqueJoinResult != null) {
                    uniqueJoinResult.add(tupleIndices);
                }
                else {
                    result.add(tupleIndices);
                }
//                writeLog("INFO:Bingo: " + Arrays.toString(tupleIndices));
                joinIndex = proposeNextInScope(
                        plan.joinOrder.order, splitTable, joinIndices.get(joinIndex),
                        joinIndex, tupleIndices, tid, nrDPThreads, joinIndices);
//                writeLog("Pre Propose Next: " + Arrays.toString(tupleIndices));
//                writeLog("Pre Propose Next: " + Arrays.toString(tupleIndices));
            }
        }

        int remainingBudget = budget;
//        int remainingBudget = Integer.MAX_VALUE;
        // Number of completed tuples added
        nrResultTuples = 0;
        Arrays.fill(this.nrVisits, 0);
        Arrays.fill(this.downOps, 0);
        Arrays.fill(this.upOps, 0);
        deepIndex = -1;
//        writeLog("Start: " + Arrays.toString(tupleIndices) + " " + joinIndex);
        System.arraycopy(tupleIndices, 0, state.tupleIndices, 0, nrTables);
        // Execute join order until budget depleted or all input finished -
        // at each iteration start, tuple indices contain next tuple
        // combination to look at.
        while (remainingBudget > 0 && joinIndex >= 0) {
//            long startTimer = System.currentTimeMillis();
//            ++statsInstance.nrIterations;
            //log("Offsets:\t" + Arrays.toString(offsets));
            //log("Indices:\t" + Arrays.toString(tupleIndices));
            // Get next table in join order
            int nextTable = plan.joinOrder.order[joinIndex];
            deepIndex = Math.max(deepIndex, joinIndex);
//            writeLog("Indices: " + Arrays.toString(tupleIndices) + "; Join: " + joinIndex);
//            writeLog("Indices: " + Arrays.toString(tupleIndices));
//            writeLog("Budget: " + remainingBudget + "\tCheck: " + Arrays.toString(tupleIndices));
//            writeLog("Budget: " + remainingBudget + "\tCheck: " + Arrays.toString(tupleIndices));
            // Integrate table offset
            tupleIndices[nextTable] = Math.max(
                    offsets[nextTable], tupleIndices[nextTable]);
            // Evaluate all applicable predicates on joined tuples
            KnaryBoolEval unaryPred = unaryPreds[nextTable];
            if ((PreConfig.FILTER || unaryPred == null ||
                    unaryPred.evaluate(tupleIndices)>0) &&
//                    evaluateAll(applicablePreds.get(joinIndex), tupleIndices)
                    evaluateInScope(joinIndices.get(joinIndex), applicablePreds.get(joinIndex),
                            tupleIndices, splitTable, nextTable, tid, nrDPThreads)
            ) {
                ++statsInstance.nrTuples;
//                long start1 = System.currentTimeMillis();
                // Do we have a complete result row?
                if(joinIndex == plan.joinOrder.order.length - 1) {
                    // Complete result row -> add to result
                    ++nrResultTuples;
                    if (uniqueJoinResult != null) {
                        uniqueJoinResult.add(tupleIndices);
                    }
                    else {
                        result.add(tupleIndices);
                    }
//                    writeLog("INFO:Bingo: " + Arrays.toString(tupleIndices));
                    joinIndex = proposeNextInScope(
                            plan.joinOrder.order, splitTable, joinIndices.get(joinIndex),
                            joinIndex, tupleIndices, tid, nrDPThreads, joinIndices);
//                    long end1 = System.currentTimeMillis();
//                    writeLog("Iteration: " + (start1 - startTimer) + " " + (end1 - start1));
//                    writeLog(String.valueOf(end - start));
//                    writeLog("Budget: " + remainingBudget + "\tPropose Next: " + Arrays.toString(tupleIndices));
//                    writeLog("Budget: " + remainingBudget + "\tPropose Next: " + Arrays.toString(tupleIndices));
                } else {
                    // No complete result row -> complete further
                    joinIndex++;
                }
            } else {
                // At least one of applicable predicates evaluates to false -
                // try next tuple in same table.
//                long start1 = System.currentTimeMillis();
                joinIndex = proposeNextInScope(
                        plan.joinOrder.order, splitTable, joinIndices.get(joinIndex),
                        joinIndex, tupleIndices, tid, nrDPThreads, joinIndices);
//                long end1 = System.currentTimeMillis();
//                writeLog(String.valueOf(end - start));
//                writeLog("Budget: " + remainingBudget + "\tPropose Next: " + Arrays.toString(tupleIndices));
//                writeLog("Budget: " + remainingBudget + "\tPropose Next: " + Arrays.toString(tupleIndices));
//                writeLog("Iteration: " + (start1 - startTimer) + " " + (end1 - start1));
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

    private void executeWithBudget(LeftDeepPartitionPlan plan, State state, int[] offsets) {
        // Extract variables for convenient access
        int nrTables = query.nrJoined;
        int[] tupleIndices = new int[nrTables];
        List<List<JoinPartitionIndexWrapper>> joinIndices = plan.joinIndices;
//        List<List<KnaryBoolEval>> applicablePreds = plan.applicablePreds;
        List<List<NonEquiNode>> applicablePreds = plan.nonEquiNodes;
        // Initialize state and flags to prepare budgeted execution
//        int joinIndex = state.lastIndex;
        System.arraycopy(state.tupleIndices, 0, tupleIndices, 0, nrTables);
        int joinIndex = 0;
        int lastCheck = nrTables - 1;
        for (int i = 0; i <= lastCheck; i++) {
            int table = plan.joinOrder.order[i];
            joinIndex = i;
            if (query.temporaryTables.contains(table)) {
                tupleIndices[table] = offsets[table];
            }
            if (!evaluate(joinIndices.get(i), applicablePreds.get(i),
                    tupleIndices, table)) {
                for (int back = joinIndex + 1; back < nrTables; back++) {
                    int backTable = plan.joinOrder.order[back];
                    tupleIndices[backTable] = 0;
                }
                joinIndex = proposeNext(
                        plan.joinOrder.order, joinIndices, joinIndex, tupleIndices, true);
                break;
            }
            else if (joinIndex == nrTables - 1){
                ++nrResultTuples;
                if (uniqueJoinResult != null) {
                    uniqueJoinResult.add(tupleIndices);
                }
                else {
                    result.add(tupleIndices);
                }
//                writeLog("INFO:Bingo: " + Arrays.toString(tupleIndices));
                joinIndex = proposeNext(
                        plan.joinOrder.order, joinIndices, joinIndex, tupleIndices, false);
            }
        }

        int remainingBudget = budget;
//        int remainingBudget = Integer.MAX_VALUE;
        // Number of completed tuples added
        nrResultTuples = 0;
        Arrays.fill(this.nrVisits, 0);
        Arrays.fill(this.downOps, 0);
        Arrays.fill(this.upOps, 0);
        deepIndex = -1;
//        writeLog("Start: " + Arrays.toString(tupleIndices) + " " + joinIndex);
        System.arraycopy(tupleIndices, 0, state.tupleIndices, 0, nrTables);
        // Execute join order until budget depleted or all input finished -
        // at each iteration start, tuple indices contain next tuple
        // combination to look at.
        while (remainingBudget > 0 && joinIndex >= 0) {
//            ++statsInstance.nrIterations;
            //log("Offsets:\t" + Arrays.toString(offsets));
            //log("Indices:\t" + Arrays.toString(tupleIndices));
            // Get next table in join order
            int nextTable = plan.joinOrder.order[joinIndex];
            deepIndex = Math.max(deepIndex, joinIndex);
//            writeLog("Indices: " + Arrays.toString(tupleIndices) + "; Join: " + joinIndex);
//            writeLog("Indices: " + Arrays.toString(tupleIndices));
            // Integrate table offset
            tupleIndices[nextTable] = Math.max(
                    offsets[nextTable], tupleIndices[nextTable]);
            // Evaluate all applicable predicates on joined tuples
            KnaryBoolEval unaryPred = unaryPreds[nextTable];
            if ((PreConfig.FILTER || unaryPred == null ||
                    unaryPred.evaluate(tupleIndices)>0) &&
//                    evaluateAll(applicablePreds.get(joinIndex), tupleIndices)
                    evaluate(joinIndices.get(joinIndex), applicablePreds.get(joinIndex),
                            tupleIndices, nextTable)
            ) {
                ++statsInstance.nrTuples;
                // Do we have a complete result row?
                if(joinIndex == plan.joinOrder.order.length - 1) {
                    // Complete result row -> add to result
                    ++nrResultTuples;
                    if (uniqueJoinResult != null) {
                        uniqueJoinResult.add(tupleIndices);
                    }
                    else {
                        result.add(tupleIndices);
                    }
//                    writeLog("INFO:Bingo: " + Arrays.toString(tupleIndices));
                    joinIndex = proposeNext(
                            plan.joinOrder.order, joinIndices, joinIndex, tupleIndices, true);
                } else {
                    // No complete result row -> complete further
                    joinIndex++;
                }
            } else {
                // At least one of applicable predicates evaluates to false -
                // try next tuple in same table.
                joinIndex = proposeNext(
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

    public double getSplitTableReward(int[] joinOrder, int splitCtr, int firstCtr) {
//        int nrJoined = 10;
//        int[] nrVisits = new int[]{1, 1, 15, 0, 0, 589, 47, 1, 0, 1};
//        joinOrder = new int[]{3, 5, 9, 6, 4, 2, 7, 8, 1, 0};
//        double[] downOps = new double[]{10.0, 25.0, 41.0, 0.0, 165.0, 4.0, 191.0, 40.0, 12.0, 6.0};
//        double[] upOps = new double[]{6.0, 12.0, 10.0, 0.0, 165.0, 0.0, 4.0, 15.0, 12.0, 2.0};
//        int nrThreads = 24;
//        splitCtr = 3;
//        firstCtr = 1;
        int table = joinOrder[splitCtr];
        double upOperations = upOps[table];
        double downOperations = downOps[table];
        double firstDown = downOps[joinOrder[firstCtr]];
        double indexSize = nrVisits[table];
        double splitDown = downOperations * nrThreads;
        double splitUpDelta = upOperations > 0 ? upOperations * (nrThreads - 1) : (int)(splitDown / indexSize);
        double progress = splitDown;
        for (int joinIndex = splitCtr - 1; joinIndex >= firstCtr; joinIndex--) {
            table = joinOrder[joinIndex];
            indexSize = nrVisits[table];

            upOperations = upOps[table];
            downOperations = downOps[table];
            if (downOperations == 0 || indexSize < nrThreads) {
                return firstDown;
            }
            splitDown = downOperations + splitUpDelta;
            splitUpDelta = upOperations > 0 ? (int)(upOperations /
                    downOperations * splitUpDelta) : (int)(splitDown / indexSize);
            if (splitUpDelta == 0) {
                return joinIndex == firstCtr ? splitDown : firstDown;
            }
            else {
                progress = splitDown;
            }
        }
//        double progress = 0;
//        double weight = 1.0;
//        for (int joinIndex = 0; joinIndex < nrJoined; joinIndex++) {
//            int table = joinOrder[joinIndex];
//            int indexSize = nrVisits[table];
//            if (indexSize > nrThreads) {
//                double downOperations = downOps[table];
//                double upOperations = upOps[table];
//                weight = weight * (1.0 / indexSize);
//                double tableProgress = (weight * (table == splitTable ? (nrThreads * downOperations - indexSize * upOperations) :
//                        (downOperations - indexSize * upOperations)));
//                progress = progress + tableProgress;
//            }
//        }

        return progress;
    }
}
