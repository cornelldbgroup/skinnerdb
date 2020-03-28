package joining.parallel.join;

import config.*;
import expressions.compilation.KnaryBoolEval;
import joining.parallel.indexing.IntIndexRange;
import joining.parallel.plan.LeftDeepPartitionPlan;
import joining.parallel.progress.ParallelProgressTracker;
import joining.plan.JoinOrder;
import joining.progress.State;
import net.sf.jsqlparser.expression.Expression;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;
import predicate.NonEquiNode;
import preprocessing.Context;
import query.QueryInfo;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class FixJoin extends SPJoin {
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
     * Whether to use a fixed join.
     */
    public boolean isFixed = false;
    /**
     * The termination flag
     */
    public final AtomicBoolean terminate;
    /**
     * The number of threads for executor
     */
    public final int nrWorking = ParallelConfig.EXE_EXECUTORS;
    /**
     * Thread pool for each executor
     */
    public final ExecutorService executorService;
    /**
     * The first table that is larger than 1.
     */
    private int firstTable;
    /**
     * Initializes join algorithm for given input query.
     *
     * @param query			query to process
     * @param preSummary	summary of pre-processing
     * @param budget		budget per episode
     */
    public FixJoin(QueryInfo query, Context preSummary,
                   int budget, int nrThreads, int tid, Map<Expression, NonEquiNode> predToEval) throws Exception {
        super(query, preSummary, budget, nrThreads, tid, predToEval);
        this.planCache = new HashMap<>();
        // Collect unary predicates
        this.unaryPreds = new KnaryBoolEval[nrJoined];
        this.tupleIndexDelta = new int[nrJoined];
        this.terminate = new AtomicBoolean(false);
        this.executorService = Executors.newFixedThreadPool(nrWorking);
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
    public double execute(int[] order, int roundCtr) throws Exception {
        // Treat special case: at least one input relation is empty
        for (int tableCtr=0; tableCtr<nrJoined; ++tableCtr) {
            if (cardinalities[tableCtr]==0) {
                isFinished = true;
                return 1;
            }
        }
//        order = new int[]{8, 2, 5, 1, 7, 9, 3, 6, 4, 0};
//        order = new int[]{3, 10, 11, 7, 0, 8, 9, 2, 1, 6, 13, 5, 12, 4};
        // Lookup or generate left-deep query plan
        JoinOrder joinOrder = new JoinOrder(order);
        int joinHash = joinOrder.splitHashCode(-1);
        LeftDeepPartitionPlan plan = planCache.get(joinHash);
        if (plan == null) {
            plan = new LeftDeepPartitionPlan(query, predToEval, joinOrder);
            planCache.putIfAbsent(joinHash, plan);
        }

//        long timer1 = System.currentTimeMillis();
        // Execute from ing state, save progress, return progress
        this.firstTable = getFirstLargeTable(order);

        State state = tracker.continueFromSP(joinOrder, tid, firstTable);
        if (LoggingConfig.PARALLEL_JOIN_VERBOSE) {
            writeLog("Round: " + roundCtr + "\t" + "Join: " + Arrays.toString(order));
        }
        int[] offsets;
        if (JoinConfig.OFFSETS_SHARING) {
            offsets = Arrays.copyOf(tracker.tableOffset, nrJoined);
        }
        else {
            offsets = Arrays.copyOf(tracker.tableOffsetMaps[0][0], nrJoined);
        }
//        writeLog("Start: " + state.toString());
//        writeLog("Offset: " + Arrays.toString(offsets));
//        long timer2 = System.currentTimeMillis();
        // move the indices into correct position
        boolean forward = false;
        for (int i = 0; i < nrJoined; i++) {
            int table = order[i];
            if (!forward) {
                if (state.tupleIndices[table] < offsets[table]) {
                    forward = true;
                }
            }
            else {
                state.tupleIndices[table] = 0;
            }
            plan.joinIndices.get(i).forEach(index ->index.reset(state.tupleIndices));
        }
        if (isFixed) {
            Arrays.fill(state.tupleIndices , 0);
//            executeFixedJoin(plan, state, offsets);
            terminate.set(false);
            executeParallelFixedJoin(plan, state, offsets);
        }
        else {
            executeWithBudget(plan, state, offsets);
            // progress in the left table.
            double reward = reward(joinOrder.order, tupleIndexDelta, offsets, state.tupleIndices);
            // Get the first table whose cardinality is larger than 1.
            state.roundCtr = 0;
            tracker.updateProgressSP(joinOrder, state, tid, roundCtr, firstTable);
            lastState = state;
            return reward;
        }
//        long timer3 = System.currentTimeMillis();
        lastState = state;
//        long timer5 = System.currentTimeMillis();
//        writeLog((timer5 - timer1) + "\t" + (timer2 - timer1) + "\t" + (timer3 - timer2)
//                + "\t" + (timer4 - timer3) + "\t" + (timer5 - timer4));
        return 0;
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

//        if (!preds.isEmpty()) {
//            boolean another = boolEval.evaluate(tupleIndices) > 0;
//            if (!another) {
//                try {
//                    Materialize.materializeTupleIndices(preSummary.columnMapping, tupleIndices, query);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//                System.out.println("Wrong");
//            }
//        }
        return true;
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
                           int curIndex, int[] tupleIndices) {
        int nextTable = joinOrder[curIndex];
        int nextCardinality = cardinalities[nextTable];
        List<JoinPartitionIndexWrapper> indexWrappers = indexWrappersList.get(curIndex);
        // If there is no equi-predicates.
        if (indexWrappers.isEmpty()) {
            tupleIndices[nextTable]++;
        }
        else {
            boolean first = true;
//            long timer0 = System.currentTimeMillis();
            for (JoinPartitionIndexWrapper wrapper : indexWrappers) {
//                long timer10 = System.currentTimeMillis();
                if (!first) {
                    if (wrapper.evaluate(tupleIndices)) {
//                        long timer11 = System.currentTimeMillis();
//                        time.add("" + (timer11 - timer10));
                        continue;
                    }
                }
//                long timer11 = System.currentTimeMillis();
//                int nextRaw = wrapper.nextIndex(tupleIndices, null);
                int nextRaw = wrapper.nextIndexFromLast(tupleIndices, null, tid);
//                long timer12 = System.currentTimeMillis();
                if (nextRaw < 0 || nextRaw == nextCardinality) {
                    tupleIndices[nextTable] = nextCardinality;
//                    long timer13 = System.currentTimeMillis();
//                    time.add((timer11 - timer10) + "\t" + (timer12 - timer11) + "\t: " + (timer13 - timer12));
                    break;
                }
                else {
                    tupleIndices[nextTable] = nextRaw;
                }
                first = false;
//                long timer13 = System.currentTimeMillis();
//                time.add((timer11 - timer10) + "\t" + (timer12 - timer11) + "\t" + (timer13 - timer12));
            }
//            long timer1 = System.currentTimeMillis();
//            if (timer1 - timer0 > 100) {
//                StringBuilder wrapperStr = new StringBuilder();
//                for (JoinPartitionIndexWrapper wrapper: indexWrappers) {
//                    wrapperStr.append(wrapper.toString());
//                }
//                for (JoinPartitionIndexWrapper wrapper : indexWrappers) {
//                    writeLog("Index Size: " + wrapper.nrIndexed(tupleIndices));
//                }
//                writeLog(Arrays.toString(time.toArray()) + "\t" + (timer1 - timer0));
//                writeLog(wrapperStr.toString());
//            }
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
        // Number of completed tuples added
        nrResultTuples = 0;
        // Execute join order until budget depleted or all input finished -
        // at each iteration start, tuple indices contain next tuple
        // combination to look at.
        while (remainingBudget > 0 && joinIndex >= 0) {
            //log("Offsets:\t" + Arrays.toString(offsets));
            //log("Indices:\t" + Arrays.toString(tupleIndices));
            // Get next table in join order
            int nextTable = plan.joinOrder.order[joinIndex];
//            if (tupleIndices[nextTable] == 0) {
//                int size = nrIndexed(plan.joinOrder.order, joinIndices.get(joinIndex), joinIndex, tupleIndices);
//                nrIndexed[nextTable] = size;
//                nrVisited[nextTable]++;
//            }
            // Integrate table offset
            tupleIndices[nextTable] = Math.max(
                    offsets[nextTable], tupleIndices[nextTable]);
            // Evaluate all applicable predicates on joined tuples
            KnaryBoolEval unaryPred = unaryPreds[nextTable];
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
                    result.add(tupleIndices);
//                    writeLog("INFO:Bingo: " + Arrays.toString(tupleIndices));
                    joinIndex = proposeNextInScope(
                            plan.joinOrder.order, joinIndices, joinIndex, tupleIndices);
                } else {
                    // No complete result row -> complete further
                    joinIndex++;
                    //System.out.println("Current Join Index2:"+ joinIndex);
                }
            } else {
                // At least one of applicable predicates evaluates to false -
                // try next tuple in same table.
                joinIndex = proposeNextInScope(
                        plan.joinOrder.order, joinIndices, joinIndex, tupleIndices);

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

//    private void executeFixedJoin(LeftDeepPartitionPlan plan, State state, int[] offsets) {
//        // Extract variables for convenient access
//        int nrTables = query.nrJoined;
//        int[] tupleIndices = new int[nrTables];
//        List<List<JoinPartitionIndexWrapper>> joinIndices = plan.joinIndices;
////        List<List<KnaryBoolEval>> applicablePreds = plan.applicablePreds;
//        List<List<NonEquiNode>> applicablePreds = plan.nonEquiNodes;
//        // Initialize state and flags to prepare budgeted execution
//        System.arraycopy(state.tupleIndices, 0, tupleIndices, 0, nrTables);
//        // Number of completed tuples added
//        nrResultTuples = 0;
//
//        // Execute join order until budget depleted or all input finished -
//        // at each iteration start, tuple indices contain next tuple
//        // combination to look at.
//        int firstTable = plan.joinOrder.order[0];
//        int start = Math.max(offsets[firstTable], tupleIndices[firstTable]);
//        for (int firstTuple = start; firstTuple < cardinalities[firstTable]; firstTuple++) {
//            List<int[]> intermediateResults = new LinkedList<>();
//            // Integrate table offset
//            tupleIndices[firstTable] = firstTuple;
//            intermediateResults.add(tupleIndices.clone());
//            for (int nextIndex = 1; nextIndex < nrTables; nextIndex++) {
//                int nextTable = plan.joinOrder.order[nextIndex];
//                // Integrate table offset
//                tupleIndices[nextTable] = Math.max(
//                        offsets[nextTable], tupleIndices[nextTable]);
//                Iterator<int[]> resultsIter = intermediateResults.iterator();
//                List<int[]> curResult = new LinkedList<>();
//
//                int nextCardinality = cardinalities[nextTable];
//                int curTuple = tupleIndices[nextTable];
//                List<JoinPartitionIndexWrapper> indexWrappers = joinIndices.get(nextIndex);
//                // If there is no equi-predicates.
//                if (indexWrappers.isEmpty()) {
//                    while (resultsIter.hasNext()) {
//                        int[] tuples = resultsIter.next();
//                        for (int i = curTuple; i < nextCardinality; i++) {
//                            tuples[nextTable] = i;
//                            curResult.add(tuples.clone());
//                        }
//                    }
//                }
//                else {
//                    while (resultsIter.hasNext()) {
//                        int[] tuples = resultsIter.next();
//                        tuples[nextTable] = curTuple;
//                        // get smallest index
//                        int small = Integer.MAX_VALUE;
//                        int index = 0;
//                        for (int i = 0; i < indexWrappers.size(); i++) {
//                            int size = indexWrappers.get(i).indexSize(tuples);
//                            if (size < small) {
//                                small = size;
//                                index = i;
//                            }
//                        }
//                        JoinPartitionIndexWrapper prob = indexWrappers.remove(index);
//                        int startPos = prob.lastPositionsStart;
//                        int endPos = prob.lastPositionsEnd;
//                        int[] positions = prob.nextIndex.positions;
//
//                        boolean firstPass = true;
//                        for (JoinPartitionIndexWrapper wrapper : indexWrappers) {
//                            if (!wrapper.evaluate(tuples)) {
//                                firstPass = false;
//                                break;
//                            }
//                        }
//                        if (firstPass && prob.evaluate(tuples)) {
//                            if (nextIndex == nrTables - 1) {
//                                result.add(tuples);
//                            }
//                            else {
//                                curResult.add(tuples.clone());
//                            }
//                        }
//                        for (int pos = startPos; pos <= endPos; pos++) {
//                            int nextTuple = prob.nextIndex.unique ? prob.lastFirst : positions[pos];
//                            tuples[nextTable] = nextTuple;
//                            boolean pass = true;
//                            for (JoinPartitionIndexWrapper wrapper : indexWrappers) {
//                                if (!wrapper.evaluate(tuples)) {
//                                    pass = false;
//                                    break;
//                                }
//                            }
//                            if (pass) {
//                                if (nextIndex == nrTables - 1) {
//                                    result.add(tuples);
//                                }
//                                else {
//                                    curResult.add(tuples.clone());
//                                }
//                            }
//                        }
//                        indexWrappers.add(prob);
//                    }
//                    if (flag.get()) {
//                        return;
//                    }
//                }
//                intermediateResults = curResult;
//            }
//        }
//    }


    private void executeParallelFixedJoin(LeftDeepPartitionPlan plan, State state, int[] offsets) {
        // Extract variables for convenient access
        int nrTables = query.nrJoined;
        int[] tupleIndices = new int[nrTables];
        List<List<JoinPartitionIndexWrapper>> joinIndices = plan.joinIndices;
        // Initialize state and flags to prepare budgeted execution
        System.arraycopy(state.tupleIndices, 0, tupleIndices, 0, nrTables);
        // Number of completed tuples added
        nrResultTuples = 0;

        // Execute join order until budget depleted or all input finished -
        // at each iteration start, tuple indices contain next tuple
        // combination to look at.
        int firstTable = plan.joinOrder.order[0];
        int start = Math.max(offsets[firstTable], tupleIndices[firstTable]);
        List<int[]> intermediateResults = new ArrayList<>(cardinalities[firstTable] - start);
        int firstIndex = 0;
        while (plan.joinOrder.order[firstIndex] != this.firstTable) {
            firstIndex++;
        }

        for (int firstTuple = start; firstTuple < cardinalities[firstTable]; firstTuple++) {
            tupleIndices[firstTable] = firstTuple;
            intermediateResults.add(tupleIndices.clone());
        }
        for (int nextIndex = 1; nextIndex < nrTables; nextIndex++) {
            int nextTable = plan.joinOrder.order[nextIndex];
            // Integrate table offset
            tupleIndices[nextTable] = Math.max(
                    offsets[nextTable], tupleIndices[nextTable]);
            // some join statistics for current index
            int nextCardinality = cardinalities[nextTable];
            int curTuple = tupleIndices[nextTable];
            List<JoinPartitionIndexWrapper> indexWrappers = joinIndices.get(nextIndex);
            final boolean isEmpty = indexWrappers.isEmpty();
            List<int[]> finalIntermediateResults = intermediateResults;
            // partition the input table
            List<IntIndexRange> ranges = this.split(finalIntermediateResults.size());
            List<int[]> curResults = new ArrayList<>();
            if (ranges.size() < nrWorking) {
                int[] points = new int[2];
                List<int[]> newPointsList = new ArrayList<>();
                for (int i = 0; i < indexWrappers.size(); i++) {
                    newPointsList.add(new int[2]);
                }
                if (isEmpty) {
                    for (int[] tuples: finalIntermediateResults) {
                        for (int tuple = curTuple; tuple < nextCardinality; tuple++) {
                            tuples[nextTable] = tuple;
                            curResults.add(tuples.clone());
                        }
                        if (terminate.get()) {
                            state.lastIndex = nextIndex;
                            return;
                        }
                    }
                }
                else {
                    for (int[] tuples: finalIntermediateResults) {
                        tuples[nextTable] = curTuple;
                        // get smallest index
                        int small = Integer.MAX_VALUE;
                        int index = 0;
                        for (int i = 0; i < indexWrappers.size(); i++) {
                            int[] newPoints = newPointsList.get(i);
                            int size = indexWrappers.get(i).indexSize(tuples, newPoints);
                            if (size < small) {
                                small = size;
                                index = i;
                                System.arraycopy(newPoints, 0, points, 0, 2);
                            }
                        }

                        JoinPartitionIndexWrapper prob = indexWrappers.get(index);
                        int startPos = points[0];
                        int endPos = points[1];
                        int[] positions = prob.nextIndex.positions;

                        List<JoinPartitionIndexWrapper> remainedWrappers = new ArrayList<>(indexWrappers);
                        remainedWrappers.remove(index);
                        for (int pos = startPos; pos <= endPos; pos++) {
                            int nextTuple = prob.nextIndex.unique ? pos : positions[pos];
                            tuples[nextTable] = nextTuple;
                            boolean pass = true;
                            for (JoinPartitionIndexWrapper wrapper : remainedWrappers) {
                                if (!wrapper.evaluate(tuples)) {
                                    pass = false;
                                    break;
                                }
                            }
                            if (pass) {
                                curResults.add(tuples.clone());
                            }
                        }
                        if (terminate.get()) {
                            state.lastIndex = nextIndex;
                            return;
                        }
                    }
                }
            }
            else {
                this.firstTable = nextTable;
                List<Future<List<int[]>>> futures = new ArrayList<>();
                for (IntIndexRange batch : ranges) {
                    int startIndex = batch.firstTuple;
                    int endIndex = batch.lastTuple;
                    final int indexStart = nextIndex;
                    futures.add(executorService.submit(() -> {
                        int[] points = new int[2];
                        int[] threadIndices = tupleIndices.clone();
                        List<int[]> threadFinalResults = new ArrayList<>();
                        for (int threadIndex = indexStart; threadIndex < nrTables; threadIndex++) {
                            int rStart;
                            int rEnd;
                            List<int[]> source;
                            if (threadIndex == indexStart) {
                                rStart = startIndex;
                                rEnd = endIndex;
                                source = finalIntermediateResults;
                            }
                            else {
                                rStart = 0;
                                rEnd = threadFinalResults.size() - 1;
                                source = threadFinalResults;
                            }
                            int threadTable = plan.joinOrder.order[threadIndex];
                            // Integrate table offset
                            threadIndices[threadTable] = Math.max(
                                    offsets[threadTable], threadIndices[threadTable]);
                            // some join statistics for current index
                            int threadCardinality = cardinalities[threadTable];
                            int threadTuple = threadIndices[threadTable];
                            List<JoinPartitionIndexWrapper> threadIndexWrappers = joinIndices.get(threadIndex);
                            final boolean threadEmpty = threadIndexWrappers.isEmpty();

                            List<int[]> newPointsList = new ArrayList<>();
                            for (int i = 0; i < threadIndexWrappers.size(); i++) {
                                newPointsList.add(new int[2]);
                            }
                            List<int[]> threadResults = new ArrayList<>();
                            if (threadEmpty) {
                                for (int rid = rStart; rid <= rEnd; rid++) {
                                    int[] tuples = source.get(rid);
                                    for (int tuple = threadTuple; tuple < threadCardinality; tuple++) {
                                        tuples[threadTable] = tuple;
                                        threadResults.add(tuples.clone());
                                    }
                                }
                            }
                            else {
                                for (int rid = rStart; rid <= rEnd; rid++) {
                                    int[] tuples = source.get(rid);
                                    tuples[threadTable] = threadTuple;
                                    // get smallest index
                                    int small = Integer.MAX_VALUE;
                                    int index = 0;
                                    for (int i = 0; i < threadIndexWrappers.size(); i++) {
                                        int[] newPoints = newPointsList.get(i);
                                        int size = threadIndexWrappers.get(i).indexSize(tuples, newPoints);
                                        if (size < small) {
                                            small = size;
                                            index = i;
                                            System.arraycopy(newPoints, 0, points, 0, 2);
                                        }
                                    }

                                    JoinPartitionIndexWrapper prob = threadIndexWrappers.get(index);
                                    int startPos = points[0];
                                    int endPos = points[1];
                                    int[] positions = prob.nextIndex.positions;

                                    List<JoinPartitionIndexWrapper> remainedWrappers = new ArrayList<>(threadIndexWrappers);
                                    remainedWrappers.remove(index);
                                    for (int pos = startPos; pos <= endPos; pos++) {
                                        int nextTuple = prob.nextIndex.unique ? pos : positions[pos];
                                        tuples[threadTable] = nextTuple;
                                        boolean pass = true;
                                        for (JoinPartitionIndexWrapper wrapper : remainedWrappers) {
                                            if (!wrapper.evaluate(tuples)) {
                                                pass = false;
                                                break;
                                            }
                                        }
                                        if (pass) {
                                            threadResults.add(tuples.clone());
                                        }
                                    }
                                }
                            }
                            threadFinalResults = threadResults;
                            if (terminate.get()) {
                                return new ArrayList<>();
                            }
                        }
                        return threadFinalResults;
                    }));
                }

                for (int i = 0; i < ranges.size(); i++) {
                    Future<List<int[]>> futureResult = futures.get(i);
                    try {
                        List<int[]> resultList = futureResult.get();
                        resultList.forEach(result::add);
                    } catch (InterruptedException | ExecutionException ignored) {

                    }
                }
                state.lastIndex = terminate.get() ? 1 : -1;
                break;
            }
            intermediateResults = curResults;
        }
    }



    /**
     * Splits table with given cardinality into tuple batches
     * according to the configuration for joining.parallel processing.
     *
     * @return list of row ranges (batches)
     */
    public List<IntIndexRange> split(int cardinality) {
        List<IntIndexRange> batches = new ArrayList<>();
//        int batchSize = (int) Math.max(ParallelConfig.PRE_BATCH_SIZE, Math.round((cardinality + 0.0) / nrWorking));
        int batchSize = ParallelConfig.PRE_BATCH_SIZE;
        for (int batchCtr = 0; batchCtr * batchSize < cardinality;
             ++batchCtr) {
            int startIdx = batchCtr * batchSize;
            int tentativeEndIdx = startIdx + batchSize - 1;
            int endIdx = Math.min(cardinality - 1, tentativeEndIdx);
            IntIndexRange IntIndexRange = new IntIndexRange(startIdx, endIdx, batchCtr);
            batches.add(IntIndexRange);
        }
        return batches;
    }


    @Override
    public boolean isFinished() {
        return isFinished || (lastState != null && lastState.isFinished());
    }
}
