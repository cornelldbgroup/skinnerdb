package joining.parallel.join;

import config.*;
import expressions.compilation.KnaryBoolEval;
import joining.parallel.indexing.IntIndexRange;
import joining.parallel.plan.LeftDeepPartitionPlan;
import joining.parallel.progress.ParallelProgressTracker;
import joining.plan.JoinOrder;
import joining.progress.State;
import joining.result.ResultTuple;
import net.sf.jsqlparser.expression.Expression;
import org.omg.CORBA.SystemException;
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

    private List<int[]>[] resultList;
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
        System.out.println("Working Numbers: " + nrWorking);
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
        order = new int[]{8, 2, 5, 1, 7, 9, 3, 6, 4, 0};
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
        if (true) {
            Arrays.fill(state.tupleIndices , 0);
//            executeFixedJoin(plan, state, offsets);
            terminate.set(false);
            long exe1 = System.currentTimeMillis();
            executeParallelRePartition(plan, state, offsets);
            long exe2 = System.currentTimeMillis();
            System.out.println("Execution: " + (exe2 - exe1));
            lastState = state;
            return 0;
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

    // re-partitioning
    private void executeParallelRePartition(LeftDeepPartitionPlan plan, State state, int[] offsets) {
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
        int[] intermediateResults = new int[(cardinalities[firstTable] - start) * nrTables];


        for (int firstTuple = start; firstTuple < cardinalities[firstTable]; firstTuple++) {
            tupleIndices[firstTable] = firstTuple;
            int resultPos = firstTuple - start;
            System.arraycopy(tupleIndices, 0, intermediateResults, resultPos, nrTables);
        }
        //
        for (int nextIndex = 1; nextIndex < nrTables; nextIndex++) {
            long timer0 = System.currentTimeMillis();
            // the next table to join
            int nextTable = plan.joinOrder.order[nextIndex];
            // the cardinality of the next table
            int nextCardinality = cardinalities[nextTable];
            // Integrate table offset
            tupleIndices[nextTable] = Math.max(
                    offsets[nextTable], tupleIndices[nextTable]);
            // the minimal tuple for current table
            int curTuple = tupleIndices[nextTable];
            // join indexes
            List<JoinPartitionIndexWrapper> indexWrappers = joinIndices.get(nextIndex);
            final boolean isEmpty = indexWrappers.isEmpty();
            final boolean isSingle = indexWrappers.size() == 1;
            // partition the input table
            List<IntIndexRange> batches = this.split(intermediateResults.length / nrTables, nrTables);
            final int finalNextIndex = nrTables;


            // first run: counting size of output for each thread in parallel
            List<Future<Integer>> futures = new ArrayList<>();
            for (IntIndexRange batch: batches) {
                int startPos = batch.firstTuple;
                int endPos = batch.lastTuple;
                int[] finalIntermediateResults = intermediateResults;
                futures.add(executorService.submit(() -> {
                    long t1 = System.currentTimeMillis();
                    int[] points = new int[2];
                    // temporary array of tuples copying from intermediate results
                    int[] tuples = new int[nrTables];
                    List<int[]> newPointsList = new ArrayList<>();
                    for (int i = 0; i < indexWrappers.size(); i++) {
                        newPointsList.add(new int[2]);
                    }
                    // the index wrapper is empty
                    if (isEmpty) {
                        // how many intermediate results?
                        int size = (endPos - startPos) / finalNextIndex;
                        // Cartesian product
                        size = size * (nextCardinality - curTuple);
                        return size;
                    }
                    else {
                        int allResultCounts = 0;
                        // iterate all tuples in intermediate results
                        for (int pos = startPos; pos < endPos; pos += finalNextIndex) {
                            System.arraycopy(finalIntermediateResults, pos, tuples, 0, finalNextIndex);
                            tuples[nextTable] = curTuple;
                            // get the smallest index
                            // the size of index is defined by the number of matched rows
                            // that are larger than the offset
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
                            // there is only one join index
                            if (isSingle) {
                                allResultCounts += small;
                            }
                            else {
                                // the index with the least matched rows.
                                JoinPartitionIndexWrapper prob = indexWrappers.get(index);
                                // points has two elements: first is the start position in the positions array.
                                // second is the end position in the positions array.
                                int startIndexPos = points[0];
                                int endIndexPos = points[1];
                                int[] positions = prob.nextIndex.positions;
                                // the remaining indexes that are used to evaluate
                                List<JoinPartitionIndexWrapper> remainedWrappers = new ArrayList<>(indexWrappers);
                                remainedWrappers.remove(index);
                                // iterate all matched rows in the right table
                                for (int indexPos = startIndexPos; indexPos <= endIndexPos; indexPos++) {
                                    int nextTuple = prob.nextIndex.unique ? indexPos : positions[indexPos];
                                    tuples[nextTable] = nextTuple;
                                    boolean pass = true;
                                    for (JoinPartitionIndexWrapper wrapper : remainedWrappers) {
                                        if (!wrapper.evaluate(tuples)) {
                                            pass = false;
                                            break;
                                        }
                                    }
                                    // if the matched row also matched with the remaining predicates.
                                    if (pass) {
                                        allResultCounts++;
                                    }
                                }
                            }
                        }
                        long t2 = System.currentTimeMillis();
                        System.out.println(finalNextIndex + " First: " + startPos + " " + (t2 - t1));
                        return allResultCounts;
                    }
                }));
            }
            int nrBatches = batches.size();
            int[] batchesCount = new int[nrBatches];
            int prefixSum = 0;
            for (int i = 0; i < nrBatches; i++) {
                batchesCount[i] = prefixSum;
                Future<Integer> batchCount = futures.get(i);
                try {
                    prefixSum += batchCount.get();
                } catch (InterruptedException | ExecutionException ignored) {

                }
            }
            long timer1 = System.currentTimeMillis();
            System.out.println("Second Run: ");
            // second run: write data to intermediate results
            final int[] newIntermediateResults = new int[prefixSum * nrTables];
            List<Future<Integer>> secondFutures = new ArrayList<>();
            for (int batchCtr = 0; batchCtr < nrBatches; batchCtr++) {
                IntIndexRange batch = batches.get(batchCtr);
                int startPos = batch.firstTuple;
                int endPos = batch.lastTuple;
                int[] finalIntermediateResults = intermediateResults;
                int writeStartPos = batchesCount[batchCtr] * nrTables;
                secondFutures.add(executorService.submit(() -> {
                    long t1 = System.currentTimeMillis();
                    int[] points = new int[2];
                    // temporary array of tuples copying from intermediate results
                    int[] tuples = new int[nrTables];
                    List<int[]> newPointsList = new ArrayList<>();
                    for (int i = 0; i < indexWrappers.size(); i++) {
                        newPointsList.add(new int[2]);
                    }
                    int writePos = writeStartPos;
                    // the index wrapper is empty
                    if (isEmpty) {
                        for (int pos = startPos; pos < endPos; pos += finalNextIndex) {
                            System.arraycopy(finalIntermediateResults, pos, tuples, 0, finalNextIndex);
                            for (int row = curTuple; row < nextCardinality; row++) {
                                tuples[nextTable] = row;
                                System.arraycopy(tuples, 0, newIntermediateResults, writePos, finalNextIndex);
                                writePos += finalNextIndex;
                            }
                        }
                    }
                    else {
                        // iterate all intermediate results
                        for (int pos = startPos; pos < endPos; pos += finalNextIndex) {
                            System.arraycopy(finalIntermediateResults, pos, tuples, 0, finalNextIndex);
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
                            int startIndexPos = points[0];
                            int endIndexPos = points[1];
                            int[] positions = prob.nextIndex.positions;

                            List<JoinPartitionIndexWrapper> remainedWrappers = new ArrayList<>(indexWrappers);
                            remainedWrappers.remove(index);
                            for (int indexPos = startIndexPos; indexPos <= endIndexPos; indexPos++) {
                                int nextTuple = prob.nextIndex.unique ? indexPos : positions[indexPos];
                                tuples[nextTable] = nextTuple;
                                boolean pass = true;
                                for (JoinPartitionIndexWrapper wrapper : remainedWrappers) {
                                    if (!wrapper.evaluate(tuples)) {
                                        pass = false;
                                        break;
                                    }
                                }
                                if (pass) {
                                    System.arraycopy(tuples, 0, newIntermediateResults, writePos, finalNextIndex);
                                    writePos += finalNextIndex;
                                }
                            }
                        }
                        long t2 = System.currentTimeMillis();
                        System.out.println(finalNextIndex + " Second: " + startPos + " " + (t2 - t1));
                    }
                    return 0;
                }));
            }
            for (int i = 0; i < nrBatches; i++) {
                Future<Integer> batchCount = futures.get(i);
                try {
                    prefixSum += batchCount.get();
                } catch (InterruptedException | ExecutionException ignored) {

                }
            }
            long timer2 = System.currentTimeMillis();
            System.out.println("Index: " + nextIndex + "; First: " + (timer1 - timer0) + "; Second: " + (timer2 - timer1));
            intermediateResults = newIntermediateResults;
        }
        state.lastIndex = terminate.get() ? 1 : -1;
    }

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
            if (finalIntermediateResults.size() < 1000) {
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
                List<Future<List<int[]>>> futures = new ArrayList<>();
                this.resultList = new ArrayList[ranges.size()];
                for (IntIndexRange batch : ranges) {
                    int startIndex = batch.firstTuple;
                    int endIndex = batch.lastTuple;
                    final int indexStart = nextIndex;
                    futures.add(executorService.submit(() -> {
                        long timer0 = System.currentTimeMillis();
                        int[] points = new int[2];
                        int[] threadIndices = tupleIndices.clone();
                        List<int[]> threadFinalResults = new ArrayList<>();
                        // the first table. We need to get tuples from a list
                        int threadTable = plan.joinOrder.order[indexStart];
                        // Integrate table offset
                        threadIndices[threadTable] = Math.max(
                                offsets[threadTable], threadIndices[threadTable]);
                        // some join statistics for current index
                        int threadCardinality = cardinalities[threadTable];
                        int threadTuple = threadIndices[threadTable];
                        List<JoinPartitionIndexWrapper> threadIndexWrappers = joinIndices.get(indexStart);
                        final boolean threadEmpty = threadIndexWrappers.isEmpty();

                        List<int[]> newPointsList = new ArrayList<>();
                        for (int i = 0; i < threadIndexWrappers.size(); i++) {
                            newPointsList.add(new int[2]);
                        }

                        ThreadResult[] threadResults = new ThreadResult[endIndex - startIndex + 1];
                        int allResultCounts = 0;
                        // the index wrapper is empty
                        if (threadEmpty) {
                            for (int rid = startIndex; rid <= endIndex; rid++) {
                                int[] tuples = finalIntermediateResults.get(rid);
                                int[] threadResultsArray = new int[(threadCardinality - threadTuple) * nrTables + 1];
                                for (int tuple = threadTuple; tuple < threadCardinality; tuple++) {
                                    tuples[threadTable] = tuple;
                                    int resultPos = (tuple - threadTuple) * nrTables + 1;
                                    System.arraycopy(tuples, 0, threadResultsArray, resultPos, nrTables);
                                }
                                int count = threadCardinality - threadTuple;
                                threadResultsArray[0] = count;
                                allResultCounts += count;
                                threadResults[rid - startIndex] = new ThreadResult(threadResultsArray);
                            }
                        }
                        else {
                            for (int rid = startIndex; rid <= endIndex; rid++) {
                                int[] tuples = finalIntermediateResults.get(rid);
                                threadResults[rid - startIndex] = getThreadResult(tuples, threadTable,
                                        threadTuple, newPointsList, points, nrTables, threadIndexWrappers);
                                allResultCounts += threadResults[rid - startIndex].count;
                            }
                        }
                        // master thread update a new join order?
                        if (terminate.get()) {
                            return new ArrayList<>();
                        }


                        //
                        for (int threadIndex = indexStart + 1; threadIndex < nrTables; threadIndex++) {
                            ThreadResult[] curThreadResults = new ThreadResult[allResultCounts];
                            allResultCounts = 0;
                            threadTable = plan.joinOrder.order[threadIndex];
                            // Integrate table offset
                            threadIndices[threadTable] = Math.max(
                                    offsets[threadTable], threadIndices[threadTable]);
                            // some join statistics for current index
                            threadCardinality = cardinalities[threadTable];
                            threadTuple = threadIndices[threadTable];
                            threadIndexWrappers = joinIndices.get(threadIndex);
                            final boolean empty = threadIndexWrappers.isEmpty();

                            newPointsList = new ArrayList<>();
                            for (int i = 0; i < threadIndexWrappers.size(); i++) {
                                newPointsList.add(new int[2]);
                            }

                            if (empty) {
                                int nrTuples = 0;
                                for (ThreadResult threadResult: threadResults) {
                                    int count = threadResult.count;
                                    int[] threadResultsArray = new int[count * nrTables + 1];
                                    for (int tuplesCtr = 0; tuplesCtr < count; tuplesCtr++) {
                                        int tuplePos = 1 + tuplesCtr * nrTables;
                                        for (int tuple = threadTuple; tuple < threadCardinality; tuple++) {
                                            int resultPos = (tuple - threadTuple) * nrTables + 1;
                                            System.arraycopy(threadResult.result, tuplePos, threadResultsArray, resultPos, nrTables);
                                            threadResultsArray[resultPos + threadIndex] = tuple;
                                        }
                                        int newCount = threadCardinality - threadTuple;
                                        threadResultsArray[0] = newCount;
                                        allResultCounts += newCount;
                                        threadResults[nrTuples] = new ThreadResult(threadResultsArray);
                                        nrTuples++;
                                    }
                                }

                                for (ThreadResult threadResult: threadResults) {
                                    int count = threadResult.count;
                                    int[] threadResultsArray = new int[count * nrTables + 1];
                                }

//                                for (int rid = startIndex; rid <= endIndex; rid++) {
//                                    int[] tuples = source.get(rid);
//                                    for (int tuple = threadTuple; tuple < threadCardinality; tuple++) {
//                                        tuples[threadTable] = tuple;
//                                    }
//                                }
                            }
                            else {
//                                for (int rid = startIndex; rid <= endIndex; rid++) {
//                                    int[] tuples = source.get(rid);
//                                    ThreadResult threadResult = getThreadResult(tuples, threadTable,
//                                            threadTuple, newPointsList, points, nrTables, threadIndexWrappers);
//                                }
                            }
//                            threadFinalResults = threadResults;
                            // master thread update a new join order?
                            if (terminate.get()) {
                                return new ArrayList<>();
                            }
                        }
                        long timer1 = System.currentTimeMillis();
                        System.out.println("Start: " + startIndex + " " + (timer1 - timer0));
                        return threadFinalResults;
                    }));
                }

                for (int i = 0; i < ranges.size(); i++) {
                    Future<List<int[]>> futureResult = futures.get(i);
                    try {
                        this.resultList[i] = futureResult.get();
                    } catch (InterruptedException | ExecutionException ignored) {

                    }
                }
                state.lastIndex = terminate.get() ? 1 : -1;
                break;
            }
            intermediateResults = curResults;
        }
    }

    private ThreadResult getThreadResult(int[] tuples,
                                  int threadTable,
                                  int threadTuple,
                                  List<int[]> newPointsList,
                                  int[] points,
                                  int nrTables,
                                  List<JoinPartitionIndexWrapper> threadIndexWrappers) {
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
        int[] threadResultsArray = new int[(endPos - startPos + 1) * nrTables + 1];
        int count = 0;
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
                int resultPos = count * nrTables + 1;
                System.arraycopy(tuples, 0, threadResultsArray, resultPos, nrTables);
                count++;
            }
        }
        threadResultsArray[0] = count;
        return new ThreadResult(threadResultsArray);
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
        int batchSize;
        if (cardinality >= ParallelConfig.PRE_BATCH_SIZE) {
            batchSize = (int) Math.floor((cardinality + 0.0) / nrWorking);

        }
        else {
            batchSize = ParallelConfig.PRE_BATCH_SIZE;
        }
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

    /**
     * Splits table with given cardinality into tuple batches
     * according to the configuration for joining.parallel processing.
     *
     * @return list of row ranges (batches)
     */
    public List<IntIndexRange> split(int cardinality, int nrTables) {
        List<IntIndexRange> batches = new ArrayList<>();
        int batchSize = (int) Math.ceil((cardinality + 0.0) / nrWorking);

        for (int batchCtr = 0; batchCtr * batchSize < cardinality;
             ++batchCtr) {
            int startIdx = batchCtr * batchSize;
            int tentativeEndIdx = startIdx + batchSize;
            int endIdx = Math.min(cardinality, tentativeEndIdx);
            IntIndexRange IntIndexRange = new IntIndexRange(startIdx * nrTables,
                    endIdx * nrTables, batchCtr);
            batches.add(IntIndexRange);
        }
        return batches;
    }


    @Override
    public boolean isFinished() {
        return isFinished || (lastState != null && lastState.isFinished());
    }
}

class ThreadResult {
    final int[] result;
    final int count;
    public ThreadResult(int[] result) {
        this.result = result;
        count = result[0];
    }
}