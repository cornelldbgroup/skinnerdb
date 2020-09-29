package joining.parallel.join;

import config.*;
import expressions.compilation.KnaryBoolEval;
import joining.parallel.indexing.IntIndexRange;
import joining.parallel.plan.LeftDeepPartitionPlan;
import joining.parallel.progress.ParallelProgressTracker;
import joining.plan.JoinOrder;
import joining.progress.State;
import net.sf.jsqlparser.expression.Expression;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import predicate.NonEquiNode;
import preprocessing.Context;
import query.QueryInfo;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

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
     * Avoids redundant joining work by storing progress.
     */
    final Map<Integer, ThreadResult[]>[] progressCache;
    /**
     * Avoids redundant joining work by storing progress.
     */
    final Map<Integer, Pair<Integer, Double>>[] statsCache;
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
    public final int nrWorking;
    /**
     * Thread pool for each executor
     */
    public final ExecutorService executorService;
    /**
     * Result list
     */
    public List<int[]>[] resultList;
    /**
     * Result list for parallelization
     */
    public ThreadResult[][] threadResultsList;

    public final double[] joinProbs;


    public final long[] maxSizes;

    public int nrTuples = 0;

    /**
     * Initializes join algorithm for given input query.
     *
     * @param query			query to process
     * @param preSummary	summary of pre-processing
     * @param budget		budget per episode
     */
    public FixJoin(QueryInfo query, Context preSummary,
                   int budget, int nrThreads, int tid, Map<Expression, NonEquiNode> predToEval, int nrWorks) throws Exception {
        super(query, preSummary, budget, nrThreads, tid, predToEval);
        this.nrWorking = nrWorks;
        this.planCache = new HashMap<>();
        this.progressCache = new HashMap[ParallelConfig.NR_BATCHES];
        this.statsCache = new HashMap[ParallelConfig.NR_BATCHES];
        this.maxSizes = new long[ParallelConfig.NR_BATCHES];
        for (int i = 0; i < ParallelConfig.NR_BATCHES; i++) {
            this.progressCache[i] = new HashMap<>();
            this.statsCache[i] = new HashMap<>();
        }
        // Collect unary predicates
        this.unaryPreds = new KnaryBoolEval[nrJoined];
        this.tupleIndexDelta = new int[nrJoined];
        this.joinProbs = new double[nrJoined];
        this.terminate = new AtomicBoolean(false);
        if (nrWorks > 0) {
            System.out.println("Working Numbers: " + nrWorks);
            this.executorService = Executors.newFixedThreadPool(nrWorks);
        }
        else {
            this.executorService = null;
        }
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

//        order = QueryStats.optimal;
//        System.out.println(Arrays.toString(order));
//        order = new int[]{8, 2, 5, 1, 7, 9, 3, 6, 4, 0};
//        order = new int[]{3, 10, 11, 7, 0, 8, 9, 2, 1, 6, 13, 5, 12, 4};
//        order = new int[]{1, 3, 0, 2};
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
        State state = new State(nrJoined);
        int[] offsets = Arrays.copyOf(tracker.tableOffsetMaps[0][0], nrJoined);
//        Arrays.fill(offsets, 0);

//            executeFixedJoin(plan, state, offsets);
        terminate.set(false);
//            long exe1 = System.currentTimeMillis();
//            executeParallelRePartition(plan, state, offsets);
        executeParallelFixedJoin(plan, state, offsets);
//            long exe2 = System.currentTimeMillis();
//            System.out.println("Execution: " + (exe2 - exe1));
        lastState = state;
        if (!terminate.get()) {
            state.lastIndex = -1;
            isFinished = true;
        }
        int constant = 4 * nrJoined;
        List<Pair<Pair<Integer, Integer>, Pair<Integer, Double>>> pairList = new LinkedList<>();
        for (int i = 0; i < progressCache.length; i++) {
            for (Map.Entry<Integer, Pair<Integer, Double>> entry: statsCache[i].entrySet()) {
                Pair<Integer, Double> resultPair = entry.getValue();
                Pair<Integer, Integer> keyPair = new ImmutablePair<>(i, entry.getKey());
                pairList.add(new ImmutablePair<>(keyPair, resultPair));
            }
        }
        int size = pairList.stream().mapToInt(pair -> pair.getRight().getLeft()).sum() * constant;


        if (size > ParallelConfig.MAX_CACHE_SIZE) {
            // utility based
            pairList.sort(Comparator.comparing(o -> o.getRight().getRight()));
            Pair<Pair<Integer, Integer>, Pair<Integer, Double>> entry = pairList.get(0);
            int bid = entry.getKey().getLeft();
            int key = entry.getKey().getRight();
            int itemSize = entry.getRight().getLeft() * constant;
            while (size > ParallelConfig.MAX_CACHE_SIZE) {
                statsCache[bid].remove(key);
                progressCache[bid].remove(key);
                pairList.remove(0);
                size -= itemSize;
                if (size > 0) {
                    entry = pairList.get(0);
                    bid = entry.getKey().getLeft();
                    key = entry.getKey().getRight();
                    itemSize = entry.getRight().getLeft() * constant;
                }
            }
            // FIFO
//            for (int i = 0; i < progressCache.length; i++) {
//                List<Pair<Pair<Integer, Integer>, Pair<Integer, Double>>> batchList = new LinkedList<>();
//                for (Map.Entry<Integer, Pair<Integer, Double>> entry: statsCache[i].entrySet()) {
//                    Pair<Integer, Double> resultPair = entry.getValue();
//                    Pair<Integer, Integer> keyPair = new ImmutablePair<>(i, entry.getKey());
//                    batchList.add(new ImmutablePair<>(keyPair, resultPair));
//                }
//                batchList.sort(Comparator.comparing(o -> o.getLeft().getRight()));
//                Iterator<Pair<Pair<Integer, Integer>, Pair<Integer, Double>>> batchIter = batchList.iterator();
//                while (size > ParallelConfig.MAX_CACHE_SIZE && batchIter.hasNext()) {
//                    Pair<Pair<Integer, Integer>, Pair<Integer, Double>> entry = batchIter.next();
//                    int key = entry.getLeft().getRight();
//                    int itemSize = entry.getRight().getLeft() * constant;
//                    statsCache[i].remove(key);
//                    progressCache[i].remove(key);
//                    batchIter.remove();
//                    size -= itemSize;
//                }
//            }
        }


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

    // parallel breadth-first join algorithm without re-paritioning
    private void executeParallelFixedJoin(LeftDeepPartitionPlan plan, State state, int[] offsets) {
        // Extract variables for convenient access
        int nrTables = query.nrJoined;
        int[] tupleIndices = new int[nrTables];
        List<List<JoinPartitionIndexWrapper>> joinIndices = plan.joinIndices;
        List<List<NonEquiNode>> applicablePreds = plan.nonEquiNodes;
        // Initialize state and flags to prepare budgeted execution
        System.arraycopy(state.tupleIndices, 0, tupleIndices, 0, nrTables);
        // Number of completed tuples added
        nrResultTuples = 0;
        int nrIterations = 0;

        // Execute join order using standard join algorithm.
        int firstTable = plan.joinOrder.order[0];
        int start = Math.max(offsets[firstTable], tupleIndices[firstTable]);
        List<int[]> intermediateResults = new ArrayList<>();

        // Iterate all tables in the join order
        for (int nextIndex = 0; nextIndex < nrTables; nextIndex++) {
            int nextTable = plan.joinOrder.order[nextIndex];
            // Integrate table offset
            tupleIndices[nextTable] = Math.max(
                    offsets[nextTable], tupleIndices[nextTable]);
            // Some join statistics for current index
            int nextCardinality = cardinalities[nextTable];
            int curTuple = tupleIndices[nextTable];
            List<JoinPartitionIndexWrapper> indexWrappers = joinIndices.get(nextIndex);
            final boolean isEmpty = indexWrappers.isEmpty();
            final boolean isFirst = nextIndex == 0;
            List<int[]> finalIntermediateResults = intermediateResults;
            // Partition the input intermediate results
            int splitSize = isFirst ? cardinalities[firstTable] - start : finalIntermediateResults.size();
            List<IntIndexRange> ranges = this.split(splitSize);
            List<int[]> curResults = isFirst && splitSize < ParallelConfig.PRE_BATCH_SIZE ?
                    new ArrayList<>(cardinalities[firstTable] - start) :
                    // TODO: specify the size of the list
                    new ArrayList<>();

            // if the intermediate result size is smaller than the minimal batch size.
            // use breadth-first join sequentially
            // Please just skip this branch because the else branch uses the same join algorithm but in parallel
            if (splitSize < ParallelConfig.PRE_BATCH_SIZE) {
                int[] points = new int[2];
                List<int[]> newPointsList = new ArrayList<>();
                for (int i = 0; i < indexWrappers.size(); i++) {
                    newPointsList.add(new int[2]);
                }
                if (isFirst) {
                    for (int firstTuple = start; firstTuple < cardinalities[firstTable]; firstTuple++) {
                        tupleIndices[firstTable] = firstTuple;
                        curResults.add(tupleIndices.clone());
                        nrIterations++;
                        if (terminate.get()) {
                            state.lastIndex = 1;
                            return;
                        }
                    }
                }
                else if (isEmpty) {
                    for (int[] tuples: finalIntermediateResults) {
                        int tuple = curTuple;
                        while (tuple < nextCardinality) {
                            tuples[nextTable] = tuple;
                            // whether pass non-equi join predicate
                            boolean pass = true;
                            for (NonEquiNode pred : applicablePreds.get(nextIndex)) {
                                if (!pred.evaluate(tuples, nextTable, cardinalities[nextTable])) {
                                    pass = false;
                                    break;
                                }
                            }
                            if (pass) {
                                curResults.add(tuples.clone());
                                nrIterations++;
                            }
                            tuple = tuples[nextTable] + 1;
                            if (terminate.get()) {
                                state.lastIndex = nextIndex;
                                return;
                            }
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
                            // evaluate non-equi join predicates
                            for (NonEquiNode pred : applicablePreds.get(index)) {
                                if (!pred.evaluate(tuples, nextTable, cardinalities[nextTable]) || !pass) {
                                    pass = false;
                                    break;
                                }
                            }
                            if (pass) {
                                curResults.add(tuples.clone());
                                nrIterations++;
                            }
                            if (terminate.get()) {
                                state.lastIndex = nextIndex;
                                return;
                            }
                        }
                    }
                }
            }
            // Parallel breadth-first join algorithm
            else {
                List<Future<List<int[]>>> futures = new ArrayList<>();
                this.resultList = new ArrayList[ranges.size()];
                this.threadResultsList = new ThreadResult[ranges.size()][];
                for (IntIndexRange batch : ranges) {
                    // Start and end position of the partition.
                    int startIndex = batch.firstTuple;
                    int endIndex = batch.lastTuple;
                    final int indexStart = nextIndex;
                    final int bid = batch.bid;
                    final int finalIterations = nrIterations;
                    futures.add(executorService.submit(() -> {
                        int[] points = new int[2];
                        int[] threadIndices = tupleIndices.clone();
                        List<int[]> threadFinalResults = new ArrayList<>();
                        // For the first table that is larger than batch size, we need to get tuples from a list
                        int threadTable = plan.joinOrder.order[indexStart];
                        int threadIterations = finalIterations;
                        // Integrate table offset
                        threadIndices[threadTable] = Math.max(
                                offsets[threadTable], threadIndices[threadTable]);
                        // Some join statistics for current index
                        int threadCardinality = cardinalities[threadTable];
                        int threadTuple = threadIndices[threadTable];
                        List<JoinPartitionIndexWrapper> threadIndexWrappers = joinIndices.get(indexStart);
                        List<NonEquiNode> threadNonEquiPreds = applicablePreds.get(indexStart);
                        final boolean threadEmpty = threadIndexWrappers.isEmpty();
                        final boolean threadFirst = indexStart == 0;
                        // Create a list of int array. Each array is according to a join index.
                        // The array has two elements. After running indexSize function,
                        // the first element will be used to save the start matched row and
                        // the second element will be used to save the end matched row.
                        List<int[]> newPointsList = new ArrayList<>();
                        for (int i = 0; i < threadIndexWrappers.size(); i++) {
                            newPointsList.add(new int[2]);
                        }

                        // the prefix key of joined tables
                        int prefixKey = plan.joinOrder.getPrefixKey(indexStart + 1);
                        int finalIndexStart = indexStart;
                        ThreadResult[] threadResults = null;
                        // find the longest prefix key
                        for (int prefixLen = nrTables; prefixLen > 0; prefixLen--) {
                            int key = plan.joinOrder.getPrefixKey(prefixLen);
                            if (progressCache[bid].containsKey(key)) {
                                finalIndexStart = prefixLen - 1;
                                threadResults = progressCache[bid].get(key);
                                break;
                            }
                        }
                        int allResultCounts = 0;
                        if (threadResults != null) {
                            System.out.println("Cache hit: " + finalIndexStart);
                            for (ThreadResult threadResult: threadResults) {
                                if (threadResult != null) {
                                    allResultCounts += threadResult.count;
                                }
                            }
                        }
                        else {
                            // Index wrapper is empty
                            if (threadFirst) {
                                // Create an object array that has the same size of intermediate result partition
                                threadResults = new ThreadResult[1];
                                int[] threadResultsArray = new int[nrTables * (endIndex - startIndex + 1) + 1];
                                for (int firstTuple = start + startIndex; firstTuple <= start + endIndex; firstTuple++) {
                                    int tablePos = 1 + (firstTuple - startIndex - start) * nrTables + firstTable;
                                    threadResultsArray[tablePos] = firstTuple;
                                    if (terminate.get()) {
                                        return new ArrayList<>();
                                    }
                                    // Accumulate how many new tuples we have found.
                                    allResultCounts++;
                                    threadIterations++;
                                }
                                threadResultsArray[0] = allResultCounts;
                                // save the encapsulation into the object array.
                                threadResults[0] = new ThreadResult(threadResultsArray);
                            }
                            else if (threadEmpty) {
                                // Create an object array that has the same size of intermediate result partition
                                threadResults = new ThreadResult[endIndex - startIndex + 1];
                                // Iterate all intermediate tuples
                                int[] tuples = new int[nrTables];
                                for (int rid = startIndex; rid <= endIndex; rid++) {
                                    System.arraycopy(finalIntermediateResults.get(rid), 0, tuples, 0, nrTables);
                                    // For this tuple, we know there are (threadCardinality - threadTuple) numbers of
                                    // tuples that will be joined to the given tuple.
                                    // And we save all tuples in a long array.
                                    int count = 0;
                                    int newCount = threadNonEquiPreds.size() == 0 ? threadCardinality - threadTuple : 1;
                                    int[] threadResultsArray = new int[newCount * nrTables + 1];
                                    int tuple = threadTuple;
                                    while (tuple < threadCardinality) {
                                        tuples[threadTable] = tuple;
                                        // Whether pass non-equi join predicate
                                        boolean pass = true;
                                        for (NonEquiNode pred : applicablePreds.get(indexStart)) {
                                            if (!pred.evaluate(tuples, threadTable, cardinalities[threadTable])) {
                                                pass = false;
                                                break;
                                            }
                                        }
                                        if (pass) {
                                            int resultPos = count * nrTables + 1;
//                                            if (resultPos > threadResultsArray.length - 1) {
//                                                System.out.println("Large: " + resultPos + " " + threadResultsArray.length +
//                                                        "\n" + Arrays.toString(tuples) + "\n" +
//                                                        Arrays.toString(threadResultsArray));
//                                                System.exit(0);
//                                            }
                                            System.arraycopy(tuples, 0, threadResultsArray, resultPos, nrTables);
                                            count++;
                                        }
                                        tuple = tuples[threadTable] + 1;
                                        // master thread update a new join order?
                                        if (terminate.get()) {
                                            return new ArrayList<>();
                                        }
                                    }
                                    threadResultsArray[0] = count;
                                    // accumulate how many new tuples we have found.
                                    allResultCounts += count;
                                    threadIterations += count;
                                    // save the encapsulation into the object array.
                                    threadResults[rid - startIndex] = new ThreadResult(threadResultsArray);
                                }
                            }
                            else {
                                // Create an object array that has the same size of intermediate result partition
                                threadResults = new ThreadResult[endIndex - startIndex + 1];
                                // Iterate all intermediate tuples
                                int[] tuples = new int[nrTables];
                                for (int rid = startIndex; rid <= endIndex; rid++) {
                                    System.arraycopy(finalIntermediateResults.get(rid), 0, tuples, 0, nrTables);
                                    threadResults[rid - startIndex] = getThreadResult(tuples, threadTable,
                                            threadTuple, newPointsList, points, nrTables,
                                            threadIndexWrappers, threadNonEquiPreds);
                                    if (threadResults[rid - startIndex] == null) {
                                        return new ArrayList<>();
                                    }
                                    int newCount = threadResults[rid - startIndex].count;
                                    allResultCounts += newCount;
                                    threadIterations += newCount;
                                }
                            }

                            // Write the results to the cache
                            this.progressCache[bid].put(prefixKey, threadResults);
                            double score = joinProbs[indexStart] * threadIterations;
                            this.statsCache[bid].put(prefixKey, new ImmutablePair<>(allResultCounts, score));
                            if (finalIndexStart == nrTables - 1) {
                                this.threadResultsList[bid] = threadResults;
                            }
                            long max = this.maxSizes[bid];
                            this.maxSizes[bid] = Math.max(max, allResultCounts);
                        }

                        // For the remaining tables, we cannot get tuples from a list.
                        // Instead, we need to get tuples from the object array.
                        for (int threadIndex = finalIndexStart + 1; threadIndex < nrTables; threadIndex++) {
//                            long timer0 = System.currentTimeMillis();
                            prefixKey = plan.joinOrder.getPrefixKey(threadIndex + 1);
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
                            threadNonEquiPreds = applicablePreds.get(threadIndex);
                            final boolean empty = threadIndexWrappers.isEmpty();

                            newPointsList = new ArrayList<>();
                            for (int i = 0; i < threadIndexWrappers.size(); i++) {
                                newPointsList.add(new int[2]);
                            }
                            // the number of tuples from the intermediate results that have been joined
                            int nrTuples = 0;
                            if (empty) {
                                int newCount = threadNonEquiPreds.size() == 0 ?
                                        threadCardinality - threadTuple : 1;
                                // iterate each encapsulation in the object array
                                for (ThreadResult threadResult: threadResults) {
                                    // the number of saved tuples in each object.
                                    int count = threadResult.count;
                                    for (int tuplesCtr = 0; tuplesCtr < count; tuplesCtr++) {
                                        int[] threadResultsArray = new int[newCount * nrTables + 1];
                                        int actualCount = 0;
                                        int startPos = tuplesCtr * nrTables + 1;
                                        System.arraycopy(threadResult.result, startPos, threadIndices, 0, nrTables);
                                        // iterate all rows in the right table
                                        int tuple = threadTuple;
                                        while (tuple < threadCardinality) {
                                            threadIndices[threadTable] = tuple;
                                            // whether pass non-equi join predicate
                                            boolean pass = true;
                                            for (NonEquiNode pred : applicablePreds.get(threadIndex)) {
                                                if (!pred.evaluate(threadIndices, threadTable, cardinalities[threadTable])) {
                                                    pass = false;
                                                    break;
                                                }
                                            }
                                            if (pass) {
                                                int resultPos = actualCount * nrTables + 1;
                                                System.arraycopy(threadIndices, 0,
                                                        threadResultsArray, resultPos, nrTables);
                                                actualCount++;
                                            }
                                            tuple = threadIndices[threadTable] + 1;
                                            // master thread update a new join order?
                                            if (terminate.get()) {
                                                return new ArrayList<>();
                                            }
                                        }
                                        threadResultsArray[0] = actualCount;
                                        allResultCounts += actualCount;
                                        threadIterations += actualCount;
                                        curThreadResults[nrTuples] = new ThreadResult(threadResultsArray);
                                        nrTuples++;
                                    }
                                }
                            }
                            else {
                                // iterate each encapsulation in the object array
                                for (ThreadResult threadResult: threadResults) {
                                    // the number of saved tuples in each object.
                                    int count = threadResult.count;
                                    int[] resultArray = threadResult.result;
                                    // iterate each tuple in the object.
                                    for (int resultCtr = 0; resultCtr < count; resultCtr++) {
                                        int startPos = resultCtr * nrTables + 1;
                                        System.arraycopy(resultArray, startPos, threadIndices, 0, nrTables);
                                        // create a new object encapsulation and save it to according position.
                                        curThreadResults[nrTuples] = getThreadResult(threadIndices, threadTable,
                                                threadTuple, newPointsList, points, nrTables,
                                                threadIndexWrappers, threadNonEquiPreds);
                                        if (curThreadResults[nrTuples] == null) {
                                            return new ArrayList<>();
                                        }
                                        int newCount = curThreadResults[nrTuples].count;
                                        allResultCounts += newCount;
                                        threadIterations += newCount;
                                        nrTuples++;
                                    }
                                }
                            }
                            threadResults = curThreadResults;
                            // write the results to the cache
                            this.progressCache[bid].put(prefixKey, threadResults);
                            double score = joinProbs[threadIndex] * threadIterations;
                            this.statsCache[bid].put(prefixKey, new ImmutablePair<>(allResultCounts, score));
//                            long timer1 = System.currentTimeMillis();
//                            System.out.println("Start: " + threadIndex + ": " +
//                                    allResultCounts + " " + timer0 + " " + timer1 + " " + (timer1 - timer0) + " " + this.nrTuples);
                            long max = this.maxSizes[bid];
                            this.maxSizes[bid] = Math.max(max, allResultCounts);
                        }
                        this.threadResultsList[bid] = threadResults;
//                        long startY = System.currentTimeMillis();
//                        System.out.println("Batch " + bid + ": [" + startX + "," + startY + "] " + (startY - startX));
                        return threadFinalResults;
                    }));
                }

                for (int i = 0; i < ranges.size(); i++) {
                    Future<List<int[]>> futureResult = futures.get(i);
                    try {
                        this.resultList[i] = futureResult.get();
                    } catch (Exception ignored) {
                        ignored.printStackTrace();
                    }
                }
                break;
            }
            intermediateResults = curResults;

            if (nextIndex == nrTables - 1) {
                resultList = new ArrayList[1];
                resultList[0] = curResults;
            }
        }

    }

    /**
     * Get ThreadResult for a given tuple
     * @param tuples                a tuple from the previous intermediate results
     * @param threadTable           the right table to join
     * @param threadTuple           the offset of the right table
     * @param newPointsList         data structure to save start and end matched rows for each join index
     * @param points                data structure to save start and end matched rows for the smallest join index
     * @param nrTables              the numbers of tables
     * @param threadIndexWrappers   all join indexes
     * @return
     */
    private ThreadResult getThreadResult(int[] tuples,
                                         int threadTable,
                                         int threadTuple,
                                         List<int[]> newPointsList,
                                         int[] points,
                                         int nrTables,
                                         List<JoinPartitionIndexWrapper> threadIndexWrappers,
                                         List<NonEquiNode> threadNonEquiPreds) {
        int firstOne = tuples[threadTable];

        int count = 0;
        int nextCardinality = cardinalities[threadTable];
        boolean finish = true;
        while (finish) {
            if (evaluateInScope(threadIndexWrappers, threadNonEquiPreds, tuples, threadTable)) {
                count++;
                tuples[threadTable]++;
                if (tuples[threadTable] == nextCardinality) {
                    break;
                }
            }
            else {
                // If there is no equi-predicates.
                if (threadIndexWrappers.isEmpty()) {
                    tuples[threadTable]++;
                    if (tuples[threadTable] == nextCardinality) {
                        break;
                    }
                }
                else {
                    boolean first = true;
                    for (JoinPartitionIndexWrapper wrapper : threadIndexWrappers) {
                        if (!first) {
                            if (wrapper.evaluate(tuples)) {
                                continue;
                            }
                        }
                        int nextRaw = wrapper.nextIndex(tuples, null);
                        if (nextRaw < 0 || nextRaw == nextCardinality) {
                            tuples[threadTable] = nextCardinality;
                            finish = false;
                            break;
                        }
                        else {
                            tuples[threadTable] = nextRaw;
                        }
                        first = false;
                    }
                }
            }

            // master thread update a new join order?
            if (terminate.get()) {
                return null;
            }
        }

        tuples[threadTable] = firstOne;
        int[] threadResultsArray = new int[count * nrTables + 1];
        count = 0;
        finish = true;
        while (finish) {
            if (evaluateInScope(threadIndexWrappers, threadNonEquiPreds, tuples, threadTable)) {
                int resultPos = count * nrTables + 1;
                System.arraycopy(tuples, 0, threadResultsArray, resultPos, nrTables);
                count++;
                tuples[threadTable]++;
                if (tuples[threadTable] == nextCardinality) {
                    break;
                }
            }
            else {
                // If there is no equi-predicates.
                if (threadIndexWrappers.isEmpty()) {
                    tuples[threadTable]++;
                    if (tuples[threadTable] == nextCardinality) {
                        break;
                    }
                }
                else {
                    boolean first = true;
                    for (JoinPartitionIndexWrapper wrapper : threadIndexWrappers) {
                        if (!first) {
                            if (wrapper.evaluate(tuples)) {
                                continue;
                            }
                        }
                        int nextRaw = wrapper.nextIndex(tuples, null);
                        if (nextRaw < 0 || nextRaw == nextCardinality) {
                            tuples[threadTable] = nextCardinality;
                            finish = false;
                            break;
                        }
                        else {
                            tuples[threadTable] = nextRaw;
                        }
                        first = false;
                    }
                }
            }
            // master thread update a new join order?
            if (terminate.get()) {
                return null;
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
            batchSize = (int) Math.ceil((cardinality + 0.0) / ParallelConfig.NR_BATCHES);
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
        int batchSize = (int) Math.ceil((cardinality + 0.0) / 20);

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