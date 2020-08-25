package joining.parallel.join;

import catalog.CatalogManager;
import config.LoggingConfig;
import expressions.compilation.KnaryBoolEval;
import joining.parallel.parallelization.join.*;
import joining.parallel.plan.LeftDeepPartitionPlan;
import joining.parallel.progress.ParallelProgressTracker;
import joining.parallel.progress.TaskState;
import joining.parallel.progress.TaskTracker;
import joining.parallel.statistics.StatsInstance;
import joining.plan.JoinOrder;
import joining.progress.State;
import joining.result.ResultTuple;
import net.sf.jsqlparser.expression.Expression;
import predicate.NonEquiNode;
import preprocessing.Context;
import query.QueryInfo;
import statistics.QueryStats;


import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ParaJoin {
    /**
     * Number of steps per episode.
     */
    public int budget;
    /**
     * The query for which join orders are evaluated.
     */
    public final QueryInfo query;
    /**
     * Number of tables joined by query.
     */
    public final int nrJoined;
    /**
     * At i-th position: cardinality of i-th joined table
     * (after pre-processing).
     */
    public final int[] cardinalities;
    /**
     * Summarizes pre-processing steps.
     */
    public final Context preSummary;
    /**
     * Maps non-equi join predicates to compiled evaluators.
     */
    public final Map<Expression, NonEquiNode> predToEval;
    /**
     * Collects result tuples and contains
     * finally a complete result.
     */
    public final Queue<ResultTuple> tuples = new ConcurrentLinkedQueue<>();
    /**
     * Number of working threads.
     */
    public final int nrThreads;
    /**
     * Instance of statistics records.
     */
    public StatsInstance statsInstance;
    /**
     * A list of logs.
     */
    public List<String> logs;
    /**
     * Number of episode.
     */
    public long roundCtr;
    /**
     * Number of completed tuples produced
     * during last invocation.
     */
    public int nrResultTuples;
    /**
     * Offset tuples for each table.
     */
    public final int[] offsets;
    /**
     * Avoids redundant planning work by storing left deep plans.
     */
    final Map<Integer, LeftDeepPartitionPlan> planCache;
    /**
     * Cache unfinished join tasks information for each join order.
     */
    final Map<Integer, JoinInfo> taskCache;
    /**
     * Current planning work.
     */
    public LeftDeepPartitionPlan plan;
    /**
     * Current join task queue.
     */
    public ConcurrentLinkedQueue<int[]>[] curTasks;
    /**
     * Current join task queue.
     */
    public ConcurrentLinkedQueue<List<int[]>> simTasks;
    /**
     * Current join task queue.
     */
    public ConcurrentLinkedQueue<int[]>[] createdTasks;
    /**
     * Avoids redundant evaluation work by tracking tasks.
     */
    public TaskTracker tracker;
    /**
     * Associates each table index with unary predicates.
     */
    final KnaryBoolEval[] unaryPreds;
    /**
     * Given the join order, calculate unit reward for each table.
     */
    public final double[] rewardPerTable;
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
     * Fork join thread pool.
     */
    public final ExecutorService threadPool;
    /**
     * List of working threads.
     */
    public final List<JoinMicroThread> threads;
    /**
     * List of working threads.
     */
    public final List<JoinTaskThread> taskThreads;
    /**
     * List of working threads.
     */
    public final List<JoinSimThread> simThreads;

    /**
     * Initializes join operator for given query
     * and initialize new join result.
     *
     * @param query			query to process
     * @param preSummary	summarizes pre-processing steps
     */
    public ParaJoin(QueryInfo query, Context preSummary, int budget, int nrThreads,
                  Map<Expression, NonEquiNode> predToEval) throws Exception {
        this.query = query;
        this.nrJoined = query.nrJoined;
        this.preSummary = preSummary;
        this.budget = budget;
        // Retrieve table cardinalities
        this.cardinalities = new int[nrJoined];
        this.nrThreads = nrThreads;
        this.statsInstance = new StatsInstance();
        this.logs = new ArrayList<>();
        for (Map.Entry<String,Integer> entry :
                query.aliasToIndex.entrySet()) {
            String alias = entry.getKey();
            String table = preSummary.aliasToFiltered.get(alias);
            int index = entry.getValue();
            int cardinality = CatalogManager.getCardinality(table);
            cardinalities[index] = cardinality;
        }
        this.predToEval = predToEval;
        this.planCache = new HashMap<>();
        this.taskCache = new HashMap<>();
        // Collect unary predicates
        this.unaryPreds = new KnaryBoolEval[nrJoined];
        this.offsets = new int[nrJoined];
        this.rewardPerTable = new double[nrJoined];
        this.tupleIndexDelta = new int[nrJoined];
        this.threadPool = Executors.newFixedThreadPool(nrThreads);
        this.threads = new ArrayList<>(nrThreads);
        this.taskThreads = new ArrayList<>(nrThreads);
        this.simThreads = new ArrayList<>(nrThreads);
    }
    /**
     * Executes a given join order for a given budget of steps
     * (i.e., predicate evaluations). Result tuples are added
     * to result set. Budget and result set are created during
     * the class initialization.
     *
     * @param order   table join order
     */
    public double execute(int[] order, int roundCtr) throws Exception {
        // Treat special case: at least one input relation is empty
        for (int tableCtr=0; tableCtr<nrJoined; ++tableCtr) {
            if (cardinalities[tableCtr]==0) {
                isFinished = true;
                return 1;
            }
        }
        order = new int[]{0, 1, 2};
//        System.arraycopy(QueryStats.optimal, 0, order, 0, nrJoined);
        // Lookup or generate left-deep query plan
        JoinOrder joinOrder = new JoinOrder(order);
        int joinHash = joinOrder.splitHashCode(-1);
        JoinInfo info = taskCache.get(joinHash);
        if (info == null) {
            LeftDeepPartitionPlan plan = new LeftDeepPartitionPlan(query, predToEval, joinOrder);
            JoinInfo joinInfo = new JoinInfo(plan, null, new int[nrJoined]);
            taskCache.putIfAbsent(joinHash, joinInfo);
            info = joinInfo;
        }
        this.plan = info.plan;
        // Read state from the tracker
        TaskState state = tracker.continueFrom(joinOrder, this);
        // Initialize the set of tasks
        if (this.curTasks == null) {
            this.curTasks = new ConcurrentLinkedQueue[nrJoined];
            this.createdTasks = new ConcurrentLinkedQueue[nrJoined];
            for (int joinCtr = 0; joinCtr < nrJoined; joinCtr++) {
                this.curTasks[joinCtr] = new ConcurrentLinkedQueue<>();
                this.createdTasks[joinCtr] = new ConcurrentLinkedQueue<>();
            }
            // Add first task
            JoinMicroTask initTask = new JoinMicroTask(new int[nrJoined], 0, this, -1);
            initTask.compute();
        }

        int[] startIndices = state.tupleIndices;
        // Initialize offsets
        System.arraycopy(tracker.tableOffset, 0, this.offsets, 0, nrJoined);

//        writeLog("Round: " + roundCtr + "\tJoin Order: " + Arrays.toString(order));
//        writeLog("Start: " + Arrays.toString(startIndices));


        if (threads.size() == 0) {
            // Atomic flags
            AtomicInteger budget = new AtomicInteger(this.budget * nrThreads);
            AtomicBoolean isFinished = new AtomicBoolean(false);
            for (int tid = 0; tid < nrThreads; tid++) {
                JoinMicroThread thread = new JoinMicroThread(isFinished, budget, this.curTasks, threads, this);
                threads.add(thread);
            }
        }
        else {
            threads.get(0).reset(this.budget * nrThreads);
        }
        List<Future<Double>> rewardFutures = threadPool.invokeAll(threads);
        double reward = 0;
        for (Future<Double> result: rewardFutures) {
            reward += result.get();
        }
        // Size of task
        int[] headTask = null;
        for (ConcurrentLinkedQueue<int[]> taskSet: curTasks) {
            int[] firstTask = taskSet.size() > 0 ? taskSet.peek() : null;
            if (headTask == null) {
                headTask = firstTask;
            }
        }
        this.isFinished = headTask == null;
        if (!this.isFinished) {
            int[] headIndices = headTask;
            // Store tuple index deltas used to calculate reward
            for (int tableCtr = 0; tableCtr < nrJoined; ++tableCtr) {
                int start = Math.max(offsets[tableCtr], startIndices[tableCtr]);
                int end = Math.max(offsets[tableCtr], headIndices[tableCtr]);
                tupleIndexDelta[tableCtr] = end - start;
            }

            reward = reward(joinOrder.order,
                    tupleIndexDelta, offsets);

            System.arraycopy(headIndices, 0, state.tupleIndices, 0, nrJoined);
            tracker.updateProgress(joinOrder, state);
            int nrTask = 0;
            for (ConcurrentLinkedQueue<int[]> tasks: curTasks) {
                nrTask += tasks.size();
            }
//            writeLog("End: " + Arrays.toString(headIndices) + "\tReward: " + reward + "\tSize: " + nrTask);
        }
        else {
//            writeLog("End: " + "\tReward: " + reward);
            for (int tableCtr = 0; tableCtr < nrJoined; ++tableCtr) {
                int start = Math.max(offsets[tableCtr], startIndices[tableCtr]);
                tupleIndexDelta[tableCtr] = cardinalities[tableCtr] - start;
            }
        }
        return reward;
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
    }

    public boolean isFinished() {
        return isFinished;
    }

    /**
     * Put a log sentence into a list of logs.
     *
     * @param line      log candidate
     */
    public void writeLog(String line) {
        if (LoggingConfig.PARALLEL_JOIN_VERBOSE) {
            logs.add(line);
        }
    }

    public void initTasks() throws Exception {
        int[] order = new int[]{0, 1, 2};
//        System.arraycopy(QueryStats.optimal, 0, order, 0, nrJoined);
        // Lookup or generate left-deep query plan
        JoinOrder joinOrder = new JoinOrder(order);
        int joinHash = joinOrder.splitHashCode(-1);
        JoinInfo info = taskCache.get(joinHash);
        if (info == null) {
            LeftDeepPartitionPlan plan = new LeftDeepPartitionPlan(query, predToEval, joinOrder);
            JoinInfo joinInfo = new JoinInfo(plan, null, new int[nrJoined]);
            taskCache.putIfAbsent(joinHash, joinInfo);
            info = joinInfo;
        }
        this.plan = info.plan;
        // Initialize the set of tasks
        if (this.curTasks == null) {
            this.curTasks = new ConcurrentLinkedQueue[nrJoined];
            this.createdTasks = new ConcurrentLinkedQueue[nrJoined];
            for (int joinCtr = 0; joinCtr < nrJoined; joinCtr++) {
                this.curTasks[joinCtr] = new ConcurrentLinkedQueue<>();
                this.createdTasks[joinCtr] = new ConcurrentLinkedQueue<>();
            }
            // Add first task
            JoinMicroTask initTask = new JoinMicroTask(new int[nrJoined], 0, this, -1);
            initTask.compute();
        }

        // Atomic flags
        AtomicInteger budget = new AtomicInteger(Integer.MAX_VALUE);
        AtomicBoolean isFinished = new AtomicBoolean(false);
        for (int tid = 0; tid < nrThreads; tid++) {
            JoinMicroThread thread = new JoinMicroThread(isFinished, budget, this.curTasks, threads, this);
            threads.add(thread);
            // task thread
            LinkedList<int[]>[] list = new LinkedList[nrJoined];
            for (int joinCtr = 0; joinCtr < nrJoined; joinCtr++) {
                list[joinCtr] = new LinkedList<>();
            }
            JoinTaskThread taskThread = new JoinTaskThread(isFinished, budget, this.curTasks, taskThreads, this);
            taskThreads.add(taskThread);
        }
        List<Future<Double>> rewardFutures = threadPool.invokeAll(threads);
        double reward = 0;
        for (Future<Double> result: rewardFutures) {
            reward += result.get();
        }
        // Initialize the set of tasks
        int threadCtr = 0;
        int nrTasks = 0;
        for (int table = 0; table < nrJoined; table++) {
            int tableTask = createdTasks[table].size();
            nrTasks += tableTask;
            System.out.println("Table Tasks: " + tableTask);
            this.curTasks[table].addAll(createdTasks[table]);
        }
    }

    public void runTasks() throws Exception {
        int[] order = new int[]{0, 1, 2};
//        System.arraycopy(QueryStats.optimal, 0, order, 0, nrJoined);
        // Lookup or generate left-deep query plan
        JoinOrder joinOrder = new JoinOrder(order);
        int joinHash = joinOrder.splitHashCode(-1);
        JoinInfo info = taskCache.get(joinHash);
        if (info == null) {
            LeftDeepPartitionPlan plan = new LeftDeepPartitionPlan(query, predToEval, joinOrder);
            JoinInfo joinInfo = new JoinInfo(plan, null, new int[nrJoined]);
            taskCache.putIfAbsent(joinHash, joinInfo);
            info = joinInfo;
        }
        this.plan = info.plan;
        taskThreads.get(0).reset(this.budget * nrThreads);

        List<Future<Double>> rewardFutures = threadPool.invokeAll(taskThreads);
        double reward = 0;
        for (Future<Double> result: rewardFutures) {
            reward += result.get();
        }
        int nrTask = 0;
        for (ConcurrentLinkedQueue<int[]> tasks: this.curTasks) {
            nrTask += tasks.size();
        }
        this.isFinished = nrTask == 0;
    }

    public void simulateTasks() throws Exception {
        int[] order = new int[]{0, 1, 2};
//        System.arraycopy(QueryStats.optimal, 0, order, 0, nrJoined);
        // Lookup or generate left-deep query plan
        JoinOrder joinOrder = new JoinOrder(order);
        int joinHash = joinOrder.splitHashCode(-1);
        JoinInfo info = taskCache.get(joinHash);
        int localBudget = 500;
        if (info == null) {
            LeftDeepPartitionPlan plan = new LeftDeepPartitionPlan(query, predToEval, joinOrder);
            JoinInfo joinInfo = new JoinInfo(plan, null, new int[nrJoined]);
            taskCache.putIfAbsent(joinHash, joinInfo);
            info = joinInfo;
        }

        this.plan = info.plan;
        // Initialize the set of tasks
        if (this.simTasks == null) {
            this.simTasks = new ConcurrentLinkedQueue<>();
        }

        JoinMicroTask firstTask = new JoinMicroTask(this);
        LinkedList<int[]> taskList = firstTask.seqTasks();
        Iterator<int[]> taskIter = taskList.iterator();
        List<int[]> smallList = new ArrayList<>(localBudget);
        while (taskIter.hasNext()) {
            int[] task = taskIter.next();
            smallList.add(task);
            if (smallList.size() == localBudget) {
                this.simTasks.add(smallList);
                smallList = new ArrayList<>(localBudget);
            }
        }
        if (smallList.size() > 0) {
            this.simTasks.add(smallList);
        }
        AtomicInteger budget = new AtomicInteger(Integer.MAX_VALUE);
        AtomicBoolean isFinished = new AtomicBoolean(false);
        for (int tid = 0; tid < nrThreads; tid++) {
            JoinSimThread simThread = new JoinSimThread(isFinished, budget, this.simTasks, simThreads,
                    this, localBudget);
            simThreads.add(simThread);
        }
        // Initialize the set of tasks
        int nrTasks = simTasks.size();
        System.out.println("Nr Tasks: " + nrTasks);
    }

    public void runSimulatedTasks() throws Exception {
        int[] order = new int[]{0, 1, 2};
//        System.arraycopy(QueryStats.optimal, 0, order, 0, nrJoined);
        // Lookup or generate left-deep query plan
        JoinOrder joinOrder = new JoinOrder(order);
        int joinHash = joinOrder.splitHashCode(-1);
        JoinInfo info = taskCache.get(joinHash);
        if (info == null) {
            LeftDeepPartitionPlan plan = new LeftDeepPartitionPlan(query, predToEval, joinOrder);
            JoinInfo joinInfo = new JoinInfo(plan, null, new int[nrJoined]);
            taskCache.putIfAbsent(joinHash, joinInfo);
            info = joinInfo;
        }
        this.plan = info.plan;
        simThreads.get(0).reset(this.budget * nrThreads);

        List<Future<Double>> rewardFutures = threadPool.invokeAll(simThreads);
        double reward = 0;
        for (Future<Double> result: rewardFutures) {
            reward += result.get();
        }
        int nrTask = this.simTasks.size();
        this.isFinished = nrTask == 0;
    }
}
