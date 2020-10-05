package joining.parallel.join;

import catalog.CatalogManager;
import config.LoggingConfig;
import config.ParallelConfig;
import expressions.ExpressionInfo;
import expressions.compilation.EvaluatorType;
import expressions.compilation.ExpressionCompiler;
import expressions.compilation.KnaryBoolEval;
import joining.parallel.statistics.StatsInstance;
import joining.parallel.uct.SPNode;
import joining.plan.HotSet;
import joining.progress.State;
import joining.result.JoinResult;
import net.sf.jsqlparser.expression.Expression;
import org.apache.commons.lang3.tuple.Pair;
import predicate.NonEquiNode;
import preprocessing.Context;
import query.QueryInfo;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class SPJoin {
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
    public final JoinResult result;
    /**
     * Number of working threads.
     */
    public final int nrThreads;
    /**
     * ID of the join operator.
     */
    public final int tid;
    /**
     * Instance of statistics records.
     */
    public StatsInstance statsInstance;
    /**
     * Last state after a episode.
     */
    public State lastState;
    /**
     * A list of logs.
     */
    public List<String> logs;
    /**
     * Number of episode.
     */
    public long roundCtr;
    /**
     * The id of search space.
     */
    public int sid = -1;
    /**
     * The prefix of tables that identify a search space.
     */
    public int prefix;
    /**
     * Partial actions assigned to a thread.
     */
    public Map<Integer, Set<Integer>> nextActions;
    /**
     * Number of update for each table.
     */
    public final int[] nrVisited;
    /**
     * Number of indexed size for each table
     */
    public final int[] nrIndexed;
    /**
     * Progress in the left most table.
     */
    public double progress;
    /**
     * The statistics of constraints.
     */
    public final Map<Pair<Integer, Integer>, int[]> constraintsStats;
    /**
     * The statistics of constraints.
     */
    public final Map<HotSet, Integer> joinStats;
    /**
     * A list of constraints of partitioned search space.
     */
    public List<Pair<Integer, Integer>> constraints = new ArrayList<>();
    /**
     * The overall counts for statistics.
     */
    public int statsCount;
    /**
     * The flag whether the space is re-partitioned.
     */
    public int nextDetect;
    /**
     * Statistics to store visit number
     */
    public final long[] visits;

    ExpressionCompiler compiler;
    KnaryBoolEval boolEval;

    /**
     * Initializes join operator for given query
     * and initialize new join result.
     *
     * @param query			query to process
     * @param preSummary	summarizes pre-processing steps
     */
    public SPJoin(QueryInfo query, Context preSummary, int budget, int nrThreads, int tid,
                  Map<Expression, NonEquiNode> predToEval) throws Exception {
        this.query = query;
        this.nrJoined = query.nrJoined;
        this.preSummary = preSummary;
        this.budget = budget;
        // Retrieve table cardinalities
        this.cardinalities = new int[nrJoined];
        this.nrThreads = nrThreads;
        this.tid = tid;
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
        this.result = new JoinResult(nrJoined);
        this.predToEval = predToEval;
        this.nrVisited = new int[nrJoined];
        this.visits = new long[nrJoined];
        this.nrIndexed = new int[nrJoined];
        this.constraintsStats = new HashMap<>();
        query.constraints.forEach(pair -> constraintsStats.put(pair, new int[2]));
        joinStats = new ConcurrentHashMap<>();
    }

    /**
     * Executes given join order for a given number of steps.
     *
     * @param order		execute this join order
     * @return			reward (higher reward means faster progress)
     * @throws Exception
     */
    public abstract double execute(int[] order, int roundCtr) throws Exception;
    /**
     * Returns true iff a complete join result was generated.
     *
     * @return	true iff query processing is finished
     */
    public abstract boolean isFinished();

    /**
     * Get the first table that has more than 1 row.
     * @param order     Join order.
     * @return          the first large table
     */
    public int getFirstLargeTable(int[] order) {
        int firstTable = order[0];
        for (int table : order) {
            if (cardinalities[table] > 1) {
                return table;
            }
        }
        return firstTable;
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

    /**
     * Initialize partial actions
     * that need to be evaluated in each level.
     *
     * @param treeLevel     the deepest tree level
     */
    public void splitSpace(int treeLevel) {
        this.prefix = treeLevel;
        nextActions = new HashMap<>();
    }

    /**
     * Initialize the search spaces for different threads.
     *
     * @param preNodes      the node candidates
     * @param nodes         a list of nodes for the thread in the tree level
     * @param treeLevel     the level where the number of nodes is larger than #Threads
     */
    public void initSearchRoot(SPNode[] preNodes, List<Integer> nodes, int treeLevel) {
        if (nodes.size() > 0) {
            this.splitSpace(treeLevel);
            nodes.forEach(nodeIndex -> {
                SPNode node = preNodes[nodeIndex];
                while (node.parent != null) {
                    int sid = node.parent.sid;
                    nextActions.computeIfAbsent(sid, key -> new HashSet<>()).add(node.action);
                    node = node.parent;
                }
            });
        }
    }
}
