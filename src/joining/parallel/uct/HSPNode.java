package joining.parallel.uct;

import config.JoinConfig;
import config.ParallelConfig;
import joining.parallel.join.SPJoin;
import joining.uct.SelectionPolicy;
import query.QueryInfo;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Represents node in search parallel UCT search tree.
 *
 * @author Ziyun Wei
 */
public class HSPNode {
    /**
     * The query for which we are optimizing.
     */
    final QueryInfo query;
    /**
     * Iteration in which node was created.
     */
    final long createdIn;
    /**
     * Level of node in tree (root node has level 0).
     * At the same time the join order index into
     * which table selected in this node is inserted.
     */
    public final int treeLevel;
    /**
     * Number of possible actions from this state.
     */
    public final int nrActions;
    /**
     * Assigns each action index to child node.
     */
    public final HSPNode[] childNodes;
    /**
     * The table to join.
     */
    public final int joinedTable;
    /**
     * Number of times this node was visited.
     */
    private int[] nrVisits;
    /**
     * Number of times each action was tried out.
     */
    private final int[][] nrTries;
    /**
     * Reward accumulated for specific actions.
     */
    private final double[][] accumulatedReward;
    /**
     * node statistics that should be aligned to a cache line
     */
    public NodeStatistics[] nodeStatistics;
    /**
     * Total number of tables to join.
     */
    final int nrTables;
    /**
     * List of unjoined tables (we use a list instead of a set
     * to enable shuffling during playouts).
     */
    final List<Integer> unjoinedTables;
    /**
     * Set of already joined tables (each UCT node represents
     * a state in which a subset of tables are joined).
     */
    public final Set<Integer> joinedTables;
    /**
     * Associates each action index with a next table to join.
     */
    public final int[] nextTable;
    /**
     * Indicates whether the search space is restricted to
     * join orders that avoid Cartesian products. This
     * flag should only be activated if it is ensured
     * that a given query can be evaluated under that
     * constraint.
     */
    final boolean useHeuristic;
    /**
     * Contains actions that are consistent with the "avoid
     * Cartesian products" heuristic. UCT algorithm will
     * restrict focus on such actions if heuristic flag
     * is activated.
     */
    final Set<Integer> recommendedActions;
    /**
     * concurrent priority set
     */
    public LinkedList<Integer> prioritySet;
    /**
     * Number of threads.
     */
    final int nrThreads;
    /**
     * the parent of current node
     */
    public final HSPNode parent;
    /**
     * The action number of parent node;
     */
    public final int action;
    /**
     * The id of search space.
     */
    public int sid = -1;
    /**
     * Timeout for next forget
     */
    public final int[] nextForget;

    /**
     * Initialize UCT root node.
     *
     * @param roundCtr     	current round number
     * @param query        	the query which is optimized
     * @param useHeuristic 	whether to avoid Cartesian products
     */
    public HSPNode(long roundCtr, QueryInfo query,
                   boolean useHeuristic, int nrThreads) {
        // Count node generation
        joinedTable = 0;
        this.query = query;
        this.nrTables = query.nrJoined;
        this.nrThreads = nrThreads;
        createdIn = roundCtr;
        treeLevel = 0;
        nrActions = nrTables;
        childNodes = new HSPNode[nrActions];
        nrVisits = new int[nrThreads];
        nrTries = new int[nrThreads][nrActions];
        accumulatedReward = new double[nrThreads][nrActions];
        unjoinedTables = new ArrayList<>();
        joinedTables = new HashSet<>();
        nextTable = new int[nrTables];
        parent = null;
        for (int tableCtr = 0; tableCtr < nrTables; ++tableCtr) {
            unjoinedTables.add(tableCtr);
            nextTable[tableCtr] = tableCtr;
        }
        this.useHeuristic = useHeuristic;
        recommendedActions = new HashSet<>();
        for (int action = 0; action < nrActions; ++action) {
            int table = nextTable[action];
            if (!query.temporaryTables.contains(table)) {
                recommendedActions.add(action);
            }
        }
        this.nodeStatistics = new NodeStatistics[nrThreads];

        for (int i = 0; i < nrThreads; i++) {
            this.nodeStatistics[i] = new NodeStatistics(nrActions);
        }

        this.prioritySet = new LinkedList<>();
        for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
            int table = nextTable[actionCtr];
            if (!query.temporaryTables.contains(table)) {
                prioritySet.add(actionCtr);
            }
        }
        this.action = 0;
        nextForget = new int[nrThreads];
        Arrays.fill(nextForget, 10);
    }
    /**
     * Initializes UCT node by expanding parent node.
     *
     * @param roundCtr    current round number
     * @param parent      parent node in UCT tree
     * @param joinedTable new joined table
     */
    public HSPNode(long roundCtr, HSPNode parent, int joinedTable, int action) {
        // Count node generation
        this.joinedTable = joinedTable;
        createdIn = roundCtr;
        treeLevel = parent.treeLevel + 1;
        nrActions = parent.nrActions - 1;
        nrThreads = parent.nrThreads;
        childNodes = new HSPNode[nrActions];
        nrVisits = new int[nrThreads];
        nrTries = new int[nrThreads][nrActions];
        accumulatedReward = new double[nrThreads][nrActions];
        query = parent.query;
        nrTables = parent.nrTables;
        unjoinedTables = new ArrayList<>();
        joinedTables = new HashSet<>();
        joinedTables.addAll(parent.joinedTables);
        joinedTables.add(joinedTable);
        this.parent = parent;
        for (Integer table : parent.unjoinedTables) {
            if (table != joinedTable) {
                unjoinedTables.add(table);
            }
        }
        this.nodeStatistics = new NodeStatistics[nrThreads];

        for (int i = 0; i < nrThreads; i++) {
            this.nodeStatistics[i] = new NodeStatistics(nrActions);
        }
        nextTable = new int[nrActions];
        for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
            nextTable[actionCtr] = unjoinedTables.get(actionCtr);
        }
        // Calculate recommended actions if heuristic is activated
        this.useHeuristic = parent.useHeuristic;
        if (useHeuristic) {
            recommendedActions = new HashSet<>();
            // Iterate over all actions
            for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
                // Get table associated with (join) action
                int table = nextTable[actionCtr];
                // Check if at least one predicate connects current
                // tables to new table.
                if (query.connected(joinedTables, table)) {
                    recommendedActions.add(actionCtr);
                } // over predicates
            } // over actions
            if (recommendedActions.isEmpty()) {
                // add all actions to recommended actions
                for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
                    int table = nextTable[actionCtr];
                    if (!query.temporaryTables.contains(table)) {
                        recommendedActions.add(actionCtr);
                    }
                }
            }
            if (recommendedActions.isEmpty()) {
                // add all actions to recommended actions
                for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
                    recommendedActions.add(actionCtr);
                }
            }
        } // if heuristic is used
        else {
            recommendedActions = null;
        }

        List<Integer> priorityActions = new ArrayList<>();
        for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
            if (!useHeuristic || recommendedActions.contains(actionCtr)) {
                priorityActions.add(actionCtr);
            }
        }
        this.prioritySet = new LinkedList<>(priorityActions);
        this.action = action;
        nextForget = new int[nrThreads];
        Arrays.fill(nextForget, 100);
    }


    /**
     * Select most interesting action to try next. Also updates
     * list of unvisited actions.
     *
     * @param policy	policy used to select action
     * @return index of action to try next
     */
    int selectAction(long roundCtr, SelectionPolicy policy, int tid,
                     SPJoin spJoin, boolean isLocal,
                     boolean useLearning) {

        if (useLearning) {
            /*
             * We apply the UCT formula as no actions are untried.
             * We iterate over all actions and calculate their
             * UCT value, updating best action and best UCT value
             * on the way. We start iterations with a randomly
             * selected action to ensure that we pick a random
             * action among the ones with maximal UCT value.
             */
            Integer priorAction = null;
            if (!prioritySet.isEmpty()) {
                priorAction = prioritySet.pollFirst();
            }
            if (priorAction != null) {
                return priorAction;
            }
            int nrVisits = 0;
            int[] nrTries = new int[nrActions];
            double[] accumulatedReward = new double[nrActions];
            if (isLocal) {
                NodeStatistics threadStats = nodeStatistics[tid];
                nrVisits += threadStats.nrVisits;
                for(Integer recAction : recommendedActions) {
                    int threadTries = threadStats.nrTries[recAction];
                    nrTries[recAction] += threadTries;
                    accumulatedReward[recAction] += threadStats.accumulatedReward[recAction];
                }
            }
            else {
                nrVisits += this.nrVisits[tid];
                for(Integer recAction : recommendedActions) {
                    int threadTries = this.nrTries[tid][recAction];
                    nrTries[recAction] += threadTries;
                    accumulatedReward[recAction] += this.accumulatedReward[tid][recAction];
                }
            }
            /* When using the default selection policy (UCB1):
             * We apply the UCT formula as no actions are untried.
             * We iterate over all actions and calculate their
             * UCT value, updating best action and best UCT value
             * on the way. We start iterations with a randomly
             * selected action to ensure that we pick a random
             * action among the ones with maximal UCT value.
             */
            int bestAction = -1;
            double bestQuality = -1;
            List<Integer> randomActions = new ArrayList<>(recommendedActions);
            Collections.shuffle(randomActions, ThreadLocalRandom.current());
            for (Integer action : randomActions) {
                // Calculate index of current action
                int nrTry = nrTries[action];
                if (nrTry == 0) {
                    return action;
                }
                double meanReward = accumulatedReward[action] / nrTry;
                double exploration = Math.sqrt(Math.log(nrVisits) / nrTry);
                // Assess the quality of the action according to policy
                double quality = meanReward + JoinConfig.EXPLORATION_WEIGHT * exploration;
                if (quality > bestQuality) {
                    bestAction = action;
                    bestQuality = quality;
                }
            }
            // Otherwise: return best action.
            return bestAction;

        }
        else {
            int bestAction = -1;
            double bestQuality = Integer.MAX_VALUE;
            List<Integer> randomActions = new ArrayList<>(recommendedActions);
            Collections.shuffle(randomActions, ThreadLocalRandom.current());
            switch (ParallelConfig.HEURISTIC_POLICY) {
                case 0: {
                    for(Integer recAction : randomActions) {
                        int table = nextTable[recAction];
                        int cardinality = spJoin.cardinalities[table];
                        if (cardinality < bestQuality) {
                            bestAction = recAction;
                            bestQuality = cardinality;
                        }
                    }
                    break;
                }
                case 1: {
//                    spJoin.writeLog(Arrays.toString(recommendedActions.stream().mapToDouble(recAction -> {
//                        int table = nextTable[recAction];
//                        int cardinality = spJoin.cardinalities[table];
//                        return treeLevel == 0 ?
//                                cardinality :
//                                query.estimate(joinedTables, table);
//                    }).toArray()) + "\n" + Arrays.toString(recommendedActions.stream().mapToDouble(recAction -> nextTable[recAction]).toArray()) +
//                            "\n" + Arrays.toString(recommendedActions.stream().mapToInt(recAction -> {
//                        int table = nextTable[recAction];
//                        int cardinality = spJoin.cardinalities[table];
//                        return cardinality;
//                    }).toArray()));
                    for(Integer recAction : randomActions) {
                        int table = nextTable[recAction];
                        int cardinality = spJoin.cardinalities[table];
                        double selectivity = treeLevel == 0 ?
                                cardinality : query.estimate(joinedTables, table) * cardinality;
                        if (selectivity < bestQuality) {
                            bestAction = recAction;
                            bestQuality = selectivity;
                        }
                    }
                    break;
                }
                default: {

                }
            }
            return bestAction;
        }
    }
    /**
     * Updates UCT statistics after sampling.
     *
     * @param selectedAction action taken
     * @param reward         reward achieved
     */
    void updateStatistics(int selectedAction, double reward, int tid) {
        accumulatedReward[tid][selectedAction] += reward;
        ++nrVisits[tid];
        ++nrTries[tid][selectedAction];
    }
    /**
     * Randomly complete join order with remaining tables,
     * invoke evaluation, and return obtained reward.
     *
     * @param joinOrder partially completed join order
     * @return obtained reward
     */
    double playout(long roundCtr, int[] joinOrder, SPJoin spJoin, boolean useLearning) throws Exception {
        // Last selected table
        int lastTable = joinOrder[treeLevel];
        // Should we avoid Cartesian product joins?
        if (useLearning) {
            if (useHeuristic) {
                Set<Integer> newlyJoined = new HashSet<>(joinedTables);
                newlyJoined.add(lastTable);
                // Iterate over join order positions to fill
                List<Integer> unjoinedTablesShuffled = new ArrayList<>(unjoinedTables);
                Collections.shuffle(unjoinedTablesShuffled, ThreadLocalRandom.current());
                for (int posCtr = treeLevel + 1; posCtr < nrTables; ++posCtr) {
                    boolean foundTable = false;
                    for (int table : unjoinedTablesShuffled) {
                        if (!newlyJoined.contains(table) &&
                                query.connected(newlyJoined, table)) {
                            joinOrder[posCtr] = table;
                            newlyJoined.add(table);
                            foundTable = true;
                            break;
                        }
                    }
                    if (!foundTable) {
                        for (int table : unjoinedTablesShuffled) {
                            if (!newlyJoined.contains(table)) {
                                joinOrder[posCtr] = table;
                                newlyJoined.add(table);
                                break;
                            }
                        }
                    }
                }
            } else {
                // Shuffle remaining tables
                Collections.shuffle(unjoinedTables);
                Iterator<Integer> unjoinedTablesIter = unjoinedTables.iterator();
                // Fill in remaining join order positions
                for (int posCtr = treeLevel + 1; posCtr < nrTables; ++posCtr) {
                    int nextTable = unjoinedTablesIter.next();
                    while (nextTable == lastTable) {
                        nextTable = unjoinedTablesIter.next();
                    }
                    joinOrder[posCtr] = nextTable;
                }
            }
        }
        else {
            Set<Integer> newlyJoined = new HashSet<>(joinedTables);
            newlyJoined.add(lastTable);
            // Iterate over join order positions to fill
            List<Integer> unjoinedTablesShuffled = new ArrayList<>(unjoinedTables);
            switch (ParallelConfig.HEURISTIC_POLICY) {
                case 0: {
                    for (int posCtr = treeLevel + 1; posCtr < nrTables; ++posCtr) {
                        int bestTable = -1;
                        double bestQuality = Integer.MAX_VALUE;
                        for (int table : unjoinedTablesShuffled) {
                            if (!newlyJoined.contains(table) &&
                                    query.connected(newlyJoined, table)) {
                                int cardinality = spJoin.cardinalities[table];
                                if (cardinality < bestQuality) {
                                    bestTable = table;
                                    bestQuality = cardinality;
                                }
                            }
                        }
                        if (bestTable < 0) {
                            for (int table : unjoinedTablesShuffled) {
                                if (!newlyJoined.contains(table)) {
                                    bestTable = table;
                                    break;
                                }
                            }
                        }
                        joinOrder[posCtr] = bestTable;
                        newlyJoined.add(bestTable);
                    }
                    break;
                }
                case 1: {
                    for (int posCtr = treeLevel + 1; posCtr < nrTables; ++posCtr) {
                        int bestTable = -1;
                        double bestQuality = Integer.MAX_VALUE;
                        for (int table : unjoinedTablesShuffled) {
                            if (!newlyJoined.contains(table) &&
                                    query.connected(newlyJoined, table)) {
                                int cardinality = spJoin.cardinalities[table];
                                double selectivity = treeLevel == 0 ?
                                        cardinality : query.estimate(joinedTables, table) * cardinality;
                                if (selectivity < bestQuality) {
                                    bestTable = table;
                                    bestQuality = selectivity;
                                }
                            }
                        }
                        if (bestTable < 0) {
                            for (int table : unjoinedTablesShuffled) {
                                if (!newlyJoined.contains(table)) {
                                    bestTable = table;
                                    break;
                                }
                            }
                        }
                        joinOrder[posCtr] = bestTable;
                        newlyJoined.add(bestTable);
                    }
                    break;
                }
                default: {

                }
            }
        }

        // Evaluate completed join order and return reward
        return spJoin.execute(joinOrder, (int) roundCtr);
    }
    /**
     * Recursively sample from UCT tree and return reward.
     *
     * @param roundCtr  current round (used as timestamp for expansion)
     * @param joinOrder partially completed join order
     * @param policy	policy used to select actions
     * @return achieved reward
     */
    public double sample(long roundCtr, int[] joinOrder, SPJoin spJoin,
                         SelectionPolicy policy, boolean isLocal, int nrLevels, String tidStr) throws Exception {
        int tid = spJoin.tid;
        // Check if this is a (non-extendible) leaf node
        if (nrActions == 0) {
            // Initialize table nodes
            return spJoin.execute(joinOrder, (int) roundCtr);
        }
        else {
//            boolean topLevel = treeLevel < ParallelConfig.TOP_LEVEL;
//            boolean useLearning = topLevel ? tid % 2 == 0 : (tid % 4 == 0 || tid % 4 == 1);
//            int stride = nrTables / nrLevels;
            int stride = ParallelConfig.TOP_LEVEL;
            int startPos = Math.min(treeLevel / stride, nrLevels - 1);
            boolean useLearning = tidStr.charAt(nrLevels - startPos - 1) == '0';
            // inner node - select next action and expand tree if necessary
            spJoin.writeLog("Level: " + treeLevel + "\tPos: " + startPos + "\tBits: " + tidStr
                    + "\tLearning: " + useLearning);
            int action = selectAction(roundCtr, policy, tid, spJoin, isLocal, useLearning);
            int table = nextTable[action];
            joinOrder[treeLevel] = table;
            // grow tree if possible
            boolean canExpand = createdIn != roundCtr;
            HSPNode child = childNodes[action];
            // let join operator knows which space is evaluating.
            if (canExpand && child == null) {
                if (childNodes[action] == null) {
                    childNodes[action] = new HSPNode(roundCtr, this, table, action);
                }
            }
            // evaluate via recursive invocation or via playout
            boolean isSample = child != null;
            double reward = isSample ?
                    child.sample(roundCtr, joinOrder, spJoin, policy, isLocal, nrLevels, tidStr):
                    playout(roundCtr, joinOrder, spJoin, useLearning);
            // update UCT statistics and return reward
            if (isLocal) {
                nodeStatistics[tid].updateStatistics(reward, action);
            }
            else {
                updateStatistics(action, reward, tid);
            }
            return reward;
        }
    }
}