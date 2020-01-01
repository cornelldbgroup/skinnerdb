package joining.parallel.uct;

import config.JoinConfig;
import joining.parallel.join.DPJoin;
import joining.parallel.join.SPJoin;
import joining.parallel.threads.ThreadPool;
import joining.plan.JoinOrder;
import joining.uct.SelectionPolicy;
import query.QueryInfo;
import statistics.JoinStats;

import java.util.*;
import java.util.concurrent.*;

/**
 * Represents node in parallel UCT search tree.
 * In each training episode, synchronize reward gains
 * for all threads.
 *
 * @author Ziyun Wei
 */
public class SyncNode {
    /**
     * Used for randomized selection policy.
     */
    final Random random = new Random();
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
     * Actions that have not been tried yet - if the
     * heuristic is used, this only contains actions
     * that have not been tried and are recommended.
     */
    final List<Integer> priorityActions;
    /**
     * Assigns each action index to child node.
     */
    public final SyncNode[] childNodes;
    /**
     * Number of times this node was visited.
     */
    int nrVisits = 0;
    /**
     * Number of times each action was tried out.
     */
    public final int[] nrTries;
    /**
     * Reward accumulated for specific actions.
     */
    public final double[] accumulatedReward;
    /**
     * Total number of tables to join.
     */
    final int nrTables;
    /**
     * Set of already joined tables (each UCT node represents
     * a state in which a subset of tables are joined).
     */
    final Set<Integer> joinedTables;
    /**
     * List of unjoined tables (we use a list instead of a set
     * to enable shuffling during playouts).
     */
    final List<Integer> unjoinedTables;
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
     * Number of threads.
     */
    final int nrThreads;

    /**
     * Initialize UCT root node.
     *
     * @param roundCtr     current round number
     * @param query        the query which is optimized
     * @param useHeuristic whether to avoid Cartesian products
     */
    public SyncNode(long roundCtr, QueryInfo query,
                    boolean useHeuristic, int nrThreads) {
        // Count node generation
        ++JoinStats.nrUctNodes;
        this.query = query;
        this.nrTables = query.nrJoined;
        createdIn = roundCtr;
        treeLevel = 0;
        nrActions = nrTables;
        this.nrThreads = nrThreads;
        priorityActions = new ArrayList<>();
        for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
            priorityActions.add(actionCtr);
        }
        childNodes = new SyncNode[nrActions];
        nrTries = new int[nrActions];
        accumulatedReward = new double[nrActions];
        joinedTables = new HashSet<>();
        unjoinedTables = new ArrayList<>();
        nextTable = new int[nrTables];
        for (int tableCtr = 0; tableCtr < nrTables; ++tableCtr) {
            unjoinedTables.add(tableCtr);
            nextTable[tableCtr] = tableCtr;
        }
        this.useHeuristic = useHeuristic;
        recommendedActions = new HashSet<>();
        for (int action = 0; action < nrActions; ++action) {
            accumulatedReward[action] = 0;
            recommendedActions.add(action);
        }
    }

    /**
     * Initializes UCT node by expanding parent node.
     *
     * @param roundCtr    current round number
     * @param parent      parent node in UCT tree
     * @param joinedTable new joined table
     */
    public SyncNode(long roundCtr, SyncNode parent, int joinedTable) {
        // Count node generation
        ++JoinStats.nrUctNodes;
        createdIn = roundCtr;
        treeLevel = parent.treeLevel + 1;
        nrActions = parent.nrActions - 1;
        childNodes = new SyncNode[nrActions];
        nrTries = new int[nrActions];
        accumulatedReward = new double[nrActions];
        query = parent.query;
        nrTables = parent.nrTables;
        nrThreads = parent.nrThreads;
        joinedTables = new HashSet<>();
        joinedTables.addAll(parent.joinedTables);
        joinedTables.add(joinedTable);
        unjoinedTables = new ArrayList<>();
        unjoinedTables.addAll(parent.unjoinedTables);
        int indexToRemove = unjoinedTables.indexOf(joinedTable);
        unjoinedTables.remove(indexToRemove);
        nextTable = new int[nrActions];
        for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
            accumulatedReward[actionCtr] = 0;
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
                    recommendedActions.add(actionCtr);
                }
            }
        } // if heuristic is used
        else {
            recommendedActions = null;
        }
        // Collect untried actions, restrict to recommended actions
        // if the heuristic is activated.
        priorityActions = new ArrayList<>();
        for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
            if (!useHeuristic || recommendedActions.contains(actionCtr)) {
                priorityActions.add(actionCtr);
            }
        }
    }

    /**
     * Select most interesting action to try next. Also updates
     * list of unvisited actions.
     *
     * @param policy policy used to select action
     * @return index of action to try next
     */
    int selectAction(SelectionPolicy policy) {
        // Are there untried actions?
        if (!priorityActions.isEmpty()) {
            int nrUntried = priorityActions.size();
            int actionIndex = random.nextInt(nrUntried);
            int action = priorityActions.get(actionIndex);
            // Remove from untried actions and return
            priorityActions.remove(actionIndex);
            // System.out.println("Untried action: " + action);
            return action;
        } else {
            /* When using the default selection policy (UCB1):
             * We apply the UCT formula as no actions are untried.
             * We iterate over all actions and calculate their
             * UCT value, updating best action and best UCT value
             * on the way. We start iterations with a randomly
             * selected action to ensure that we pick a random
             * action among the ones with maximal UCT value.
             */
            int offset = random.nextInt(nrActions);
            int bestAction = -1;
            double bestQuality = -1;
            for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
                // Calculate index of current action
                int action = (offset + actionCtr) % nrActions;
                // if heuristic is used, choose only from recommended actions
                if (useHeuristic && recommendedActions.size() == 0) {
                    throw new RuntimeException("there are no recommended exception and we are trying to use heuristic");
                }
                if (useHeuristic && !recommendedActions.contains(action))
                    continue;
                double meanReward = accumulatedReward[action] / nrTries[action];
                double exploration = Math.sqrt(Math.log(nrVisits) / nrTries[action]);
                // Assess the quality of the action according to policy
                double quality = -1;
                switch (policy) {
                    case UCB1:
                        quality = meanReward +
                                JoinConfig.EXPLORATION_WEIGHT * exploration;
                        break;
                    case MAX_REWARD:
                    case EPSILON_GREEDY:
                        quality = meanReward;
                        break;
                    case RANDOM:
                        quality = random.nextDouble();
                        break;
                    case RANDOM_UCB1:
                        if (treeLevel == 0) {
                            quality = random.nextDouble();
                        } else {
                            quality = meanReward +
                                    JoinConfig.EXPLORATION_WEIGHT * exploration;
                        }
                        break;
                }
                //double UB = meanReward + 1E-6 * exploration;
                //double UB = meanReward + 1E-4 * exploration;
                //double UB = meanReward + 1E-1 * exploration;
                if (quality > bestQuality) {
                    bestAction = action;
                    bestQuality = quality;
                }
            }
            // For epsilon greedy, return random action with
            // probability epsilon.
            if (policy.equals(SelectionPolicy.EPSILON_GREEDY)) {
                if (random.nextDouble() <= JoinConfig.EPSILON) {
                    return random.nextInt(nrActions);
                }
            }
            // Otherwise: return best action.
            return bestAction;
        } // if there are unvisited actions
    }

    /**
     * Updates UCT statistics after sampling.
     *
     * @param selectedAction action taken
     * @param reward         reward achieved
     */
    void updateStatistics(int selectedAction, double reward) {
        ++nrVisits;
        ++nrTries[selectedAction];
        accumulatedReward[selectedAction] += reward;
    }

    /**
     * Randomly complete join order with remaining tables,
     * invoke evaluation, and return obtained reward.
     *
     * @param joinOrder a list of different join orders
     * @return obtained reward
     */
    double playoutDP(List<DPJoin> joinOps, int[] joinOrder, long roundCtr) throws Exception {
        // Last selected table
        int lastTable = joinOrder[treeLevel];
        // Should we avoid Cartesian product joins?
        if (useHeuristic) {
            Set<Integer> newlyJoined = new HashSet<>(joinedTables);
            newlyJoined.add(lastTable);
            // Iterate over join order positions to fill
            List<Integer> unjoinedTablesShuffled = new ArrayList<>(unjoinedTables);
            Collections.shuffle(unjoinedTablesShuffled);
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
        }
        else {
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
        // Evaluate completed join order and return reward
        return syncExecuteDP(joinOps, joinOrder, roundCtr);
    }

    double playout(List<SPJoin> joinOps, int[] prefix, long roundCtr) throws Exception {
        // Last selected table
        int lastTable = prefix[treeLevel];
        List<int[]> joinOrders = new ArrayList<>();
        // Should we avoid Cartesian product joins?
        for (int i = 0; i < nrThreads; i++) {
            int[] joinOrder = Arrays.copyOf(prefix, nrTables);
            joinOrders.add(joinOrder);
            if (useHeuristic) {
                Set<Integer> newlyJoined = new HashSet<>(joinedTables);
                newlyJoined.add(lastTable);
                // Iterate over join order positions to fill
                List<Integer> unjoinedTablesShuffled = new ArrayList<>(unjoinedTables);
                Collections.shuffle(unjoinedTablesShuffled);
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
            }
            else {
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
        // Evaluate completed join order and return reward
        return syncExecute(joinOps, joinOrders, roundCtr);
    }

    /**
     * Recursively sample from UCT tree and return reward.
     *
     * @param roundCtr  current round (used as timestamp for expansion)
     * @param joinOrder partially completed join order
     * @param policy    policy used to select actions
     * @return achieved reward
     */
    public double sample(long roundCtr, int[] joinOrder,
                         SelectionPolicy policy, List<SPJoin> joinOps) throws Exception {
        // Check if this is a (non-extendible) leaf node
        if (nrActions == 0) {
            List<int[]> joinOrders = new ArrayList<>(1);
            joinOrders.add(joinOrder);
            // leaf node - evaluate join order and return reward
            return syncExecute(joinOps, joinOrders, roundCtr);
        } else {
            // inner node - select next action and expand tree if necessary
            int action = selectAction(policy);
            int table = nextTable[action];
            joinOrder[treeLevel] = table;
            // grow tree if possible
            boolean canExpand = createdIn != roundCtr;
            if (childNodes[action] == null && canExpand) {
                childNodes[action] = new SyncNode(roundCtr, this, table);
            }
            // evaluate via recursive invocation or via playout
            SyncNode child = childNodes[action];
            double reward = (child != null) ?
                    child.sample(roundCtr, joinOrder, policy, joinOps) :
                    playout(joinOps, joinOrder, roundCtr);
            // update UCT statistics and return reward
            updateStatistics(action, reward);
            return reward;
        }
    }

    public double sampleDP(long roundCtr, int[] joinOrder,
                         SelectionPolicy policy, List<DPJoin> joinOps) throws Exception {
        // Check if this is a (non-extendible) leaf node
        if (nrActions == 0) {
            // leaf node - evaluate join order and return reward
            return syncExecuteDP(joinOps, joinOrder, roundCtr);
        } else {
            // inner node - select next action and expand tree if necessary
            int action = selectAction(policy);
            int table = nextTable[action];
            joinOrder[treeLevel] = table;
            // grow tree if possible
            boolean canExpand = createdIn != roundCtr;
            if (childNodes[action] == null && canExpand) {
                childNodes[action] = new SyncNode(roundCtr, this, table);
            }
            // evaluate via recursive invocation or via playout
            SyncNode child = childNodes[action];
            double reward = (child != null) ?
                    child.sampleDP(roundCtr, joinOrder, policy, joinOps) :
                    playoutDP(joinOps, joinOrder, roundCtr);
            // update UCT statistics and return reward
            updateStatistics(action, reward);
            return reward;
        }
    }

    /**
     * Execute a join order in each thread using thread parallelization.
     * Then synchronize the reward gain after all threads finish the execution.
     *
     * @param joinOps       list of join operators.
     * @param joinOrders    list of join orders
     * @param roundCtr      episode number
     * @return              reward gain
     * @throws Exception
     */
    public double syncExecute(List<SPJoin> joinOps, List<int[]> joinOrders, long roundCtr) throws Exception {
        double reward = 0;
        if (joinOrders.size() == 1) {
            SPJoin joinOp = joinOps.get(0);
            return joinOp.execute(joinOrders.get(0), (int) roundCtr);
        }
        List<Future<Double>> futures = new ArrayList<>();
        // Initialize a thread pool.
        ExecutorService executorService = ThreadPool.executorService;
        // initialize tasks for threads.
        int tid = 0;
        for (int[] order : joinOrders) {
            SPJoin joinOp = joinOps.get(tid);
            futures.add(executorService.submit(() -> joinOp.execute(order, (int) roundCtr)));
            tid++;
        }
        for (int i = 0; i < joinOrders.size(); i++) {
            Future<Double> futureResult = futures.get(i);
            try {
                double result = futureResult.get();
//                System.out.println(i + ": " + Arrays.toString(joinOrders.get(i)) + " " + result);
                reward += result;
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
        reward = reward / joinOrders.size();
        return reward;
    }

    public double syncExecuteDP(List<DPJoin> joinOps, int[] joinOrder, long roundCtr) throws Exception {
        double reward = 0;
        List<Future<Double>> futures = new ArrayList<>();
        // Initialize a thread pool.
        ExecutorService executorService = ThreadPool.executorService;
        int splitTable = getSplitTableByCard(joinOrder, joinOps.get(0).cardinalities);
        // initialize tasks for threads.
        for (DPJoin joinOp : joinOps) {
            futures.add(executorService.submit(() -> joinOp.execute(joinOrder, splitTable, (int) roundCtr)));
        }
        for (int i = 0; i < joinOps.size(); i++) {
            Future<Double> futureResult = futures.get(i);
            try {
                double result = futureResult.get();
//                System.out.println(i + ": " + Arrays.toString(joinOrders.get(i)) + " " + result);
                reward += result;
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
        reward = reward / joinOps.size();
        return reward;
    }

    /**
     * Get the split table candidate based on cardinalities of tables.
     *
     * @param joinOrder         join order
     * @param cardinalities     cardinalities of tables
     * @return
     */
    int getSplitTableByCard(int[] joinOrder, int[] cardinalities) {
        int splitLen = 5;
        int splitSize = 1000;
        int splitTable = joinOrder[0];
        int end = Math.min(splitLen, joinOrder.length);
        for (int i = 0; i < end; i++) {
            int table = joinOrder[i];
            int cardinality = cardinalities[table];
            if (cardinality > splitSize) {
                splitTable = table;
                break;
            }
        }
        return splitTable;
    }
}