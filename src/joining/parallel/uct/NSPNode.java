package joining.parallel.uct;

import config.JoinConfig;
import joining.parallel.join.HybridJoin;
import joining.parallel.join.ModJoin;
import joining.parallel.join.OldJoin;
import joining.parallel.join.SPJoin;
import joining.uct.SelectionPolicy;
import query.QueryInfo;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Represents node in search parallel UCT search tree.
 *
 * @author Anonymous
 */
public class NSPNode {
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
    public final NSPNode[] childNodes;
    /**
     * Number of times this node was visited.
     */
    int nrVisits = 0;
    /**
     * Number of times each action was tried out.
     */
    private final int[] nrTries;
    /**
     * Reward accumulated for specific actions.
     */
    private final double[] accumulatedReward;
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
    public final boolean useHeuristic;
    /**
     * Contains actions that are consistent with the "avoid
     * Cartesian products" heuristic. UCT algorithm will
     * restrict focus on such actions if heuristic flag
     * is activated.
     */
    final Set<Integer> recommendedActions;
    /**
     * Thread identification.
     */
    final int tid;
    /**
     * The start thread available for the child node.
     */
    public final int startThreads;
    /**
     * The ebd thread for the child node. (exclusive)
     */
    public final int endThreads;

    /**
     * Initialize UCT root node.
     *
     * @param roundCtr     	current round number
     * @param query        	the query which is optimized
     * @param useHeuristic 	whether to avoid Cartesian products
     */
    public NSPNode(long roundCtr, QueryInfo query,
                   boolean useHeuristic, int tid, int start, int end) {
        // Count node generation
        this.query = query;
        this.nrTables = query.nrJoined;
        this.tid = tid;
        createdIn = roundCtr;
        treeLevel = 0;
        nrActions = nrTables;
        priorityActions = new ArrayList<>();
        childNodes = new NSPNode[nrActions];
        nrTries = new int[nrActions];
        accumulatedReward = new double[nrActions];
        unjoinedTables = new ArrayList<>();
        joinedTables = new HashSet<>();
        nextTable = new int[nrTables];
        for (int tableCtr = 0; tableCtr < nrTables; ++tableCtr) {
            unjoinedTables.add(tableCtr);
            nextTable[tableCtr] = tableCtr;
        }
        this.useHeuristic = useHeuristic;
        recommendedActions = new HashSet<>();
        // Available actions
        int nrThreads = end - start;
        List<Integer> filteredActions = new ArrayList<>(nrActions);
        // Iterate over all actions
        for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
            // Get table associated with (join) action
            int table = nextTable[actionCtr];
            if (!query.temporaryTables.contains(table)) {
                filteredActions.add(actionCtr);
            }
        } // over actions
        int nrFilteredActions = filteredActions.size();
        if (nrThreads <= nrFilteredActions) {
            int baseThreads = nrFilteredActions / nrThreads;
            int remainingActions = nrFilteredActions - baseThreads * nrThreads;
            int startAction = tid * baseThreads + Math.min(remainingActions, tid);
            int endAction = startAction + baseThreads + (tid < remainingActions ? 1 : 0);
            for (int filteredActionCtr = startAction; filteredActionCtr < endAction; ++filteredActionCtr) {
                int actionCtr = filteredActions.get(filteredActionCtr);
                priorityActions.add(actionCtr);
                recommendedActions.add(actionCtr);
            }
            startThreads = tid;
            endThreads = tid + 1;
        }
        else {
            int baseActions = nrThreads / nrFilteredActions;
            int remainingThreads = nrThreads - baseActions * nrFilteredActions;
            int differentID = remainingThreads * (baseActions + 1);
            int targetAction = tid < differentID ? tid / (baseActions + 1) : (tid - remainingThreads) / baseActions;
            int startThread = baseActions * targetAction + Math.min(remainingThreads, targetAction);
            int endThread = startThread + baseActions + (targetAction < remainingThreads ? 1 : 0);
            int actionCtr = filteredActions.get(targetAction);
            priorityActions.add(actionCtr);
            recommendedActions.add(actionCtr);
            startThreads = startThread;
            endThreads = endThread;
        }

        Arrays.fill(accumulatedReward, 0);
    }
    /**
     * Initializes UCT node by expanding parent node.
     *
     * @param roundCtr    current round number
     * @param parent      parent node in UCT tree
     * @param joinedTable new joined table
     */
    public NSPNode(long roundCtr, NSPNode parent, int joinedTable) {
        // Count node generation
        createdIn = roundCtr;
        tid = parent.tid;
        treeLevel = parent.treeLevel + 1;
        nrActions = parent.nrActions - 1;
        childNodes = new NSPNode[nrActions];
        nrTries = new int[nrActions];
        accumulatedReward = new double[nrActions];
        query = parent.query;
        nrTables = parent.nrTables;
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
        List<Integer> filteredActions = new ArrayList<>(nrActions);
        if (useHeuristic) {
            // Iterate over all actions
            for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
                // Get table associated with (join) action
                int table = nextTable[actionCtr];
                // Check if at least one predicate connects current
                // tables to new table.
                if (query.connected(joinedTables, table)) {
                    filteredActions.add(actionCtr);
                } // over predicates
            } // over actions
            if (filteredActions.isEmpty()) {
                // add all actions to recommended actions
                for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
                    int table = nextTable[actionCtr];
                    if (!query.temporaryTables.contains(table)) {
                        filteredActions.add(actionCtr);
                    }
                }
            }
            if (filteredActions.isEmpty()) {
                // add all actions to recommended actions
                for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
                    filteredActions.add(actionCtr);
                }
            }
        } // if heuristic is used
        else {
            for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
                int table = nextTable[actionCtr];
                if (!query.temporaryTables.contains(table)) {
                    filteredActions.add(actionCtr);
                }
            }
            if (filteredActions.isEmpty()) {
                // add all actions to recommended actions
                for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
                    filteredActions.add(actionCtr);
                }
            }
        }

        // Search space partition
        int nrThreads = parent.endThreads - parent.startThreads;
        int shiftTid = tid - parent.startThreads;
        recommendedActions = new HashSet<>();
        priorityActions = new ArrayList<>();
        int nrFilteredActions = filteredActions.size();

        if (nrThreads == 1) {
            for (int action : filteredActions) {
                priorityActions.add(action);
                recommendedActions.add(action);
            }
            startThreads = tid;
            endThreads = tid + 1;
        }
        else if (nrThreads <= nrFilteredActions) {
            int baseThreads = nrFilteredActions / nrThreads;
            int remainingActions = nrFilteredActions - baseThreads * nrThreads;
            int startAction = shiftTid * baseThreads + Math.min(remainingActions, shiftTid);
            int endAction = startAction + baseThreads + (shiftTid < remainingActions ? 1 : 0);
            for (int filteredActionCtr = startAction; filteredActionCtr < endAction; ++filteredActionCtr) {
                int actionCtr = filteredActions.get(filteredActionCtr);
                priorityActions.add(actionCtr);
                recommendedActions.add(actionCtr);
            }
            startThreads = tid;
            endThreads = tid + 1;
        }
        else if (nrFilteredActions > 0) {
            int baseActions = nrThreads / nrFilteredActions;
            int remainingThreads = nrThreads - baseActions * nrFilteredActions;
            int differentID = remainingThreads * (baseActions + 1);
            int targetFilteredAction = shiftTid < differentID ? shiftTid / (baseActions + 1) :
                    (shiftTid - remainingThreads) / baseActions;
            int startThread = baseActions * targetFilteredAction + Math.min(remainingThreads, targetFilteredAction);
            int endThread = startThread + baseActions + (targetFilteredAction < remainingThreads ? 1 : 0);
            int targetAction = filteredActions.get(targetFilteredAction);
            priorityActions.add(targetAction);
            recommendedActions.add(targetAction);
            startThreads = startThread + parent.startThreads;
            endThreads = endThread + parent.startThreads;
        }
        else {
            startThreads = parent.startThreads;
            endThreads = parent.endThreads;
        }
        Arrays.fill(accumulatedReward, 0);
    }

    public NSPNode spaceNode() {
        // Initialize the node using the root
        NSPNode node = this;
        while (!(node.startThreads == tid && node.endThreads == tid + 1)) {
            if (node.nrActions == 0) {
                break;
            }
            int action = node.recommendedActions.iterator().next();
            int table = node.nextTable[action];
            if (node.childNodes[action] == null) {
                node.childNodes[action] = new NSPNode(node.createdIn, node, table);
            }
            node = node.childNodes[action];
        }
        return node;
    }


    /**
     * Select most interesting action to try next. Also updates
     * list of unvisited actions.
     *
     * @param policy	policy used to select action
     * @return index of action to try next
     */
    int selectAction(SelectionPolicy policy) {
        // Are there untried actions?
        if (!priorityActions.isEmpty()) {
            int nrUntried = priorityActions.size();
            int actionIndex = ThreadLocalRandom.current().nextInt(0, nrUntried);
            int action = priorityActions.get(actionIndex);
            // Remove from untried actions and return
            priorityActions.remove(actionIndex);
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
            int offset = ThreadLocalRandom.current().nextInt(0, nrActions);
            int bestAction = -1;
            double bestQuality = -1;
            for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
                // Calculate index of current action
                int action = (offset + actionCtr) % nrActions;
                // if heuristic is used, choose only from recommended actions
                if (useHeuristic && recommendedActions.size() == 0) {
                    throw new RuntimeException("there are no recommended exception and we are trying to use heuristic");
                }
                if (!recommendedActions.contains(action))
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
                }
                //double UB = meanReward + 1E-6 * exploration;
                //double UB = meanReward + 1E-4 * exploration;
                //double UB = meanReward + 1E-1 * exploration;
                if (quality > bestQuality) {
                    bestAction = action;
                    bestQuality = quality;
                }
            }
            // Otherwise: return best action.
            return bestAction;
        } // if there are unvisited actions
    }

    int selectAction() {
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
        for (int action = 0; action < nrActions; ++action) {
            // if heuristic is used, choose only from recommended actions
            if (useHeuristic && recommendedActions.size() == 0) {
                throw new RuntimeException("there are no recommended exception and we are trying to use heuristic");
            }
            if (!recommendedActions.contains(action))
                continue;
            double meanReward = nrTries[action] == 0 ? 0 : accumulatedReward[action] / nrTries[action];
            if (meanReward > bestQuality) {
                bestAction = action;
                bestQuality = meanReward;
            }
        }
        // Otherwise: return best action.
        return bestAction;
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
     * @param joinOrder partially completed join order
     * @return obtained reward
     */
    double playout(long roundCtr, int[] joinOrder, OldJoin oldJoin) throws Exception {
        // Last selected table
        int lastTable = joinOrder[treeLevel];
        // Should we avoid Cartesian product joins?
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
            Collections.shuffle(unjoinedTables, ThreadLocalRandom.current());
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
        return oldJoin.execute(joinOrder, (int) roundCtr);
    }
    /**
     * Randomly complete join order with remaining tables,
     * invoke evaluation, and return obtained reward.
     *
     * @param joinOrder partially completed join order
     * @return obtained reward
     */
    double playout(long roundCtr, int[] joinOrder, HybridJoin hybridJoin) throws Exception {
        // Last selected table
        int lastTable = joinOrder[treeLevel];
        // Should we avoid Cartesian product joins?
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
            Collections.shuffle(unjoinedTables, ThreadLocalRandom.current());
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
        return hybridJoin.execute(joinOrder, -1, (int) roundCtr);
    }
    /**
     * Recursively sample from UCT tree and return reward.
     *
     * @param roundCtr  current round (used as timestamp for expansion)
     * @param joinOrder partially completed join order
     * @param policy	policy used to select actions
     * @return achieved reward
     */
    public double sample(long roundCtr, int[] joinOrder, OldJoin oldJoin,
                         SelectionPolicy policy) throws Exception {
        // Check if this is a (non-extendible) leaf node
        if (nrActions == 0) {
            // leaf node - evaluate join order and return reward
            return oldJoin.execute(joinOrder, (int) roundCtr);
        } else {
            // inner node - select next action and expand tree if necessary
            int action = selectAction(policy);
            int table = nextTable[action];
            joinOrder[treeLevel] = table;
            // grow tree if possible
            boolean canExpand = createdIn != roundCtr;
            if (childNodes[action] == null && canExpand) {
                childNodes[action] = new NSPNode(roundCtr, this, table);
            }
            // evaluate via recursive invocation or via playout
            NSPNode child = childNodes[action];
            double reward = (child != null) ?
                    child.sample(roundCtr, joinOrder, oldJoin, policy):
                    playout(roundCtr, joinOrder, oldJoin);
            // update UCT statistics and return reward
            updateStatistics(action, reward);
            return reward;
        }
    }
    /**
     * Recursively sample from UCT tree and return reward for hybrid algorithm.
     *
     * @param roundCtr  current round (used as timestamp for expansion)
     * @param joinOrder partially completed join order
     * @param policy	policy used to select actions
     * @return achieved reward
     */
    public double sample(long roundCtr, int[] joinOrder, HybridJoin hybridJoin,
                         SelectionPolicy policy) throws Exception {
        // Check if this is a (non-extendible) leaf node
        if (nrActions == 0) {
            // leaf node - evaluate join order and return reward
            return hybridJoin.execute(joinOrder, -1, (int) roundCtr);
        } else {
            // inner node - select next action and expand tree if necessary
            int action = selectAction(policy);
            int table = nextTable[action];
            joinOrder[treeLevel] = table;
            // grow tree if possible
            boolean canExpand = createdIn != roundCtr;
            if (childNodes[action] == null && canExpand) {
                childNodes[action] = new NSPNode(roundCtr, this, table);
            }
            // evaluate via recursive invocation or via playout
            NSPNode child = childNodes[action];
            double reward = (child != null) ?
                    child.sample(roundCtr, joinOrder, hybridJoin, policy):
                    playout(roundCtr, joinOrder, hybridJoin);
            // update UCT statistics and return reward
            updateStatistics(action, reward);
            return reward;
        }
    }
    /**
     * Calculate the memory consumption of this object
     *
     * @return      memory size in Bytes
     */
    public long getSize() {
        long size = nrActions * 12L;
        size += 16 + (unjoinedTables.size() + joinedTables.size() + recommendedActions.size() + nextTable.length) * 4L;
        for (NSPNode node: childNodes) {
            if (node != null) {
                size += node.getSize();
            }
        }
        return size;
    }

    /**
     * Find the optimal join order.
     *
     * @return
     */
    public int[] optimalJoinOrder() {
        int nrJoined = query.nrJoined;
        int[] joinOrder = new int[nrJoined];
        NSPNode node = this;
        int lastTreeLevel = 0;
        Set<Integer> newlyJoined = new HashSet<>(nrJoined);
        int firstAction = -1;
        while (node.nrActions > 0) {
            int action = node.selectAction();
            if (firstAction < 0) {
                firstAction = action;
            }
            int table = node.nextTable[action];
            newlyJoined.add(table);
            lastTreeLevel = node.treeLevel;
            joinOrder[lastTreeLevel] = table;
            if (node.childNodes[action] == null) {
                break;
            }
            node = node.childNodes[action];
        }
        int lastTable = joinOrder[lastTreeLevel];
        if (lastTreeLevel < nrJoined - 1) {
            if (useHeuristic) {
                // Iterate over join order positions to fill
                List<Integer> unjoinedTablesShuffled = new ArrayList<>(node.unjoinedTables);
                for (int posCtr = lastTreeLevel + 1; posCtr < nrTables; ++posCtr) {
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
                Iterator<Integer> unjoinedTablesIter = node.unjoinedTables.iterator();
                // Fill in remaining join order positions
                for (int posCtr = node.treeLevel + 1; posCtr < nrTables; ++posCtr) {
                    int nextTable = unjoinedTablesIter.next();
                    while (nextTable == lastTable) {
                        nextTable = unjoinedTablesIter.next();
                    }
                    joinOrder[posCtr] = nextTable;
                }
            }
        }
        return joinOrder;
    }

    public double getReward(int action) {
        int nrTries = this.nrTries[action];
        return nrTries == 0 ? 0 : accumulatedReward[action] / nrTries;
    }

    public int getTries(int action) {
        return Math.max(1, this.nrTries[action]);
    }
}
