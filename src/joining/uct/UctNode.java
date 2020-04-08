package joining.uct;

import joining.join.MultiWayJoin;
import query.QueryInfo;
import statistics.JoinStats;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import config.JoinConfig;

/**
 * Represents node in UCT search tree.
 *
 * @author immanueltrummer
 */
public class UctNode {
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
    public final UctNode[] childNodes;
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
     * Evaluates a given join order and accumulates results.
     */
    final MultiWayJoin joinOp;
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
     * Set to false only for actions representing sub-queries
     * in exists expressions that are connected via predicates
     * to some of the unselected tables. (the treatment of
     * exists expression in the join algorithm assumes that
     * the corresponding tables are selected only when all
     * connected predicates can be immediately evaluated).
     */
    final boolean[] eligibleExists;
    /**
     * Set flags for all tables indicating whether they
     * are eligible for selection. We assume that this
     * function is only called after all fields except
     * for eligibleExists have been initialized.
     */
    void determineEligibility() {
        // All actions are eligible by default
        Arrays.fill(eligibleExists, true);
        // Check for ineligible actions
        for (int actionCtr=0; actionCtr<nrActions; ++actionCtr) {
            // Does this action select a table representing
            // a sub-query in an exists expression?
            int tableIdx = nextTable[actionCtr];
            String alias = query.aliases[tableIdx];
            if (query.aliasToExistsFlag.containsKey(alias)) {
                // Is this table connected to unselected
                // tables via predicates?
                Set<Integer> otherUnjoined = new HashSet<>();
                otherUnjoined.addAll(unjoinedTables);
                otherUnjoined.remove(tableIdx);
                if (query.connected(otherUnjoined, tableIdx)) {
                    eligibleExists[actionCtr] = false;
                }
            }
        }
    }
    /**
     * Initialize UCT root node.
     *
     * @param roundCtr     	current round number
     * @param query        	the query which is optimized
     * @param useHeuristic 	whether to avoid Cartesian products
     * @param joinOp		multi-way join operator allowing fast join order switching
     */
    public UctNode(long roundCtr, QueryInfo query,
                   boolean useHeuristic, MultiWayJoin joinOp) {
        // Count node generation
        ++JoinStats.nrUctNodes;
        this.query = query;
        this.nrTables = query.nrJoined;
        createdIn = roundCtr;
        treeLevel = 0;
        nrActions = nrTables;
        childNodes = new UctNode[nrActions];
        nrTries = new int[nrActions];
        accumulatedReward = new double[nrActions];
        joinedTables = new HashSet<Integer>();
        unjoinedTables = new ArrayList<>();
        nextTable = new int[nrTables];
        for (int tableCtr = 0; tableCtr < nrTables; ++tableCtr) {
            unjoinedTables.add(tableCtr);
            nextTable[tableCtr] = tableCtr;
        }
        this.joinOp = joinOp;
        this.useHeuristic = useHeuristic;
        recommendedActions = new HashSet<Integer>();
        for (int action = 0; action < nrActions; ++action) {
            accumulatedReward[action] = 0;
            recommendedActions.add(action);
        }
        this.eligibleExists = new boolean[nrActions];
        determineEligibility();
        // All eligible actions become priority actions
        priorityActions = new ArrayList<Integer>();
        for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
            if (eligibleExists[actionCtr]) {
                priorityActions.add(actionCtr);
            }
        }
    }
    /**
     * Initializes UCT node by expanding parent node.
     *
     * @param roundCtr    current round number
     * @param parent      parent node in UCT tree
     * @param joinedTable new joined table
     */
    public UctNode(long roundCtr, UctNode parent, int joinedTable) {
        // Count node generation
        ++JoinStats.nrUctNodes;
        createdIn = roundCtr;
        treeLevel = parent.treeLevel + 1;
        nrActions = parent.nrActions - 1;
        childNodes = new UctNode[nrActions];
        nrTries = new int[nrActions];
        accumulatedReward = new double[nrActions];
        query = parent.query;
        nrTables = parent.nrTables;
        joinedTables = new HashSet<Integer>();
        joinedTables.addAll(parent.joinedTables);
        joinedTables.add(joinedTable);
        unjoinedTables = new ArrayList<Integer>();
        unjoinedTables.addAll(parent.unjoinedTables);
        int indexToRemove = unjoinedTables.indexOf(joinedTable);
        unjoinedTables.remove(indexToRemove);
        nextTable = new int[nrActions];
        for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
            accumulatedReward[actionCtr] = 0;
            nextTable[actionCtr] = unjoinedTables.get(actionCtr);
        }
        this.joinOp = parent.joinOp;
        // Determine eligibility
        this.eligibleExists = new boolean[nrActions];
        determineEligibility();
        // Calculate recommended actions if heuristic is activated
        this.useHeuristic = parent.useHeuristic;
        if (useHeuristic) {
            recommendedActions = new HashSet<Integer>();
            // Iterate over all actions
            for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
                // Get table associated with (join) action
                int table = nextTable[actionCtr];
                // Check if at least one predicate connects current
                // tables to new table.
                if (query.connected(joinedTables, table) &&
                        eligibleExists[actionCtr]) {
                    recommendedActions.add(actionCtr);
                } // over predicates
            } // over actions
            if (recommendedActions.isEmpty()) {
                // add all eligible actions to recommended actions
                for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
                    if (eligibleExists[actionCtr]) {
                        recommendedActions.add(actionCtr);
                    }
                }
            }
        } // if heuristic is used
        else {
            recommendedActions = null;
        }
        // Collect untried actions, restrict to recommended actions
        // if the heuristic is activated.
        priorityActions = new ArrayList<Integer>();
        for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
            if (eligibleExists[actionCtr] && (!useHeuristic ||
                    recommendedActions.contains(actionCtr))) {
                priorityActions.add(actionCtr);
            }
        }
    }
    /**
     * Select most interesting action to try next. Also updates
     * list of unvisited actions.
     *
     * @param policy	policy used to select action
     * @return index of action to try next
     */
    int selectAction(SelectionPolicy policy) {
        // Prioritize tables representing sub-queries
        // in exists/not exists clauses if activated.
        if (JoinConfig.SIMPLE_ANTI_JOIN) {
            int offset = random.nextInt(nrActions);
            for (int actionCtr=0; actionCtr<nrActions; ++actionCtr) {
                int action = (actionCtr + offset) % nrActions;
                int table = nextTable[action];
                if (query.existsFlags[table] != 0 &&
                        eligibleExists[action]) {
                    return action;
                }
            }
        }
        // Are there untried actions?
        if (!priorityActions.isEmpty()) {
            int nrUntried = priorityActions.size();
            int actionIndex = random.nextInt(nrUntried);
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
            int offset = random.nextInt(nrActions);
            int bestAction = -1;
            double bestQuality = -1;
            for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
                // Calculate index of current action
                int action = (offset + actionCtr) % nrActions;
                // Check if action is eligible
                if (!eligibleExists[action]) {
                    continue;
                }
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
                    case MAX_VISIT:
                        quality = nrTries[action];
                        break;
                    case RANDOM:
                        quality = random.nextDouble();
                        break;
                    case RANDOM_UCB1:
                        if (treeLevel==0) {
                            quality = random.nextDouble();
                        } else {
                            quality = meanReward +
                                    JoinConfig.EXPLORATION_WEIGHT *
                                            exploration;
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
                if (random.nextDouble()<=JoinConfig.EPSILON) {
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
     * Repairs given join order by pushing tables representing
     * sub-queries in exists expression backwards if some of
     * their dependencies cannot be immediately evaluated.
     *
     * @param joinOrder		repair this join order
     */
    void repairExistPos(int[] joinOrder) {
        int[] repairedJoinOrder = new int[nrTables];
        Set<Integer> pushedBacks = new HashSet<>();
        Set<Integer> joined = new HashSet<>();
        Set<Integer> unjoined = IntStream.range(0, nrTables).
                boxed().collect(Collectors.toSet());
        int targetCtr = 0;
        for (int srcCtr=0; srcCtr<nrTables; ++srcCtr) {
            // Take care of tables that were previously pushed back
            Iterator<Integer> pushedIter = pushedBacks.iterator();
            while (pushedIter.hasNext()) {
                int nextPushed = pushedIter.next();
                // Can we add table now?
                if (!query.connected(unjoined, nextPushed)) {
                    pushedIter.remove();
                    repairedJoinOrder[targetCtr] = nextPushed;
                    ++targetCtr;
                    joined.add(nextPushed);
                    unjoined.remove(nextPushed);
                }
            }
            // Can we simply add the next table?
            int nextTable = joinOrder[srcCtr];
            if (query.existsFlags[nextTable] == 0 ||
                    !query.connected(unjoined, nextTable)) {
                repairedJoinOrder[targetCtr] = nextTable;
                ++targetCtr;
                joined.add(nextTable);
                unjoined.remove(nextTable);
            } else {
                pushedBacks.add(nextTable);
            }
        }
        // Copy repaired join order
        for (int joinCtr=0; joinCtr<nrTables; ++joinCtr) {
            joinOrder[joinCtr] = repairedJoinOrder[joinCtr];
        }
    }
    /**
     * Randomly complete join order with remaining tables,
     * invoke evaluation, and return obtained reward.
     *
     * @param joinOrder partially completed join order
     * @return obtained reward
     */
    double playout(int[] joinOrder) throws Exception {
        // Last selected table
        int lastTable = joinOrder[treeLevel];
        // Should we avoid Cartesian product joins?
        if (useHeuristic) {
            Set<Integer> newlyJoined = new HashSet<Integer>();
            newlyJoined.addAll(joinedTables);
            newlyJoined.add(lastTable);
            // Iterate over join order positions to fill
            List<Integer> unjoinedTablesShuffled = new ArrayList<Integer>();
            unjoinedTablesShuffled.addAll(unjoinedTables);
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
            repairExistPos(joinOrder);
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
            repairExistPos(joinOrder);
        }
        // Evaluate completed join order and return reward
        return joinOp.execute(joinOrder);
    }
    /**
     * Recursively sample from UCT tree and return reward.
     *
     * @param roundCtr  current round (used as timestamp for expansion)
     * @param joinOrder partially completed join order
     * @param policy	policy used to select actions
     * @return achieved reward
     */
    public double sample(long roundCtr, int[] joinOrder,
                         SelectionPolicy policy) throws Exception {
        // Check if this is a (non-extendible) leaf node
        if (nrActions == 0) {
            // leaf node - evaluate join order and return reward
            return joinOp.execute(joinOrder);
        } else {
            // inner node - select next action and expand tree if necessary
            int action = selectAction(policy);
            int table = nextTable[action];
            joinOrder[treeLevel] = table;
            // grow tree if possible
            boolean canExpand = createdIn != roundCtr;
            if (childNodes[action] == null && canExpand) {
                childNodes[action] = new UctNode(roundCtr, this, table);
            }
            // evaluate via recursive invocation or via playout
            UctNode child = childNodes[action];
            double reward = (child != null) ?
                    child.sample(roundCtr, joinOrder, policy):
                    playout(joinOrder);
            // update UCT statistics and return reward
            updateStatistics(action, reward);
            return reward;
        }
    }
}