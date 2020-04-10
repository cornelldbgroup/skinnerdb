package joining.uct;

import joining.join.MultiWayJoin;
import query.QueryInfo;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import config.JoinConfig;

/**
 * Represents node in UCT search tree.
 *
 * @author immanueltrummer
 */
public class UctNode extends TreeNode {
    /**
     * Iteration in which node was created.
     */
    final long createdIn;
    /**
     * Assigns each action index to child node.
     */
    public UctNode[] childNodes;
    /**
     * Initialize UCT root node.
     *
     * @param roundCtr     current round number
     * @param query        the query which is optimized
     * @param useHeuristic whether to avoid Cartesian products
     * @param joinOp       multi-way join operator allowing fast join order switching
     */
    public UctNode(long roundCtr, QueryInfo query,
                   boolean useHeuristic, MultiWayJoin joinOp) {
        // Count node generation
        super(query, useHeuristic, joinOp);
        this.createdIn = roundCtr;
        this.childNodes = new UctNode[nrActions];
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
        super(parent, joinedTable);
        this.createdIn = roundCtr;
        childNodes = new UctNode[nrActions];
    }

    /**
     * Select most interesting action to try next. Also updates
     * list of unvisited actions.
     *
     * @param policy policy used to select action
     * @return index of action to try next
     */
    int selectAction(SelectionPolicy policy) {
        // Prioritize tables representing sub-queries
        // in exists/not exists clauses if activated.
        if (JoinConfig.SIMPLE_ANTI_JOIN) {
            int offset = random.nextInt(nrActions);
            for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
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
                        if (treeLevel == 0) {
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
                if (random.nextDouble() <= JoinConfig.EPSILON) {
                    return random.nextInt(nrActions);
                }
            }
            // Otherwise: return best action.
            return bestAction;
        } // if there are unvisited actions
    }

    /**
     * Repairs given join order by pushing tables representing
     * sub-queries in exists expression backwards if some of
     * their dependencies cannot be immediately evaluated.
     *
     * @param joinOrder repair this join order
     */
    void repairExistPos(int[] joinOrder) {
        int[] repairedJoinOrder = new int[nrTables];
        Set<Integer> pushedBacks = new HashSet<>();
        Set<Integer> joined = new HashSet<>();
        Set<Integer> unjoined = IntStream.range(0, nrTables).
                boxed().collect(Collectors.toSet());
        int targetCtr = 0;
        for (int srcCtr = 0; srcCtr < nrTables; ++srcCtr) {
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
        for (int joinCtr = 0; joinCtr < nrTables; ++joinCtr) {
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
     * @param policy    policy used to select actions
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
                    child.sample(roundCtr, joinOrder, policy) :
                    playout(joinOrder);
            // update UCT statistics and return reward
            updateStatistics(action, reward);
            return reward;
        }
    }
}