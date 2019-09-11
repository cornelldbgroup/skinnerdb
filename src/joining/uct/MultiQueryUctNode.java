//package joining.uct;
//
//import config.JoinConfig;
//import joining.join.MultiWayJoin;
//import query.QueryInfo;
//import statistics.JoinStats;
//
//import javax.management.Query;
//import java.util.ArrayList;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Random;
//
//public class MultiQueryUctNode {
//
//    /**
//     * Used for randomized selection policy.
//     */
//    final Random random = new Random();
//
//    /**
//     * The query for which we are optimizing.
//     */
//    final List<QueryInfo> queries;
//
//    /**
//     * Iteration in which node was created.
//     */
//    final long createdIn;
//
//    /**
//     * Level of node in tree (root node has level 0).
//     * At the same time the join order index into
//     * which table selected in this node is inserted.
//     */
//    public final int treeLevel;
//
//    /**
//     * Number of possible actions from this state.
//     */
//    public final int nrActions;
//
//    /**
//     * Assigns each action index to child node.
//     */
//    public final UctNode[] childNodes;
//
//    /**
//     * Number of times this node was visited.
//     */
//    int nrVisits = 0;
//
//    /**
//     * Number of times each action was tried out.
//     */
//    public final int[] nrTries;
//
//    /**
//     * Reward accumulated for specific actions.
//     */
//    public final double[] accumulatedReward;
//
//    /**
//     *
//     */
//    public int nrQueries;
//
//    /**
//     * Indicates whether the search space is restricted to
//     * join orders that avoid Cartesian products. This
//     * flag should only be activated if it is ensured
//     * that a given query can be evaluated under that
//     * constraint.
//     */
//    final boolean useHeuristic;
//
//
//    /**
//     * Initialize UCT root node.
//     *
//     * @param roundCtr     	current round number
//     * @param query        	the query which is optimized
//     * @param useHeuristic 	whether to avoid Cartesian products
//     * @param joinOp		multi-way join operator allowing fast join order switching
//     */
//    public MultiQueryUctNode(long roundCtr, List<QueryInfo> queries,
//                   boolean useHeuristic, MultiWayJoin joinOp) {
//        // Count node generation
//        ++JoinStats.nrUctNodes;
//        this.queries = queries;
//        createdIn = roundCtr;
//        treeLevel = 0;
//        //nrActions = nrTables;
//        nrQueries = queries.size();
//        priorityActions = new ArrayList<Integer>();
//        for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
//            priorityActions.add(actionCtr);
//        }
//        childNodes = new UctNode[nrActions];
//        nrTries = new int[nrActions];
//        accumulatedReward = new double[nrActions];
//        joinedTables = new HashSet<Integer>();
//        unjoinedTables = new ArrayList<>();
//        nextTable = new int[nrTables];
//        for (int tableCtr = 0; tableCtr < nrTables; ++tableCtr) {
//            unjoinedTables.add(tableCtr);
//            nextTable[tableCtr] = tableCtr;
//        }
//        this.joinOp = joinOp;
//        this.useHeuristic = useHeuristic;
//        recommendedActions = new HashSet<Integer>();
//        for (int action = 0; action < nrActions; ++action) {
//            accumulatedReward[action] = 0;
//            recommendedActions.add(action);
//        }
//    }
//    /**
//     * Initializes UCT node by expanding parent node.
//     *
//     * @param roundCtr    current round number
//     * @param parent      parent node in UCT tree
//     * @param joinedTable new joined table
//     */
//    public UctNode(long roundCtr, UctNode parent, int joinedTable, List<QueryInfo> queries) {
//        // Count node generation
//        ++JoinStats.nrUctNodes;
//        createdIn = roundCtr;
//        treeLevel = parent.treeLevel + 1;
//        nrActions = parent.nrActions - 1;
//        childNodes = new UctNode[nrActions];
//        nrTries = new int[nrActions];
//        accumulatedReward = new double[nrActions];
//        queries = parent.queries;
//        nrTables = parent.nrTables;
//        joinedTables = new HashSet<Integer>();
//        joinedTables.addAll(parent.joinedTables);
//        joinedTables.add(joinedTable);
//        unjoinedTables = new ArrayList<Integer>();
//        unjoinedTables.addAll(parent.unjoinedTables);
//        int indexToRemove = unjoinedTables.indexOf(joinedTable);
//        unjoinedTables.remove(indexToRemove);
//        nextTable = new int[nrActions];
//        for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
//            accumulatedReward[actionCtr] = 0;
//            nextTable[actionCtr] = unjoinedTables.get(actionCtr);
//        }
//        this.joinOp = parent.joinOp;
//        // Calculate recommended actions if heuristic is activated
//        this.useHeuristic = parent.useHeuristic;
//        if (useHeuristic) {
//            recommendedActions = new HashSet<Integer>();
//            // Iterate over all actions
//            for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
//                // Get table associated with (join) action
//                int table = nextTable[actionCtr];
//                // Check if at least one predicate connects current
//                // tables to new table.
//                if (query.connected(joinedTables, table)) {
//                    recommendedActions.add(actionCtr);
//                } // over predicates
//            } // over actions
//            if (recommendedActions.isEmpty()) {
//                // add all actions to recommended actions
//                for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
//                    recommendedActions.add(actionCtr);
//                }
//            }
//        } // if heuristic is used
//        else {
//            recommendedActions = null;
//        }
//        // Collect untried actions, restrict to recommended actions
//        // if the heuristic is activated.
//        priorityActions = new ArrayList<Integer>();
//        for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
//            if (!useHeuristic || recommendedActions.contains(actionCtr)) {
//                priorityActions.add(actionCtr);
//            }
//        }
//    }
//
//    /**
//     * Select most interesting action to try next. Also updates
//     * list of unvisited actions.
//     *
//     * @param policy	policy used to select action
//     * @return index of action to try next
//     */
//    int[] selectAction(SelectionPolicy policy) {
//
//        for (int i = 0; i < nrQueries; i++) {
//
//        }
//
//
//
//
//        /* When using the default selection policy (UCB1):
//         * We apply the UCT formula as no actions are untried.
//         * We iterate over all actions and calculate their
//         * UCT value, updating best action and best UCT value
//         * on the way. We start iterations with a randomly
//         * selected action to ensure that we pick a random
//         * action among the ones with maximal UCT value.
//         */
//        int offset = random.nextInt(nrActions);
//        int bestAction = -1;
//        double bestQuality = -1;
//        for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
//            // Calculate index of current action
//            int action = (offset + actionCtr) % nrActions;
//            // if heuristic is used, choose only from recommended actions
//            if (useHeuristic && recommendedActions.size() == 0) {
//                throw new RuntimeException("there are no recommended exception and we are trying to use heuristic");
//            }
//            if (useHeuristic && !recommendedActions.contains(action))
//                continue;
//            double meanReward = accumulatedReward[action] / nrTries[action];
//            double exploration = Math.sqrt(Math.log(nrVisits) / nrTries[action]);
//            // Assess the quality of the action according to policy
//            double quality = -1;
//            switch (policy) {
//                case UCB1:
//                    quality = meanReward +
//                            JoinConfig.EXPLORATION_WEIGHT * exploration;
//                    break;
//                case MAX_REWARD:
//                case EPSILON_GREEDY:
//                    quality = meanReward;
//                    break;
//                case RANDOM:
//                    quality = random.nextDouble();
//                    break;
//                case RANDOM_UCB1:
//                    if (treeLevel==0) {
//                        quality = random.nextDouble();
//                    } else {
//                        quality = meanReward +
//                                JoinConfig.EXPLORATION_WEIGHT * exploration;
//                    }
//                    break;
//            }
//            //double UB = meanReward + 1E-6 * exploration;
//            //double UB = meanReward + 1E-4 * exploration;
//            //double UB = meanReward + 1E-1 * exploration;
//            if (quality > bestQuality) {
//                bestAction = action;
//                bestQuality = quality;
//            }
//        }
//        // For epsilon greedy, return random action with
//        // probability epsilon.
//        if (policy.equals(SelectionPolicy.EPSILON_GREEDY)) {
//            if (random.nextDouble()<=JoinConfig.EPSILON) {
//                return random.nextInt(nrActions);
//            }
//        }
//        // Otherwise: return best action.
//        return bestAction;
//    }
//
//
//    /**
//     * Recursively sample from UCT tree and return reward.
//     *
//     * @param roundCtr  current round (used as timestamp for expansion)
//     * @param joinOrder partially completed join order
//     * @param policy	policy used to select actions
//     * @return achieved reward
//     */
//    public double sample(long roundCtr, int[] joinOrder,
//                         SelectionPolicy policy) throws Exception {
//        // Check if this is a (non-extendible) leaf node
//        if (nrActions == 0) {
//            // leaf node - evaluate join order and return reward
//            return joinOp.execute(joinOrder);
//        } else {
//            // inner node - select next action and expand tree if necessary
//            int action = selectAction(policy);
//            int table = nextTable[action];
//            joinOrder[treeLevel] = table;
//            // grow tree if possible
//            boolean canExpand = createdIn != roundCtr;
//            if (childNodes[action] == null && canExpand) {
//                childNodes[action] = new UctNode(roundCtr, this, table);
//            }
//            // evaluate via recursive invocation or via playout
//            UctNode child = childNodes[action];
//            double reward = (child != null) ?
//                    child.sample(roundCtr, joinOrder, policy):
//                    playout(joinOrder);
//            // update UCT statistics and return reward
//            updateStatistics(action, reward);
//            return reward;
//        }
//    }
//
//}
