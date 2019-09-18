package joining.uct;

import config.JoinConfig;
import config.UCTConfig;
import joining.join.DPJoin;
import joining.join.MultiWayJoin;
import query.QueryInfo;
import statistics.JoinStats;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Represents node in data parallel UCT search tree.
 *
 * @author immanueltrummer
 */
public class DPNode {
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
     * Assigns each action index to child node.
     */
    public final DPNode[] childNodes;
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
     * Total number of tables to join.
     */
    final int nrTables;
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
     * Numbre of threads.
     */
    final int nrThreads;
    /**
     * UCT extension of split tables
     */
    BaseUctInner tableRoot = null;
    /**
     * Initialize UCT root node.
     *
     * @param roundCtr     	current round number
     * @param query        	the query which is optimized
     * @param useHeuristic 	whether to avoid Cartesian products
     */
    public DPNode(long roundCtr, QueryInfo query,
                   boolean useHeuristic, int nrThreads) {
        // Count node generation
//        ++JoinStats.nrUctNodes;
        this.query = query;
        this.nrTables = query.nrJoined;
        this.nrThreads = nrThreads;
        createdIn = roundCtr;
        treeLevel = 0;
        nrActions = nrTables;
        childNodes = new DPNode[nrActions];
        nrVisits = new int[nrThreads];
        nrTries = new int[nrThreads][nrActions];
        accumulatedReward = new double[nrThreads][nrActions];
        unjoinedTables = new ArrayList<>();
        nextTable = new int[nrTables];
        for (int tableCtr = 0; tableCtr < nrTables; ++tableCtr) {
            unjoinedTables.add(tableCtr);
            nextTable[tableCtr] = tableCtr;
        }
        this.useHeuristic = useHeuristic;
        recommendedActions = new HashSet<>();
        for (int action = 0; action < nrActions; ++action) {
//            accumulatedReward[action] = 0;
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
    public DPNode(long roundCtr, DPNode parent, int joinedTable) {
        // Count node generation
//        ++JoinStats.nrUctNodes;
        createdIn = roundCtr;
        treeLevel = parent.treeLevel + 1;
        nrActions = parent.nrActions - 1;
        nrThreads = parent.nrThreads;
        childNodes = new DPNode[nrActions];
        nrVisits = new int[nrThreads];
        nrTries = new int[nrThreads][nrActions];
        accumulatedReward = new double[nrThreads][nrActions];
        query = parent.query;
        nrTables = parent.nrTables;
        unjoinedTables = new ArrayList<>();
        Set<Integer> joinedTables = new HashSet<>();
        for (int table = 0; table < nrTables; table++) {
            if (parent.unjoinedTables.contains(table) && table != joinedTable) {
                unjoinedTables.add(table);
            }
            else {
                joinedTables.add(table);
            }
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
                    recommendedActions.add(actionCtr);
                }
            }
        } // if heuristic is used
        else {
            recommendedActions = null;
        }
    }


    /**
     * Select most interesting action to try next. Also updates
     * list of unvisited actions.
     *
     * @param policy	policy used to select action
     * @return index of action to try next
     */
    int selectAction(SelectionPolicy policy, int tid) {
        /*
         * We apply the UCT formula as no actions are untried.
         * We iterate over all actions and calculate their
         * UCT value, updating best action and best UCT value
         * on the way. We start iterations with a randomly
         * selected action to ensure that we pick a random
         * action among the ones with maximal UCT value.
         */
        int nrVisits = 0;
        int[] nrTries = new int[nrActions];
        double[] accumulatedReward = new double[nrActions];
        for (int i = 0; i < nrThreads; i++) {
            nrVisits += this.nrVisits[i];
            for(Integer recAction : recommendedActions) {
                int threadTries = this.nrTries[i][recAction];
                nrTries[recAction] += threadTries;
                accumulatedReward[recAction] += this.accumulatedReward[i][recAction];
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
        int offset = random.nextInt(nrActions);
        for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
            // Calculate index of current action
            int action = (offset + actionCtr) % nrActions;
            if (useHeuristic && !recommendedActions.contains(action))
                continue;
            if (this.nrTries[tid][action] == 0) {
                return action;
            }
            int nrTry = nrTries[action];
            double meanReward = accumulatedReward[action] / nrTry;
            double exploration = Math.sqrt(Math.log(nrVisits) / nrTry);
            // Assess the quality of the action according to policy
            double quality = -1;
            switch (policy) {
                case UCB1:
                    quality = meanReward +
                            JoinConfig.PARA_EXPLORATION_WEIGHT * exploration;
                    break;
                case MAX_REWARD:
                case EPSILON_GREEDY:
                    quality = meanReward;
                    break;
                case RANDOM:
                    quality = random.nextDouble();
                    break;
                case RANDOM_UCB1:
                    if (treeLevel==0) {
                        quality = random.nextDouble();
                    } else {
                        quality = meanReward +
                                JoinConfig.PARA_EXPLORATION_WEIGHT * exploration;
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
        if (bestAction < 0) {
            System.out.println(Arrays.toString(nrTries));
            System.out.println(Arrays.toString(accumulatedReward));
            System.out.println(Arrays.toString(recommendedActions.toArray()));
            System.out.println(Arrays.toString(this.nrVisits));
            System.out.println(nrVisits);
            throw new RuntimeException("not found a join order");
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
    double playout(long roundCtr, int[] joinOrder, DPJoin joinOp) throws Exception {
        // Last selected table
        int lastTable = joinOrder[treeLevel];
        // Should we avoid Cartesian product joins?
        if (useHeuristic) {
            Set<Integer> newlyJoined = new HashSet<>();
            for (int posCtr = 0; posCtr <= treeLevel; posCtr++) {
                newlyJoined.add(joinOrder[posCtr]);
            }
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
        int splitTable = getSplitTableByCard(joinOrder, joinOp.cardinalities);
        // Evaluate completed join order and return reward
        return joinOp.execute(joinOrder, splitTable, (int) roundCtr);
    }
    /**
     * Recursively sample from UCT tree and return reward.
     *
     * @param roundCtr  current round (used as timestamp for expansion)
     * @param joinOrder partially completed join order
     * @param policy	policy used to select actions
     * @return achieved reward
     */
    public double sample(long roundCtr, int[] joinOrder, DPJoin joinOp,
                         SelectionPolicy policy) throws Exception {
        int tid = joinOp.tid;
        // Check if this is a (non-extendible) leaf node
        if (nrActions == 0) {
            int end = Math.min(UCTConfig.SPLIT_LEN, joinOrder.length);
            // Initialize table nodes
            if (tableRoot == null) {
                tableRoot = new BaseUctInner(null, -1);
                for (int i = 1; i < end; i++) {
                    int table = joinOrder[i];
                    int cardinality = joinOp.cardinalities[table];
                    if (cardinality > UCTConfig.SPLIT_SIZE) {
                        tableRoot.expand(table, true);
                    }
                }
            }
            BaseUctNode splitNode = tableRoot.getMaxOrderedUCTChildOrder(joinOrder, end);
            if (splitNode != null) {
                double reward = joinOp.execute(joinOrder, splitNode.label, (int) roundCtr);
                splitNode.updataStatistics(reward);
                splitNode.parent.updataStatistics(reward);
                return reward;
            }
            else {
                int splitTable = getSplitTableByCard(joinOrder, joinOp.cardinalities);
                return joinOp.execute(joinOrder, splitTable, (int) roundCtr);
            }
        } else {
            // inner node - select next action and expand tree if necessary
            int action = selectAction(policy, tid);
            int table = nextTable[action];
            joinOrder[treeLevel] = table;
            // grow tree if possible
            boolean canExpand = createdIn != roundCtr;
            if (childNodes[action] == null && canExpand) {
                childNodes[action] = new DPNode(roundCtr, this, table);
            }
            // evaluate via recursive invocation or via playout
            DPNode child = childNodes[action];
            double reward = (child != null) ?
                    child.sample(roundCtr, joinOrder, joinOp, policy):
                    playout(roundCtr, joinOrder, joinOp);
            // update UCT statistics and return reward
            updateStatistics(action, reward, tid);
            return reward;
        }
    }

    int getSplitTableByCard(int[] joinOrder, int[] cardinalities) {
        int splitTable = joinOrder[0];
        int end = Math.min(UCTConfig.SPLIT_LEN, joinOrder.length);
        for (int i = 1; i < end; i++) {
            int table = joinOrder[i];
            int cardinality = cardinalities[table];
            if (cardinality > UCTConfig.SPLIT_SIZE) {
                splitTable = table;
                break;
            }
        }
        return splitTable;
    }
}
