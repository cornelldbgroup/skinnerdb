package joining.uct;

import joining.join.MultiWayJoin;
import joining.plan.JoinOrder;
import org.apache.commons.lang3.mutable.MutableBoolean;
import query.QueryInfo;
import statistics.JoinStats;

import java.lang.reflect.Array;
import java.sql.SQLOutput;
import java.util.*;

import config.JoinConfig;

/**
 * Represents node in brue search tree.
 *
 * @author Junxiong Wang
 */
public class BrueNode2 {
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
    public final BrueNode2[] childNodes;
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

    static HashMap<JoinOrder, BrueNode2> nodeMap = new HashMap<>();

    /**
     * Initialize UCT root node.
     *
     * @param roundCtr     current round number
     * @param query        the query which is optimized
     * @param useHeuristic whether to avoid Cartesian products
     * @param joinOp       multi-way join operator allowing fast join order switching
     */
    public BrueNode2(long roundCtr, QueryInfo query,
                     boolean useHeuristic, MultiWayJoin joinOp) {
        // Count node generation
        ++JoinStats.nrUctNodes;
        this.query = query;
        this.nrTables = query.nrJoined;
        createdIn = roundCtr;
        treeLevel = 0;
        nrActions = nrTables;
        priorityActions = new ArrayList<Integer>();
        for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
            priorityActions.add(actionCtr);
        }
        childNodes = new BrueNode2[nrActions];
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
    }

    /**
     * Initializes UCT node by expanding parent node.
     *
     * @param roundCtr    current round number
     * @param parent      parent node in UCT tree
     * @param joinedTable new joined table
     */
    public BrueNode2(long roundCtr, BrueNode2 parent, int joinedTable) {
        // Count node generation
        ++JoinStats.nrUctNodes;
        createdIn = roundCtr;
        treeLevel = parent.treeLevel + 1;
        nrActions = parent.nrActions - 1;
        childNodes = new BrueNode2[nrActions];
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
        priorityActions = new ArrayList<Integer>();
        for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
            if (!useHeuristic || recommendedActions.contains(actionCtr)) {
                priorityActions.add(actionCtr);
            }
        }
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
     * Recursively sample from UCT tree and return reward.
     *
     * @param roundCtr  current round (used as timestamp for expansion)
     * @param joinOrder partially completed join order
     * @return achieved reward
     */
    public double sample(long roundCtr, int[] joinOrder, int depth,
                         int selectSwitch, boolean expand, MutableBoolean restart) throws Exception {
        //System.out.println("roundCtr:" + roundCtr);
        //System.out.println("selectSwitchFun:" + selectSwitchFun);
        //System.out.println("order " + Arrays.toString(joinOrder));
        if (depth == nrTables) {
//            System.out.println("order " + Arrays.toString(joinOrder));
            return joinOp.execute(joinOrder);
        }
        //pick up action for the next step
        int action = 0;
        if (depth < selectSwitch) {
            //explore the current best action
            //System.out.println("explore new order=====");
            action = explorationPolicy();
            //System.out.println("explore:" + action);
        } else {
            //select the new action
            //System.out.println("exploit best order=====");
            action = estimationPolicy();
            //System.out.println("random:" + action);
        }

        int table = nextTable[action];
        joinOrder[treeLevel] = table;
        //System.out.println("table:" + table);
        double reward = 0;
        if (childNodes[action] != null) {
            //go to the lower level
            reward = childNodes[action].sample(roundCtr, joinOrder, depth + 1, selectSwitch, false, restart);
        } else {
            //go the the lower level
            JoinOrder currentOrder = new JoinOrder(Arrays.copyOfRange(joinOrder, 0, treeLevel + 1));
            BrueNode2 nextNode;
            if (nodeMap.containsKey(currentOrder)) {
                nextNode = nodeMap.get(currentOrder);
            } else {
                nextNode = new BrueNode2(roundCtr, this, table);
                nodeMap.put(currentOrder, nextNode);
            }
            if (expand) {
                //Expand the BRUE tree
                childNodes[action] = nextNode;
                if (depth != selectSwitch) {
                    restart.setTrue();
                }
                //only expand one time.
                expand = false;
            }
            reward = nextNode.sample(roundCtr, joinOrder, depth + 1, selectSwitch, expand, restart);
        }
        if (depth == selectSwitch) {
            //System.out.println("update reward");
            updateStatistics(action, reward);
        }
        return reward;
    }

    private int estimationPolicy() {
        int offset = random.nextInt(nrActions);
        int bestAction = -1;
        double bestQuality = -1;
        for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
            // Calculate index of current action
            int action = (offset + actionCtr) % nrActions;
            if (useHeuristic && !recommendedActions.contains(action))
                continue;
            double meanReward = (nrTries[action] > 0) ? accumulatedReward[action] / nrTries[action] : 0;
            //System.out.println("action:" + action);
            //System.out.println("meanReward:" + meanReward);
            if (meanReward > bestQuality) {
                bestAction = action;
                bestQuality = meanReward;
            }
        }
        return bestAction;
    }

    private int explorationPolicy() {
        int offset = random.nextInt(nrActions);
        for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
            int action = (offset + actionCtr) % nrActions;
            if (useHeuristic && !recommendedActions.contains(action))
                continue;
            return action;
        }
        return offset;
    }

//    public boolean getOptimalPolicy(int[] joinOrder, int roundCtr) {
////        for (int i = 0; i < nrActions; i++) {
////            System.out.println("reward:" + accumulatedReward[i]);
////        }
//
//        if (treeLevel < nrTables) {
//            int action = estimationPolicy();
//            int table = nextTable[action];
//            joinOrder[treeLevel] = table;
//            if (childNodes[action] != null)
//                return childNodes[action].getOptimalPolicy(joinOrder, roundCtr);
//            else {
//                childNodes[action] = new BrueNode2(roundCtr, this, table);
//                BrueNode2 child = childNodes[action];
//                child.getOptimalPolicy(joinOrder, roundCtr);
//                return false;
//            }
//        }
//
//        //System.out.println(Arrays.toString(joinOrder));
//        return true;
//    }

    public void executePhaseWithBudget(int[] joinOrder) throws Exception {
        joinOp.execute(joinOrder);
    }

    public void clearNodeMap() {
        nodeMap.clear();
    }
}
