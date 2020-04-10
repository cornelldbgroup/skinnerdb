package joining.uct;

import joining.join.MultiWayJoin;
import query.QueryInfo;
import statistics.JoinStats;

import java.util.*;

/**
 * base class to represent the tree node
 *
 * @author Junxiong Wang
 */
public abstract class TreeNode {
    /**
     * Used for randomized selection policy.
     */
    final Random random = new Random();
    /**
     * The query for which we are optimizing.
     */
    final QueryInfo query;
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
        for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
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
     * @param query        the query which is optimized
     * @param useHeuristic whether to avoid Cartesian products
     * @param joinOp       multi-way join operator allowing fast join order switching
     */
    public TreeNode(QueryInfo query,
                    boolean useHeuristic, MultiWayJoin joinOp) {
        // Count node generation
        ++JoinStats.nrUctNodes;
        this.query = query;
        this.nrTables = query.nrJoined;
        treeLevel = 0;
        nrActions = nrTables;
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
     * @param parent      parent node in UCT tree
     * @param joinedTable new joined table
     */
    public TreeNode(TreeNode parent, int joinedTable) {
        // Count node generation
        ++JoinStats.nrUctNodes;
        treeLevel = parent.treeLevel + 1;
        nrActions = parent.nrActions - 1;
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
     * Updates monte carlo tree statistics after sampling.
     *
     * @param selectedAction action taken
     * @param reward         reward achieved
     */
    void updateStatistics(int selectedAction, double reward) {
        ++nrVisits;
        ++nrTries[selectedAction];
        accumulatedReward[selectedAction] += reward;
    }


}
