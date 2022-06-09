package joining.parallel.uct;

import config.JoinConfig;
import joining.parallel.join.OldJoin;
import joining.uct.SelectionPolicy;
import query.QueryInfo;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

/**
 * Represents node in search parallel UCT search tree.
 *
 * @author Anonymous
 */
public class NASPNode {
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
     * Actions that have not been tried yet - if the
     * heuristic is used, this only contains actions
     * that have not been tried and are recommended.
     */
    final List<List<Integer>> threadPriorityActions;
    /**
     * Assigns each action index to child node.
     */
    public final NASPNode[] childNodes;
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
     * node statistics that should be aligned to a cache line
     */
    public volatile NodeStatistics[] nodeStatistics;
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
     * Contains actions that are consistent with the "avoid
     * Cartesian products" heuristic. UCT algorithm will
     * restrict focus on such actions if heuristic flag
     * is activated.
     */
    final List<Set<Integer>> threadRecommendedActions;
    /**
     * Thread identification.
     */
    final int tid;
    /**
     * Initialize UCT root node.
     *
     * @param roundCtr     	current round number
     * @param query        	the query which is optimized
     * @param useHeuristic 	whether to avoid Cartesian products
     */
    public NASPNode(long roundCtr, QueryInfo query,
                    boolean useHeuristic,
                    int nrThreads, List<Integer> unusedThreads) {
        // Count node generation
        this.query = query;
        this.nrTables = query.nrJoined;
        this.tid = -1;
        createdIn = roundCtr;
        treeLevel = 0;
        nrActions = nrTables;
        priorityActions = null;
        threadPriorityActions = new ArrayList<>();
        threadRecommendedActions = new ArrayList<>();
        this.nodeStatistics = new NodeStatistics[nrThreads];
        for (int threadCtr = 0; threadCtr < nrThreads; threadCtr++) {
            threadPriorityActions.add(new ArrayList<>(nrActions));
            threadRecommendedActions.add(new HashSet<>());
            this.nodeStatistics[threadCtr] = new NodeStatistics(nrActions);
        }
        childNodes = new NASPNode[nrActions];
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
        recommendedActions = null;
        // Available actions
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
            for (int threadCtr = 0; threadCtr < nrThreads; threadCtr++) {
                int baseThreads = nrFilteredActions / nrThreads;
                int remainingActions = nrFilteredActions - baseThreads * nrThreads;
                int startAction = threadCtr * baseThreads + Math.min(remainingActions, threadCtr);
                int endAction = startAction + baseThreads + (threadCtr < remainingActions ? 1 : 0);
                for (int filteredActionCtr = startAction; filteredActionCtr < endAction; ++filteredActionCtr) {
                    int actionCtr = filteredActions.get(filteredActionCtr);
                    threadPriorityActions.get(threadCtr).add(actionCtr);
                    threadRecommendedActions.get(threadCtr).add(actionCtr);
                }
            }
        }
        else {
            int baseActions = nrThreads / nrFilteredActions;
            int remainingThreads = nrThreads - baseActions * nrFilteredActions;
            for (int filteredActionCtr = 0; filteredActionCtr < filteredActions.size(); ++filteredActionCtr) {
                int actionCtr = filteredActions.get(filteredActionCtr);
                int table = nextTable[actionCtr];
                int startThread = filteredActionCtr * baseActions + Math.min(remainingThreads, filteredActionCtr);
                int endThread = startThread + baseActions + (filteredActionCtr < remainingThreads ? 1 : 0);
                for (int threadCtr = startThread; threadCtr < endThread; threadCtr++) {
                    threadPriorityActions.get(threadCtr).add(actionCtr);
                    threadRecommendedActions.get(threadCtr).add(actionCtr);
                }
                if (childNodes[actionCtr] == null) {
                    childNodes[actionCtr] = new NASPNode(roundCtr, this, table,
                            startThread, endThread, unusedThreads);
                }
            }
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
    public NASPNode(long roundCtr, NASPNode parent, int joinedTable,
                    int startThread, int endThread, List<Integer> unusedThreads) {
        // Count node generation
        createdIn = roundCtr;
        tid = parent.tid;
        treeLevel = parent.treeLevel + 1;
        nrActions = parent.nrActions - 1;
        childNodes = new NASPNode[nrActions];
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
        if (nrActions == 0) {
            for (int threadCtr = startThread + 1; threadCtr < endThread; threadCtr++) {
                unusedThreads.add(threadCtr);
            }
            endThread = startThread + 1;
        }
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
        int nrThreads = endThread - startThread;
        recommendedActions = null;
        priorityActions = null;
        threadPriorityActions = new ArrayList<>();
        threadRecommendedActions = new ArrayList<>();
        this.nodeStatistics = new NodeStatistics[endThread];
        for (int threadCtr = 0; threadCtr < endThread; threadCtr++) {
            if (threadCtr < startThread) {
                threadPriorityActions.add(null);
                threadRecommendedActions.add(null);
            }
            else {
                threadPriorityActions.add(new ArrayList<>());
                threadRecommendedActions.add(new HashSet<>());
                this.nodeStatistics[threadCtr] = new NodeStatistics(nrActions);
            }
        }
        int nrFilteredActions = filteredActions.size();
        if (nrThreads == 1) {
            for (int action : filteredActions) {
                threadPriorityActions.get(startThread).add(action);
                threadRecommendedActions.get(startThread).add(action);
            }
        }
        else if (nrThreads <= nrFilteredActions) {
            int baseThreads = nrFilteredActions / nrThreads;
            int remainingActions = nrFilteredActions - baseThreads * nrThreads;
            for (int threadCtr = startThread; threadCtr < endThread; threadCtr++) {
                int shiftTid = threadCtr - startThread;
                int startAction = shiftTid * baseThreads + Math.min(remainingActions, shiftTid);
                int endAction = startAction + baseThreads + (shiftTid < remainingActions ? 1 : 0);
                for (int filteredActionCtr = startAction; filteredActionCtr < endAction; ++filteredActionCtr) {
                    int actionCtr = filteredActions.get(filteredActionCtr);
                    threadPriorityActions.get(threadCtr).add(actionCtr);
                    threadRecommendedActions.get(threadCtr).add(actionCtr);
                }
            }
        }
        else if (nrFilteredActions > 0) {
            int baseActions = nrThreads / nrFilteredActions;
            int remainingThreads = nrThreads - baseActions * nrFilteredActions;
            for (int filteredActionCtr = 0; filteredActionCtr < filteredActions.size(); ++filteredActionCtr) {
                int actionCtr = filteredActions.get(filteredActionCtr);
                int table = nextTable[actionCtr];
                int actionStartThread = startThread +
                        filteredActionCtr * baseActions + Math.min(remainingThreads, filteredActionCtr);
                int actionEndThread = actionStartThread + baseActions + (filteredActionCtr < remainingThreads ? 1 : 0);
                for (int threadCtr = actionStartThread; threadCtr < actionEndThread; threadCtr++) {
                    threadPriorityActions.get(threadCtr).add(actionCtr);
                    threadRecommendedActions.get(threadCtr).add(actionCtr);
                }
                if (childNodes[actionCtr] == null) {
                    childNodes[actionCtr] = new NASPNode(roundCtr, this,
                            table, actionStartThread, actionEndThread, unusedThreads);
                }
            }
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
    public NASPNode(long roundCtr, NASPNode parent, int joinedTable,
                    int startThread, int endThread) {
        // Count node generation
        createdIn = roundCtr;
        tid = parent.tid;
        treeLevel = parent.treeLevel + 1;
        nrActions = parent.nrActions - 1;
        childNodes = new NASPNode[nrActions];
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
        int nrThreads = endThread - startThread;
        recommendedActions = null;
        priorityActions = null;
        threadPriorityActions = new ArrayList<>();
        threadRecommendedActions = new ArrayList<>();
        this.nodeStatistics = new NodeStatistics[endThread];
        for (int threadCtr = 0; threadCtr < endThread; threadCtr++) {
            if (threadCtr < startThread) {
                threadPriorityActions.add(null);
                threadRecommendedActions.add(null);
            }
            else {
                threadPriorityActions.add(new ArrayList<>());
                threadRecommendedActions.add(new HashSet<>());
                this.nodeStatistics[threadCtr] = new NodeStatistics(nrActions);
            }
        }
        int nrFilteredActions = filteredActions.size();
        if (nrThreads == 1) {
            for (int action : filteredActions) {
                threadPriorityActions.get(startThread).add(action);
                threadRecommendedActions.get(startThread).add(action);
            }
        }
        else if (nrThreads <= nrFilteredActions) {
            int baseThreads = nrFilteredActions / nrThreads;
            int remainingActions = nrFilteredActions - baseThreads * nrThreads;
            for (int threadCtr = startThread; threadCtr < endThread; threadCtr++) {
                int shiftTid = threadCtr - startThread;
                int startAction = shiftTid * baseThreads + Math.min(remainingActions, shiftTid);
                int endAction = startAction + baseThreads + (shiftTid < remainingActions ? 1 : 0);
                for (int filteredActionCtr = startAction; filteredActionCtr < endAction; ++filteredActionCtr) {
                    int actionCtr = filteredActions.get(filteredActionCtr);
                    threadPriorityActions.get(threadCtr).add(actionCtr);
                    threadRecommendedActions.get(threadCtr).add(actionCtr);
                }
            }
        }
        else if (nrFilteredActions > 0) {
            int baseActions = nrThreads / nrFilteredActions;
            int remainingThreads = nrThreads - baseActions * nrFilteredActions;
            for (int filteredActionCtr = 0; filteredActionCtr < filteredActions.size(); ++filteredActionCtr) {
                int actionCtr = filteredActions.get(filteredActionCtr);
                int actionStartThread = startThread +
                        filteredActionCtr * baseActions + Math.min(remainingThreads, filteredActionCtr);
                int actionEndThread = actionStartThread + baseActions + (filteredActionCtr < remainingThreads ? 1 : 0);
                for (int threadCtr = actionStartThread; threadCtr < actionEndThread; threadCtr++) {
                    threadPriorityActions.get(threadCtr).add(actionCtr);
                    threadRecommendedActions.get(threadCtr).add(actionCtr);
                }
            }
        }
        Arrays.fill(accumulatedReward, 0);
    }

    /**
     * Initialize UCT root node.
     *
     * @param roundCtr     	current round number
     * @param query        	the query which is optimized
     * @param useHeuristic 	whether to avoid Cartesian products
     */
    public NASPNode(long roundCtr, QueryInfo query,
                    boolean useHeuristic, List<Set<Integer>> assignment) {
        // Count node generation
        this.query = query;
        this.nrTables = query.nrJoined;
        this.tid = -1;
        createdIn = roundCtr;
        treeLevel = 0;
        nrActions = nrTables;
        priorityActions = new ArrayList<>(nrActions);
        childNodes = new NASPNode[nrActions];
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
        threadRecommendedActions = null;
        threadPriorityActions = null;
        // Available actions
        List<Integer> filteredActions = new ArrayList<>(nrActions);
        // Iterate over all actions
        for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
            // Get table associated with (join) action
            int table = nextTable[actionCtr];
            if (!query.temporaryTables.contains(table)) {
                filteredActions.add(actionCtr);
            }
        } // over actions
        Set<Integer> assignedActions = treeLevel < assignment.size() ? assignment.get(treeLevel) : null;
        if (assignedActions != null) {
            priorityActions.addAll(assignedActions);
            recommendedActions.addAll(assignedActions);
        }
        else {
            priorityActions.addAll(filteredActions);
            recommendedActions.addAll(filteredActions);
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
    public NASPNode(long roundCtr, NASPNode parent, int joinedTable, List<Set<Integer>> assignment) {
        // Count node generation
        createdIn = roundCtr;
        tid = parent.tid;
        treeLevel = parent.treeLevel + 1;
        nrActions = parent.nrActions - 1;
        childNodes = new NASPNode[nrActions];
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
        priorityActions = new ArrayList<>(nrActions);
        recommendedActions = new HashSet<>();
        threadRecommendedActions = null;
        threadPriorityActions = null;

        Set<Integer> assignedActions = treeLevel < assignment.size() ? assignment.get(treeLevel) : null;
        if (assignedActions != null) {
            priorityActions.addAll(assignedActions);
            recommendedActions.addAll(assignedActions);
        }
        else {
            priorityActions.addAll(filteredActions);
            recommendedActions.addAll(filteredActions);
        }
        Arrays.fill(accumulatedReward, 0);
    }


    /**
     * Select most interesting action to try next. Also updates
     * list of unvisited actions.
     *
     * @param policy	policy used to select action
     * @return index of action to try next
     */
    int selectAction(SelectionPolicy policy, int tid) {
        // Are there untried actions?
        List<Integer> priorityActions = threadPriorityActions.get(tid);
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
            Set<Integer> recommendedActions = threadRecommendedActions.get(tid);
            int offset = ThreadLocalRandom.current().nextInt(0, nrActions);
            int bestAction = -1;
            double bestQuality = -1;
            double[] accumulatedReward = nodeStatistics[tid].accumulatedReward;
            int[] nrTries = nodeStatistics[tid].nrTries;
            int nrVisits = nodeStatistics[tid].nrVisits;
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
            int tid = oldJoin.tid;
            int action = selectAction(policy, tid);
            int table = nextTable[action];
            joinOrder[treeLevel] = table;
            // grow tree if possible
            boolean canExpand = createdIn != roundCtr;
            if (childNodes[action] == null && canExpand) {
                childNodes[action] = new NASPNode(roundCtr, this, table, tid, tid + 1);
            }
            // evaluate via recursive invocation or via playout
            NASPNode child = childNodes[action];
            double reward = (child != null) ?
                    child.sample(roundCtr, joinOrder, oldJoin, policy):
                    playout(roundCtr, joinOrder, oldJoin);
            // update UCT statistics and return reward
            nodeStatistics[tid].updateStatistics(reward, action);
            return reward;
        }
    }
    /**
     * Recursively sample from UCT tree and return reward.
     *
     * @param roundCtr  current round (used as timestamp for expansion)
     * @param joinOrder partially completed join order
     * @param policy	policy used to select actions
     * @return achieved reward
     */
    public double sample(long roundCtr, int[] joinOrder, OldJoin oldJoin, List<Set<Integer>> assignment,
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
                childNodes[action] = new NASPNode(roundCtr, this, table, assignment);
            }
            // evaluate via recursive invocation or via playout
            NASPNode child = childNodes[action];
            double reward = (child != null) ?
                    child.sample(roundCtr, joinOrder, oldJoin, assignment, policy):
                    playout(roundCtr, joinOrder, oldJoin);
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
        for (NASPNode node: childNodes) {
            if (node != null) {
                size += node.getSize();
            }
        }
        return size;
    }

    public double getReward(int action) {
        int nrTries = this.nrTries[action];
        return nrTries == 0 ? 0 : accumulatedReward[action] / nrTries;
    }

    public void calculateSearchSpace() {

    }

    public void partitionSpace(int startThread, int endThread,
                               List<List<Set<Integer>>> actionsAssignment, NASPNode node) {
        int levelNrThreads = endThread - startThread;
        if (node == null) {
            return;
        }
        int nrActions = node.nrActions;
        double[] accumulatedReward = new double[nrActions];
        int[] nrTries = new int[nrActions];
        double[] avgRewards = new double[nrActions];
        Set<Integer> recommendedActions = new HashSet<>();
        for (Set<Integer> threadActions: node.threadRecommendedActions) {
            if (threadActions != null) {
                recommendedActions.addAll(threadActions);
            }
        }
        int nrRecommendedActions = recommendedActions.size();
        if (nrRecommendedActions == 0) {
            return;
        }
        for (NodeStatistics statistics: node.nodeStatistics) {
            if (statistics != null) {
                for (int action = 0; action < nrActions; action++) {
                    accumulatedReward[action] += statistics.accumulatedReward[action];
                    nrTries[action] += statistics.nrTries[action];
                }
            }
        }

        // Assign for threads
        if (node.treeLevel == 0) {
            double[] progress = new double[nrActions];
            for (NodeStatistics statistics: node.nodeStatistics) {
                if (statistics != null) {
                    for (int action = 0; action < nrActions; action++) {
                        progress[action] = Math.max(progress[action], statistics.progress[action]);
                    }
                }
            }
            double totalRewards = 0;
            for (int action = 0; action < nrActions; action++) {
                avgRewards[action] = accumulatedReward[action] / nrTries[action];
                totalRewards += avgRewards[action];
            }

            int[] nrAssignedThreads = new int[nrActions];
            List<Integer> sortedActions = recommendedActions.stream().sorted(
                            Comparator.comparing(action -> -1 * avgRewards[action])).
                    collect(Collectors.toList());
            int totalThreads = 0;
            boolean hasZeroAction = false;
            for (int actionCtr = 0; actionCtr < nrRecommendedActions; actionCtr++) {
                int action = sortedActions.get(actionCtr);
                int proposedThreads = Math.max((int) Math.round(avgRewards[action] /
                        totalRewards * levelNrThreads), 1);
                int remainingThreads = levelNrThreads - proposedThreads - totalThreads;
                if (remainingThreads <= 0 && actionCtr < nrRecommendedActions - 1) {
                    proposedThreads = Math.min(proposedThreads, levelNrThreads - totalThreads) - 1;
                }
                else if (remainingThreads <= 0 && actionCtr == nrRecommendedActions - 1) {
                    proposedThreads = Math.min(proposedThreads, levelNrThreads - totalThreads);
                }
                nrAssignedThreads[action] = proposedThreads;
                totalThreads += proposedThreads;
                if (proposedThreads == 0 || totalThreads == levelNrThreads) {
                    break;
                }
            }

            System.out.println(Arrays.toString(progress));
            System.out.println(Arrays.toString(avgRewards));
            System.out.println(Arrays.toString(nrAssignedThreads));
            int start = startThread;
            int end = 0;
            int zeroCtr = -1;
            for (int actionCtr = 0; actionCtr < nrRecommendedActions; actionCtr++) {
                int action = sortedActions.get(actionCtr);
                int nrProposedThreads = nrAssignedThreads[action];
                if (nrProposedThreads > 0){
                    end = start + nrProposedThreads;
                    for (int threadCtr = start; threadCtr < end; threadCtr++) {
                        List<Set<Integer>> highThreadAssignment = actionsAssignment.get(threadCtr);
                        Set<Integer> levelAssignment = new HashSet<>();
                        levelAssignment.add(action);
                        highThreadAssignment.add(levelAssignment);
                    }
                    if (nrProposedThreads > 1) {
                        partitionSpace(start, end, actionsAssignment, node.childNodes[action]);
                    }
                    start = end;
                }
                else {
                    zeroCtr = actionCtr;
                    break;
                }
            }
            if (zeroCtr >= 0) {
                // Rest of actions
                List<Set<Integer>> highThreadAssignment = actionsAssignment.get(end);
                Set<Integer> levelAssignment = new HashSet<>();
                highThreadAssignment.add(levelAssignment);
                for (int actionCtr = zeroCtr; actionCtr < nrRecommendedActions; actionCtr++) {
                    int action = sortedActions.get(actionCtr);
                    levelAssignment.add(action);
                }
            }

            // Find the most promising action
//            int maxAction = 0;
//            double maxReward = 0;
//            for (int action = 0; action < nrActions; action++) {
//                if (progress[action] > maxReward) {
//                    maxAction = action;
//                    maxReward = progress[action];
//                }
//            }
//
//            boolean atLastThread = node.threadRecommendedActions.get(endThread - 1).contains(maxAction);
//            int lowThread = atLastThread ? startThread : endThread - 1;
//            List<Set<Integer>> threadAssignment = actionsAssignment.get(lowThread);
//            Set<Integer> levelAssignment = new HashSet<>(recommendedActions);
//            levelAssignment.remove(maxAction);
//            threadAssignment.add(levelAssignment);
//            if (atLastThread) {
//                for (int threadCtr = startThread + 1; threadCtr < endThread; threadCtr++) {
//                    List<Set<Integer>> highThreadAssignment = actionsAssignment.get(threadCtr);
//                    Set<Integer> highLevelAssignment = new HashSet<>();
//                    highLevelAssignment.add(maxAction);
//                    highThreadAssignment.add(highLevelAssignment);
//                }
//                partitionSpace(startThread + 1, endThread, actionsAssignment, node.childNodes[maxAction]);
//            }
//            else {
//                for (int threadCtr = startThread; threadCtr < endThread - 1; threadCtr++) {
//                    List<Set<Integer>> highThreadAssignment = actionsAssignment.get(threadCtr);
//                    Set<Integer> highLevelAssignment = new HashSet<>();
//                    highLevelAssignment.add(maxAction);
//                    highThreadAssignment.add(highLevelAssignment);
//                }
//                partitionSpace(startThread, endThread - 1, actionsAssignment, node.childNodes[maxAction]);
//            }
        }
        else if (recommendedActions.size() == 1) {
            int maxAction = recommendedActions.iterator().next();
            for (int threadCtr = startThread; threadCtr < endThread; threadCtr++) {
                List<Set<Integer>> highThreadAssignment = actionsAssignment.get(threadCtr);
                Set<Integer> levelAssignment = new HashSet<>();
                levelAssignment.add(maxAction);
                highThreadAssignment.add(levelAssignment);
            }
            partitionSpace(startThread, endThread, actionsAssignment, node.childNodes[maxAction]);
        }
        else {
            // Estimate the size of search space from child node
            int[] nrChildActions = new int[node.nrActions];
            int sumChildActions = 0;
            for (int action: recommendedActions) {
                Set<Integer> childActions = new HashSet<>();
                if (node.childNodes[action] != null) {
                    for (Set<Integer> threadActions: node.childNodes[action].threadRecommendedActions) {
                        if (threadActions != null) {
                            childActions.addAll(threadActions);
                        }
                    }
                    nrChildActions[action] = childActions.size();
                    sumChildActions += nrChildActions[action];
                }
            }

            if (levelNrThreads >= nrRecommendedActions) {
                int[] childThreads = new int[nrActions];
                Arrays.fill(childThreads, 1);
                int totalThreads = 0;
                int remainingThreads = levelNrThreads - nrRecommendedActions;
                for (int action: recommendedActions) {
                    int nrChildThreads = (int) (Math.round((nrChildActions[action] + 0.0) /
                            sumChildActions * remainingThreads));
                    int nr = Math.min(nrChildThreads, remainingThreads - totalThreads);
                    totalThreads += nr;
                    childThreads[action] += nr;
                    if (totalThreads >= remainingThreads) {
                        break;
                    }
                }
                if (totalThreads < remainingThreads) {
                    int action = recommendedActions.iterator().next();
                    childThreads[action] += (remainingThreads - totalThreads);
                }
                int nextStart = startThread;
                int nextEnd;
                for (int action: recommendedActions) {
                    int nrChildThreads = childThreads[action];
                    nextEnd = nextStart + nrChildThreads;
                    for (int threadCtr = nextStart; threadCtr < nextEnd; threadCtr++) {
                        List<Set<Integer>> highThreadAssignment = actionsAssignment.get(threadCtr);
                        Set<Integer> levelAssignment = new HashSet<>();
                        levelAssignment.add(action);
                        highThreadAssignment.add(levelAssignment);
                    }
                    if (nextEnd - nextStart > 1) {
                        partitionSpace(nextStart, nextEnd, actionsAssignment, node.childNodes[action]);
                    }
                    nextStart = nextEnd;
                }
            }
            else {
                Iterator<Integer> actionIter = recommendedActions.iterator();
                for (int threadCtr = startThread; threadCtr < endThread - 1; threadCtr++) {
                    List<Set<Integer>> highThreadAssignment = actionsAssignment.get(threadCtr);
                    int action = actionIter.next();
                    Set<Integer> levelAssignment = new HashSet<>();
                    levelAssignment.add(action);
                    highThreadAssignment.add(levelAssignment);
                }
                List<Set<Integer>> lastThreadAssignment = actionsAssignment.get(endThread - 1);
                Set<Integer> lastThreadLevelAssignment = new HashSet<>();
                while (actionIter.hasNext()) {
                    int action = actionIter.next();
                    lastThreadLevelAssignment.add(action);
                }
                lastThreadAssignment.add(lastThreadLevelAssignment);
            }
        }
    }
}
