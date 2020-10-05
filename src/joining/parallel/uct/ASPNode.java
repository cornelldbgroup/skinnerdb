package joining.parallel.uct;

import config.JoinConfig;
import joining.parallel.join.SPJoin;
import joining.uct.SelectionPolicy;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import query.QueryInfo;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Represents node in search parallel UCT search tree.
 *
 * @author Anonymous
 */
public class ASPNode {
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
    public final ASPNode[] childNodes;
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
     * Whether the specific action is in the thread's scope.
     */
    public final boolean[][] belong;
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
    public final Set<Integer> recommendedActions;
    /**
     * Contains actions that are consistent with the "avoid
     * Cartesian products" heuristic. UCT algorithm will
     * restrict focus on such actions if heuristic flag
     * is activated.
     */
    final Set<Integer>[] filteredActions;
    /**
     * concurrent priority set
     */
    public ConcurrentLinkedDeque<Integer> prioritySet;
    /**
     * Number of threads.
     */
    final int nrThreads;
    /**
     * the parent of current node
     */
    public final ASPNode parent;
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
     * Average rewards estimate.
     */
    public final double[] avgRewards;

    /**
     * Initialize UCT root node.
     *
     * @param roundCtr     	current round number
     * @param query        	the query which is optimized
     * @param useHeuristic 	whether to avoid Cartesian products
     */
    public ASPNode(long roundCtr, QueryInfo query,
                   boolean useHeuristic, int nrThreads) {
        // Count node generation
        joinedTable = 0;
        this.query = query;
        this.nrTables = query.nrJoined;
        this.nrThreads = nrThreads;
        createdIn = roundCtr;
        treeLevel = 0;
        nrActions = nrTables;
        childNodes = new ASPNode[nrActions];
        nrVisits = new int[nrThreads];
        nrTries = new int[nrThreads][nrActions];
        accumulatedReward = new double[nrThreads][nrActions];
        belong = new boolean[nrThreads][nrActions];
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
        this.filteredActions = new HashSet[nrThreads];
        for (int i = 0; i < nrThreads; i++) {
            this.nodeStatistics[i] = new NodeStatistics(nrActions);
            this.filteredActions[i] = new HashSet<>(recommendedActions);
            Arrays.fill(belong[i], true);
        }

        this.prioritySet = new ConcurrentLinkedDeque<>();
        for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
            int table = nextTable[actionCtr];
            if (!query.temporaryTables.contains(table)) {
                prioritySet.add(actionCtr);
            }
        }
        this.action = 0;
        nextForget = new int[nrThreads];
        Arrays.fill(nextForget, 100);
        avgRewards = new double[nrActions];
    }
    /**
     * Initializes UCT node by expanding parent node.
     *
     * @param roundCtr    current round number
     * @param parent      parent node in UCT tree
     * @param joinedTable new joined table
     */
    public ASPNode(long roundCtr, ASPNode parent, int joinedTable, int action) {
        // Count node generation
        this.joinedTable = joinedTable;
        createdIn = roundCtr;
        treeLevel = parent.treeLevel + 1;
        nrActions = parent.nrActions - 1;
        nrThreads = parent.nrThreads;
        childNodes = new ASPNode[nrActions];
        nrVisits = new int[nrThreads];
        nrTries = new int[nrThreads][nrActions];
        accumulatedReward = new double[nrThreads][nrActions];
        belong = new boolean[nrThreads][nrActions];
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
            recommendedActions = new HashSet<>();
            for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
                recommendedActions.add(actionCtr);
            }
        }
        this.nodeStatistics = new NodeStatistics[nrThreads];
        this.filteredActions = new HashSet[nrThreads];
        for (int i = 0; i < nrThreads; i++) {
            this.nodeStatistics[i] = new NodeStatistics(nrActions);
            this.filteredActions[i] = new HashSet<>(recommendedActions);
            Arrays.fill(belong[i], true);
        }

        List<Integer> priorityActions = new ArrayList<>();
        for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
            if (!useHeuristic || recommendedActions.contains(actionCtr)) {
                priorityActions.add(actionCtr);
            }
        }
        this.prioritySet = new ConcurrentLinkedDeque<>(priorityActions);
        this.action = action;
        nextForget = new int[nrThreads];
        Arrays.fill(nextForget, 100);
        avgRewards = new double[nrActions];
    }


    int selectAction(long roundCtr,
                     SelectionPolicy policy,
                     List<Pair<Integer, Integer>> constraints,
                     int tid,
                     int nextDetect) {
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

        // collect all statistics
        for (int i = 0; i < nrThreads; i++) {
            NodeStatistics threadStats = nodeStatistics[i];
            for (Integer recAction : recommendedActions) {
                int threadTries = threadStats.nrTries[recAction];
                nrTries[recAction] += threadTries;
                accumulatedReward[recAction] += threadStats.accumulatedReward[recAction];
            }
        }

        double[] avgRewards = new double[nrActions];
        int[] tableToActions = new int[nrTables];
        for (int i = 0; i < nrActions; i++) {
            tableToActions[nextTable[i]] = i;
        }

        // reset the filter
        if (nextForget[tid] == nextDetect) {
            filteredActions[tid].addAll(recommendedActions);
            nextForget[tid] *= 10;
        }

        // add constraints to each node.
        Set<Integer> randomActions = filteredActions[tid];
        constraints.forEach(constraint -> {
            int left = constraint.getLeft();
            int right = constraint.getRight();
            if (!joinedTables.contains(left)) {
                int rightAction = tableToActions[right];
                randomActions.remove(rightAction);
            }
        });

        if (randomActions.size() == 0) {
            return -1;
        }

//        if (randomActions.size() == 0) {
//            System.out.println(tid + " Actions: " + Arrays.toString(recommendedActions.toArray()));
//            System.out.println(tid + " Joined: " + Arrays.toString(joinedTables.toArray()));
//            StringBuilder pairs = new StringBuilder();
//            for (Pair<Integer, Integer> pair: constraints) {
//                pairs.append(pair.toString());
//            }
//            System.out.println(tid + " Constraints: " + pairs);
//            System.exit(0);
//        }


        for (Integer action : randomActions) {
            int nrTry = nrTries[action];
            nrVisits += nrTry;
            avgRewards[action] = nrTry == 0 ? 0 : accumulatedReward[action] / nrTry;
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

//        Collections.shuffle(randomActions, ThreadLocalRandom.current());

        for (Integer action : randomActions) {
            // Calculate index of current action
            int nrTry = nrTries[action];
            if (nrTry == 0) {
                return action;
            }
            double meanReward = avgRewards[action];
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

    int selectAction(long roundCtr,
                     SelectionPolicy policy,
                     int tid) {
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

        // collect all statistics
//        for (int i = 0; i < nrThreads; i++) {
//            NodeStatistics threadStats = nodeStatistics[i];
//            for (Integer recAction : recommendedActions) {
//                int threadTries = threadStats.nrTries[recAction];
//                nrTries[recAction] += threadTries;
//                accumulatedReward[recAction] += threadStats.accumulatedReward[recAction];
//            }
//        }
        NodeStatistics threadStats = nodeStatistics[tid];
        for (Integer recAction : recommendedActions) {
            int threadTries = threadStats.nrTries[recAction];
            nrTries[recAction] += threadTries;
            accumulatedReward[recAction] += threadStats.accumulatedReward[recAction];
        }

        double[] avgRewards = new double[nrActions];


        Set<Integer> randomActions = new HashSet<>(nrActions);

        for (Integer action: recommendedActions) {
            if (belong[tid][action]) {
                randomActions.add(action);
            }
        }

        for (Integer action : randomActions) {
            int nrTry = nrTries[action];
            nrVisits += nrTry;
            avgRewards[action] = nrTry == 0 ? 0 : accumulatedReward[action] / nrTry;
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

//        Collections.shuffle(randomActions, ThreadLocalRandom.current());

        for (Integer action : randomActions) {
            // Calculate index of current action
            int nrTry = nrTries[action];
            if (nrTry == 0) {
                return action;
            }
            double meanReward = avgRewards[action];
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

    public void partitionSpace(int[] threads) {
        int end = threads[1];
        int start = threads[0];
        int nrAvailable = end - start;

        int[] nrTries = new int[nrActions];
        double[] accumulatedReward = new double[nrActions];


        // collect all statistics
        for (int i = 0; i < nrThreads; i++) {
            NodeStatistics threadStats = nodeStatistics[i];
            for (Integer recAction : recommendedActions) {
                int threadTries = threadStats.nrTries[recAction];
                nrTries[recAction] += threadTries;
                accumulatedReward[recAction] += threadStats.accumulatedReward[recAction];
            }
        }

        double[] avgRewards = new double[nrActions];
        recommendedActions.forEach(action -> {
            int nrTry = nrTries[action];
            avgRewards[action] = nrTry == 0 ? 0 : accumulatedReward[action] / nrTry;
        });
        List<Integer> sortedActions;
        int recommendSize = recommendedActions.size();

        if (nrAvailable > 1) {
            sortedActions = recommendedActions.stream().sorted(
                    Comparator.comparing(action -> -1 * avgRewards[action])).
                    collect(Collectors.toList());
            // number of actions is small
            if (nrAvailable >= recommendSize) {
                for (int i = end - 1; i > end - recommendSize; i--) {
                    boolean[] threadBelong = belong[i];
                    int actionPos = recommendSize - (end - i);
                    int action = sortedActions.get(actionPos);
                    for (int a = 0; a < threadBelong.length; a++) {
                        if (a != action) {
                            threadBelong[a] = false;
                        }
                    }
                }
                int firstAction = sortedActions.get(0);
                for (int i = 0; i <= end - recommendSize; i++) {
                    boolean[] threadBelong = belong[i];
                    for (int a = 0; a < threadBelong.length; a++) {
                        if (a != firstAction) {
                            threadBelong[a] = false;
                        }
                    }
                }
                threads[1] = end - recommendSize + 1;
                ASPNode node = childNodes[sortedActions.get(0)];
                if (node != null && node.nrActions > 0) {
                    node.partitionSpace(threads);
                }
            }
            // number of threads is small
            else {
                boolean[] lastThread = belong[end - 1];
                for (int i = 0; i < end - 1; i++) {
                    boolean[] threadBelong = belong[i];
                    int action = sortedActions.get(i);
                    for (int a = 0; a < threadBelong.length; a++) {
                        if (a != action) {
                            threadBelong[a] = false;
                        }
                    }
                    lastThread[action] = false;
                }
            }
        }
    }

    public void partitionSpaceModel(int[] threads, long[] visits) {
        int end = threads[1];
        int start = threads[0];
        int nrAvailable = end - start;

        int[] nrTries = new int[nrActions];
        double[] accumulatedReward = new double[nrActions];


        // collect all statistics
        for (int i = 0; i < nrThreads; i++) {
            NodeStatistics threadStats = nodeStatistics[i];
            for (Integer recAction : recommendedActions) {
                int threadTries = threadStats.nrTries[recAction];
                nrTries[recAction] += threadTries;
                accumulatedReward[recAction] += threadStats.accumulatedReward[recAction];
            }
        }

        double[] avgRewards = new double[nrActions];
        recommendedActions.forEach(action -> {
            int nrTry = nrTries[action];
            avgRewards[action] = nrTry == 0 ? 0 : accumulatedReward[action] / nrTry;
        });
        List<Integer> sortedActions;
        int recommendSize = recommendedActions.size();



        if (nrAvailable > 1 && recommendSize > 1) {
            sortedActions = recommendedActions.stream().sorted(
                    Comparator.comparing(action -> -1 * avgRewards[action])).
                    collect(Collectors.toList());

            if (nrAvailable >= recommendSize) {
                if (treeLevel == 0) {
                    int[] nums = new int[nrActions];
                    Arrays.fill(nums, 1);
                    long all = 0;
                    for (Integer action : sortedActions) {
                        int table = nextTable[action];
                        all += visits[table];
                    }
                    int remaining = nrAvailable - recommendSize;
                    int sum = 0;
                    for (int a = 0; a < recommendSize; a++) {
                        int action = sortedActions.get(a);
                        int table = nextTable[action];
                        int nr = (int) Math.round(remaining * (visits[table] + 0.0) / all);
                        nr = Math.min(nr, remaining - sum);
                        sum += nr;
                        nums[action] += nr;
                        if (sum >= remaining) {
                            break;
                        }
                    }
                    int threadStart = 0;
                    for (int a = 0; a < recommendSize; a++) {
                        int action = sortedActions.get(a);
                        int nr = nums[action];
                        for (int i = threadStart; i < threadStart + nr; i++) {
                            boolean[] threadBelong = belong[i];
                            for (int c = 0; c < threadBelong.length; c++) {
                                if (c != action) {
                                    threadBelong[c] = false;
                                }
                            }
                        }
                        if (nr > 1) {
                            threads[0] = threadStart;
                            threads[1] = threadStart + nr;
                            ASPNode node = childNodes[action];
                            if (node != null && node.nrActions > 0) {
                                node.partitionSpaceModel(threads, visits);
                            }
                        }
                        threadStart += nr;
                    }
                }
                else {
                    int threadsPerAction = nrAvailable / recommendSize;
                    int extraBound = nrAvailable - recommendSize * threadsPerAction;
                    int threadStart = 0;
                    for (int a = 0; a < recommendSize; a++) {
                        int nr = a < extraBound ? threadsPerAction + 1 : threadsPerAction;
                        int action = sortedActions.get(a);
                        for (int i = threadStart; i < threadStart + nr; i++) {
                            boolean[] threadBelong = belong[i];
                            for (int c = 0; c < threadBelong.length; c++) {
                                if (c != action) {
                                    threadBelong[c] = false;
                                }
                            }
                        }
                        if (nr > 1) {
                            threads[0] = threadStart;
                            threads[1] = threadStart + nr;
                            ASPNode node = childNodes[action];
                            if (node != null && node.nrActions > 0) {
                                node.partitionSpaceModel(threads, visits);
                            }
                        }
                        threadStart += nr;
                    }
                }
            }
            else {
                boolean[] lastThread = belong[end - 1];
                for (int i = start; i < end - 1; i++) {
                    boolean[] threadBelong = belong[i];
                    int action = sortedActions.get(i - start);
                    for (int a = 0; a < threadBelong.length; a++) {
                        if (a != action) {
                            threadBelong[a] = false;
                        }
                    }
                    lastThread[action] = false;
                }
            }

        }
    }

    public void partitionSpaceUtility(int[] threads, double[] utilities, int[] threadPerTable, int[] best) {
        int end = threads[1];
        int start = threads[0];
        int nrAvailable = end - start;

        int[] nrTries = new int[nrActions];
        double[] accumulatedReward = new double[nrActions];


        // collect all statistics
        for (int i = 0; i < nrThreads; i++) {
            NodeStatistics threadStats = nodeStatistics[i];
            for (Integer recAction : recommendedActions) {
                int threadTries = threadStats.nrTries[recAction];
                nrTries[recAction] += threadTries;
                accumulatedReward[recAction] += threadStats.accumulatedReward[recAction];
            }
        }

        double[] avgRewards = new double[nrActions];
        recommendedActions.forEach(action -> {
            int nrTry = nrTries[action];
            avgRewards[action] = nrTry == 0 ? 0 : accumulatedReward[action] / nrTry;
        });
        List<Integer> sortedActions;
        int recommendSize = recommendedActions.size();

        if (nrAvailable > 1 && recommendSize > 1) {
            if (nrAvailable >= recommendSize) {
                if (treeLevel == 0) {
                    sortedActions = recommendedActions.stream().sorted(
                            Comparator.comparing(action -> -1 * utilities[nextTable[action]])).
                            collect(Collectors.toList());
                    int[] nums = new int[nrActions];
                    Arrays.fill(nums, 1);
                    double all = 0;
                    for (Integer action : sortedActions) {
                        int table = nextTable[action];
                        all += utilities[table];
                    }
                    int remaining = nrAvailable - recommendSize;
                    int sum = 0;
                    for (int a = 0; a < recommendSize; a++) {
                        int action = sortedActions.get(a);
                        int table = nextTable[action];
                        int nr = (int) Math.round(remaining * (utilities[table] + 0.0) / all);
                        nr = Math.min(nr, remaining - sum);
                        sum += nr;
                        nums[action] += nr;
                        if (sum >= remaining) {
                            break;
                        }
                    }
                    // record number of threads
                    for (int a = 0; a < recommendSize; a++) {
                        int action = sortedActions.get(a);
                        int table = nextTable[action];
                        threadPerTable[table] = nums[action];
                    }
                    int threadStart = 0;
                    for (int a = 0; a < recommendSize; a++) {
                        int action = sortedActions.get(a);
                        int nr = nums[action];
                        for (int i = threadStart; i < threadStart + nr; i++) {
                            boolean[] threadBelong = belong[i];
                            Arrays.fill(threadBelong, true);
                            for (int c = 0; c < threadBelong.length; c++) {
                                if (c != action) {
                                    threadBelong[c] = false;
                                }
                            }
                        }
                        if (nr > 1) {
                            threads[0] = threadStart;
                            threads[1] = threadStart + nr;
                            ASPNode node = childNodes[action];
                            if (node != null && node.nrActions > 0) {
                                node.partitionSpaceUtility(threads, utilities, threadPerTable, best);
                            }
                        }
                        threadStart += nr;
                    }
                }
                else {
                    sortedActions = recommendedActions.stream().sorted(
                            Comparator.comparing(action -> -1 * avgRewards[action])).
                            collect(Collectors.toList());
                    int threadsPerAction = nrAvailable / recommendSize;
                    int extraBound = nrAvailable - recommendSize * threadsPerAction;
                    int threadStart = 0;
                    for (int a = 0; a < recommendSize; a++) {
                        int nr = a < extraBound ? threadsPerAction + 1 : threadsPerAction;
                        int action = sortedActions.get(a);
                        for (int i = threadStart; i < threadStart + nr; i++) {
                            boolean[] threadBelong = belong[i];
                            Arrays.fill(threadBelong, true);
                            for (int c = 0; c < threadBelong.length; c++) {
                                if (c != action) {
                                    threadBelong[c] = false;
                                }
                            }
                        }
                        if (nr > 1) {
                            threads[0] = threadStart;
                            threads[1] = threadStart + nr;
                            ASPNode node = childNodes[action];
                            if (node != null && node.nrActions > 0) {
                                node.partitionSpaceUtility(threads, utilities, threadPerTable, best);
                            }
                        }
                        threadStart += nr;
                    }
                }
            }
            else {
                if (treeLevel == 0) {
                    Arrays.fill(threadPerTable, 1);
                    int[] rank = new int[nrTables];
                    for (int i = 0; i < nrTables; i++) {
                        int table = best[i];
                        rank[table] = i;
                    }
                    sortedActions = recommendedActions.stream().sorted(
                            Comparator.comparing(action -> rank[nextTable[action]])).
                            collect(Collectors.toList());
                    System.out.println(Arrays.toString(sortedActions.toArray()));
                }
                else {
                    sortedActions = recommendedActions.stream().sorted(
                            Comparator.comparing(action -> -1 * avgRewards[action])).
                            collect(Collectors.toList());
                }
//                boolean[] lastThread = belong[end - 1];
//                Arrays.fill(lastThread, true);
                for (int i = start; i < end; i++) {
                    boolean[] threadBelong = belong[i];
                    Arrays.fill(threadBelong, true);
                    int action = sortedActions.get(i - start);
                    for (int a = 0; a < threadBelong.length; a++) {
                        if (a != action) {
                            threadBelong[a] = false;
                        }
                    }
//                    lastThread[action] = false;
                }
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
    int selectAction(long roundCtr, SelectionPolicy policy, int tid, SPJoin spJoin, int[] last, int[] joinOrder) {
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
        int[] nrVisited = new int[nrActions];
        int[] nrIndexed = new int[nrActions];




        // collect all statistics
        for (int i = 0; i < nrThreads; i++) {
            NodeStatistics threadStats = nodeStatistics[i];
            nrVisits += threadStats.nrVisits;
            for (Integer recAction : recommendedActions) {
                int threadTries = threadStats.nrTries[recAction];
                nrTries[recAction] += threadTries;
                accumulatedReward[recAction] += threadStats.accumulatedReward[recAction];
                nrVisited[recAction] += threadStats.nrVisited[recAction];
                nrIndexed[recAction] += threadStats.nrIndexed[recAction];
            }
        }

        double[] avgRewards = new double[nrActions];
        recommendedActions.forEach(action -> {
            int nrTry = nrTries[action];
            avgRewards[action] = nrTry == 0 ? 0 : accumulatedReward[action] / nrTry;
        });
        List<Integer> randomActions;
        int lastID = last[0];
//        spJoin.writeLog(Arrays.toString(nextTable) + ": " + Arrays.toString(nrVisited) +
//                "\t" + Arrays.toString(nrIndexed) + "\t" + Arrays.toString(this.avgRewards));
        if (tid <= lastID && lastID > 0 &&  recommendedActions.size() > 1) {
            if (treeLevel == 0) {
                randomActions = recommendedActions.stream().sorted(
                        Comparator.comparing(
                                action -> -1 * (0.5 * this.avgRewards[action] + 0.5 * avgRewards[action]))).
                        collect(Collectors.toList());
                int promisingAction = randomActions.get(0);
                int nrPriorActions = randomActions.size();
                int bound = lastID + 1 - nrPriorActions;
                if (bound >= 0) {
                    if (tid <= bound) {
                        last[0] = bound;
                        return promisingAction;
                    }
                    else {
                        last[0] = -1;
                        return randomActions.get(tid - bound);
                    }
                }
                else {
                    last[0] = -1;
                    if (tid < lastID) {
                        return randomActions.get(tid);
                    }
                    else {
                        randomActions = randomActions.subList(lastID, nrPriorActions);
                    }
                }
            }
            else {
                for (Integer action: recommendedActions) {
                    int table = nextTable[action];
                    Set<Integer> connection = query.joinConnection.get(table);
                    if (connection.size() == 1) {
                        return action;
                    }
                }
                double[] avgIndexed = new double[nrActions];
                recommendedActions.forEach(action -> {
                    int nrTry = nrVisited[action];
                    avgIndexed[action] = nrTry == 0 ? 0 : (nrIndexed[action] + 0.0) / nrTry;
                });
                randomActions = recommendedActions.stream().sorted(
                        Comparator.comparing(action -> avgIndexed[action])).collect(Collectors.toList());
                int promisingAction = randomActions.get(0);
                int nrPriorActions = randomActions.size();
                int bound = lastID + 1 - nrPriorActions;
                int badAction = randomActions.get(nrPriorActions - 1);
                double badSize = avgIndexed[badAction];
                double size = Arrays.stream(avgIndexed).sum();
                double remainingSize = size - badSize;
                if (badSize >= remainingSize && bound >= 0) {
                    if (tid == lastID) {
                        last[0] = -1;
                        return badAction;
                    }
                    else {
                        int pid = 0;
                        for (Integer action: recommendedActions) {
                            if (action != badAction) {
                                if (tid <= bound) {
                                    last[0] = bound;
                                    return action;
                                }
                                else if (pid + bound == tid) {
                                    last[0] = -1;
                                    return action;
                                }
                                pid++;
                            }
                        }

                    }
                }
                else {
                    if (bound >= 0) {
                        if (tid <= bound) {
                            last[0] = bound;
                            return promisingAction;
                        }
                        else {
                            last[0] = -1;
                            return randomActions.get(tid - bound);
                        }
                    }
                    else {
                        last[0] = -1;
                        if (tid < lastID) {
                            return randomActions.get(tid);
                        }
                        else {
                            randomActions = randomActions.subList(lastID, nrPriorActions);
                        }
                    }
                }

            }
        }
        else {
            double[] avgIndexed = new double[nrActions];
            recommendedActions.forEach(action -> {
                int nrTry = nrVisited[action];
                avgIndexed[action] = nrTry == 0 ? 0 : (nrIndexed[action] + 0.0) / nrTry;
            });
            randomActions = recommendedActions.stream().sorted(
                    Comparator.comparing(action -> avgIndexed[action])).collect(Collectors.toList());
//            int priorTable = treeLevel > 0 ? joinOrder[treeLevel - 1] : -1;
            for (Integer action: recommendedActions) {
                int table = nextTable[action];
                Set<Integer> connection = query.joinConnection.get(table);
                if (connection.size() == 1) {
                    return action;
                }
            }
            return randomActions.get(0);
//            if (randomActions.size() == 0)
//                randomActions.addAll(recommendedActions);
        }


//        if (tid <= lastID && lastID > 0 &&  recommendedActions.size() > 1) {
//            randomActions = recommendedActions.stream().sorted(
//                    Comparator.comparing(action ->  -1 * avgRewards[action])).collect(Collectors.toList());
//            int nrPriorActions = randomActions.size();
//            int promisingAction = randomActions.get(0);
//            double allRewards = Arrays.stream(avgRewards).sum();
//            double promisingReward = avgRewards[promisingAction];
//            double restRewards = allRewards - promisingReward;
//            // the promising reward is dominated
//            if (restRewards <= promisingReward && treeLevel == 0) {
//                if (tid == lastID) {
//                    last[0] = -1;
//                    randomActions = randomActions.subList(1, nrPriorActions);
//                }
//                else {
//                    last[0]--;
//                    return promisingAction;
//                }
//            }
//            else if (treeLevel < nrTables || restRewards <= promisingReward) {
//                int bound = lastID + 1 - nrPriorActions;
//                if (bound >= 0) {
//                    if (tid <= bound) {
//                        last[0] = bound;
//                        return promisingAction;
//                    }
//                    else {
//                        last[0] = -1;
//                        return randomActions.get(tid - bound);
//                    }
//                }
//                else {
//                    last[0] = -1;
//                    if (tid < lastID) {
//                        return randomActions.get(tid);
//                    }
//                    else {
//                        randomActions = randomActions.subList(lastID, nrPriorActions);
//                    }
//                }
//            }
//            // the rest rewards are dominated
//            else {
//                int minPartitions = Math.min(nrPriorActions - 1, lastID);
//                double avgReward = restRewards / minPartitions;
//                int nrPartitions = 0;
//                double acc = 0;
//                int start = 0;
//                int end = nrPriorActions;
//                int diff = lastID - tid;
//                for (int i = nrPriorActions - 1; i >= 1; i--) {
//                    acc += avgRewards[randomActions.get(i)];
//                    start = i;
//                    if (acc >= avgReward || i == 1) {
//                        if (diff == nrPartitions) {
//                            randomActions = randomActions.subList(start, end);
//                            nrPartitions++;
//                            last[0] -= nrPartitions;
//                            break;
//                        }
//                        else {
//                            acc = 0;
//                            end = i;
//                            nrPartitions++;
//                        }
//                    }
//                }
////                spJoin.writeLog(tid + ": " + nrPartitions + "\t[" + start + "," + end + "]\t" + diff);
//                // thread is in the promising part
//                if (diff >= nrPartitions) {
//                    last[0] -= nrPartitions;
//                    return promisingAction;
//                }
//            }
//        }
//        else {
//            randomActions = new ArrayList<>(recommendedActions);
//        }



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

//        Collections.shuffle(randomActions, ThreadLocalRandom.current());

        for (Integer action : randomActions) {
            // Calculate index of current action
            int nrTry = nrTries[action];
            if (nrTry == 0) {
                return action;
            }
            double meanReward = avgRewards[action];
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
    double playout(long roundCtr, int[] joinOrder, SPJoin spJoin) throws Exception {
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
            List<Integer> unjoinedTablesShuffled = new ArrayList<>(unjoinedTables);
            Collections.shuffle(unjoinedTablesShuffled, ThreadLocalRandom.current());
            Iterator<Integer> unjoinedTablesIter = unjoinedTablesShuffled.iterator();
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
        return spJoin.execute(joinOrder, (int) roundCtr);
    }

    double playout(long roundCtr, int[] joinOrder, SPJoin spJoin,
                   List<Pair<Integer, Integer>> constraint) throws Exception {
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
    public double sample(long roundCtr,
                         int[] joinOrder,
                         SPJoin spJoin,
                         SelectionPolicy policy) throws Exception {
        int tid = spJoin.tid;
        // Check if this is a (non-extendible) leaf node
        if (nrActions == 0) {
            // Initialize table nodes
            return spJoin.execute(joinOrder, (int) roundCtr);
        }
        else {
            // inner node - select next action and expand tree if necessary
            int action = selectAction(roundCtr, policy, tid);
            if (action < 0) {
                System.out.println(tid + " " + Arrays.toString(belong[tid]));
                System.exit(0);
            }
            int table = nextTable[action];
            joinOrder[treeLevel] = table;
            // grow tree if possible
            boolean canExpand = createdIn != roundCtr;
            ASPNode child = childNodes[action];
            // let join operator knows which space is evaluating.
            if (canExpand && child == null) {
                if (childNodes[action] == null) {
                    childNodes[action] = new ASPNode(roundCtr, this, table, action);
                }
            }
            // evaluate via recursive invocation or via playout
            boolean isSample = child != null;
            double reward = isSample ?
                    child.sample(roundCtr, joinOrder, spJoin, policy):
                    playout(roundCtr, joinOrder, spJoin);
            // update UCT statistics and return reward
//            reward = 0.01;
            nodeStatistics[tid].updateStatistics(reward, action);
            if (treeLevel == 0 && roundCtr < 100) {
                avgRewards[table] = Math.max(avgRewards[table], spJoin.progress);
            }
            return reward;
        }
    }
    public double sample(long roundCtr,
                         int[] joinOrder,
                         SPJoin spJoin,
                         SelectionPolicy policy,
                         boolean[] tags,
                         double[] weights,
                         int branchLevel, int[] last) throws Exception {
        int tid = spJoin.tid;
        // Check if this is a (non-extendible) leaf node
        if (nrActions == 0) {
            // Initialize table nodes
            return spJoin.execute(joinOrder, (int) roundCtr);
        }
        else {
            // inner node - select next action and expand tree if necessary
            int action = selectAction(roundCtr, policy, tid, spJoin, last, joinOrder);
            if (action == -1) {
                System.out.println("tid: " + tid + "\t" + Arrays.toString(belong[tid]) + " " +
                        Arrays.toString(recommendedActions.toArray()));
                System.exit(0);
            }
            int table = nextTable[action];
            joinOrder[treeLevel] = table;
            // grow tree if possible
            boolean canExpand = createdIn != roundCtr;
            ASPNode child = childNodes[action];
            // let join operator knows which space is evaluating.
            if (canExpand && child == null) {
                if (childNodes[action] == null) {
                    childNodes[action] = new ASPNode(roundCtr, this, table, action);
                }
            }
            // evaluate via recursive invocation or via playout
            boolean isSample = child != null;
            double reward = isSample ?
                    child.sample(roundCtr, joinOrder, spJoin, policy, tags, weights, branchLevel + 1, last):
                    playout(roundCtr, joinOrder, spJoin);
            // update UCT statistics and return reward
//            reward = 0.01;
//            nodeStatistics[tid].updateStatistics(reward, action);
            nodeStatistics[tid].updateStatistics(reward, spJoin.nrVisited[table], spJoin.nrIndexed[table], action);
            if (treeLevel == 0 && roundCtr < 100) {
                avgRewards[table] = Math.max(avgRewards[table], spJoin.progress);
            }
            return reward;
        }
    }

    public double sample(long roundCtr,
                         int[] joinOrder,
                         SPJoin spJoin,
                         SelectionPolicy policy,
                         List<Pair<Integer, Integer>> constraints,
                         int nextDetect) throws Exception {
        int tid = spJoin.tid;
        // Check if this is a (non-extendible) leaf node
        if (nrActions == 0) {
            // Initialize table nodes
            return spJoin.execute(joinOrder, (int) roundCtr);
        }
        else {
            double reward = -1;
            while (reward < 0) {
                // inner node - select next action and expand tree if necessary
                int action = selectAction(roundCtr, policy, constraints, tid, nextDetect);
                if (action == -1) {
                    return -1;
                }
                int table = nextTable[action];
                joinOrder[treeLevel] = table;
                // grow tree if possible
                boolean canExpand = createdIn != roundCtr;
                ASPNode child = childNodes[action];
                // let join operator knows which space is evaluating.
                if (canExpand && child == null) {
                    if (childNodes[action] == null) {
                        childNodes[action] = new ASPNode(roundCtr, this, table, action);
                    }
                }
                // evaluate via recursive invocation or via playout
                boolean isSample = child != null;
                reward = isSample ?
                        child.sample(roundCtr, joinOrder, spJoin, policy, constraints, nextDetect):
                        playout(roundCtr, joinOrder, spJoin);
                if (reward >= 0) {
                    // update UCT statistics and return reward
                    nodeStatistics[tid].updateStatistics(reward, action);
                }
                else {
                    if (isSample && child.filteredActions[tid].size() == 0) {
                        filteredActions[tid].remove(action);
                    }
                }
            }

            return reward;
        }
    }

    public void getConstraints(int size, List<Pair<Integer, Integer>> constraints) {
        ASPNode node = this;
        while (constraints.size() < size) {
            if (node.recommendedActions.size() > 1) {
                int[] nrTries = new int[node.nrActions];
                // collect all statistics
                for (int i = 0; i < nrThreads; i++) {
                    NodeStatistics threadStats = node.nodeStatistics[i];
                    for (Integer recAction : node.recommendedActions) {
                        int threadTries = threadStats.nrTries[recAction];
                        nrTries[recAction] += threadTries;
                    }
                }
                int hotIndex = -1;
                int hotVisits = -1;
                for (Integer action: node.recommendedActions) {
                    hotIndex = nrTries[action] > hotVisits ? action : hotIndex;
                    hotVisits = Math.max(hotVisits, nrTries[action]);
                }
                if (node.treeLevel == 0) {
                    int finalHotIndex = hotIndex;
                    int otherIndex = node.recommendedActions.stream().
                            filter(action -> action != finalHotIndex).findFirst().orElse(-1);
                    if (finalHotIndex >= 0 && otherIndex >= 0) {
                        constraints.add(new ImmutablePair<>(node.nextTable[otherIndex], node.nextTable[finalHotIndex]));
                    }
                }
                else {
                    for (Integer action: node.recommendedActions) {
                        if (action != hotIndex) {
                            constraints.add(new ImmutablePair<>(node.nextTable[action], node.nextTable[hotIndex]));
                        }
                        if (constraints.size() >= size) {
                            return;
                        }

                    }
                }
                node = node.childNodes[hotIndex];
                if (node == null) {
                    return;
                }
            }
            else if (node.nrActions > 0) {
                int hotIndex = node.recommendedActions.iterator().next();
                node = node.childNodes[hotIndex];
                if (node == null) {
                    return;
                }
            }
            else {
                break;
            }
        }
    }

    public static List<List<Pair<Integer, Integer>>> getNodeConstraints(int nrThreads, ASPNode node) {
        List<List<Pair<Integer, Integer>>> threadsConstraints = new ArrayList<>(nrThreads);
        IntStream.range(0, nrThreads).forEach(i -> threadsConstraints.add(new ArrayList<>()));
        int lastThread = nrThreads - 1;
        while (lastThread > 0) {
            if (node.recommendedActions.size() > 1) {
                int[] nrTries = new int[node.nrActions];
                // collect all statistics
                for (int i = 0; i < nrThreads; i++) {
                    NodeStatistics threadStats = node.nodeStatistics[i];
                    for (Integer recAction : node.recommendedActions) {
                        int threadTries = threadStats.nrTries[recAction];
                        nrTries[recAction] += threadTries;
                    }
                }
                List<Integer> sortedIndex = node.recommendedActions.stream().sorted(
                        Comparator.comparing(action -> -1 * nrTries[action])).collect(Collectors.toList());
//                ASPNode finalNode = node;
//                System.out.println(Arrays.toString(sortedIndex.stream().mapToInt(action -> finalNode.nextTable[action]).toArray()));
                int hotIndex = sortedIndex.get(0);
                if (node.treeLevel == 0) {
                    int otherIndex = sortedIndex.get(1);
                    int leftTable = node.nextTable[hotIndex];
                    int rightTable = node.nextTable[otherIndex];
                    Pair<Integer, Integer> hotConstraint = new ImmutablePair<>(leftTable, rightTable);
                    Pair<Integer, Integer> complementConstraint = new ImmutablePair<>(rightTable, leftTable);
                    for (int i = 0; i < lastThread; i++) {
                        threadsConstraints.get(i).add(hotConstraint);
                    }
                    threadsConstraints.get(lastThread).add(complementConstraint);

//                    int nrTables = node.nrActions - 1;
//                    for (int i = 0; i < lastThread - nrTables + 1; i++) {
//                        threadsConstraints.get(i).add(hotConstraint);
//                    }
//                    for (int i = lastThread - nrTables + 1; i <= lastThread; i++) {
//                        int sid = i - (lastThread - nrTables);
//                        int table = node.nextTable[sortedIndex.get(sid)];
//                        threadsConstraints.get(lastThread).add(new ImmutablePair<>(table, -1));
//                    }
                    lastThread--;
                }
                else {
                    int rval = 1;
                    int nrConstraints = 0;
                    while(rval <= lastThread + 1) {
                        rval <<= 1;
                        nrConstraints++;
                    }
                    nrConstraints = Math.min(node.recommendedActions.size() - 1, nrConstraints - 1);
                    rval = (int) Math.pow(2, nrConstraints);
                    List<Pair<Integer, Integer>> hotConstraints = new ArrayList<>(nrConstraints);
                    List<Pair<Integer, Integer>> complementConstraints = new ArrayList<>(nrConstraints);
                    for (int cid = 0; cid < nrConstraints; cid++) {
                        int leftTable = node.nextTable[sortedIndex.get(cid)];
                        int rightTable = node.nextTable[sortedIndex.get(cid + 1)];
                        Pair<Integer, Integer> hotConstraint = new ImmutablePair<>(leftTable, rightTable);
                        Pair<Integer, Integer> complementConstraint = new ImmutablePair<>(rightTable, leftTable);
                        hotConstraints.add(hotConstraint);
                        complementConstraints.add(complementConstraint);
                    }
//                    for (int cid = 0; cid < nrConstraints; cid++) {
//                        for (int i = 0; i < lastThread - rval + 1; i++) {
//                            threadsConstraints.get(i).add(hotConstraints.get(cid));
//                        }
//                    }
                    for (int i = lastThread - rval + 1; i <= lastThread; i++) {
                        int sid = i - (lastThread - rval + 1);
                        StringBuilder binary = new StringBuilder(Integer.toBinaryString(sid));
                        while (binary.length() < nrConstraints) {
                            binary.insert(0, "0");
                        }
                        for (int cid = 0; cid < nrConstraints; cid++) {
                            char tag = binary.charAt(cid);
                            Pair<Integer, Integer> target = tag == '0' ?
                                    hotConstraints.get(cid) : complementConstraints.get(cid);
                            threadsConstraints.get(i).add(target);
                        }
                    }
                    lastThread = lastThread - rval;

                }
                node = node.childNodes[hotIndex];
                if (node == null) {
                    break;
                }
            }
            else if (node.nrActions > 0) {
                int hotIndex = node.recommendedActions.iterator().next();
                node = node.childNodes[hotIndex];
                if (node == null) {
                    break;
                }
            }
            else {
                break;
            }
        }
        return threadsConstraints;
    }


    public double bestJoinOrder(int[] joinOrder, int tid) {
        if (nrActions > 0) {
            int maxAction = -1;
            double maxRewad = -1;
            NodeStatistics threadStats = nodeStatistics[tid];
            for(Integer recAction : recommendedActions) {
                int threadTries = threadStats.nrTries[recAction];
                double reward = 0;
                if (threadTries != 0) {
                    reward = threadStats.accumulatedReward[recAction] / threadTries;
                }
                if (reward > maxRewad) {
                    maxRewad = reward;
                    maxAction = recAction;
                }
            }

            int table = nextTable[maxAction];
            joinOrder[treeLevel] = table;
            ASPNode child = childNodes[maxAction];

            if (child == null) {
                int lastTable = joinOrder[treeLevel];
                Set<Integer> newlyJoined = new HashSet<>(joinedTables);
                newlyJoined.add(lastTable);
                // Iterate over join order positions to fill
                List<Integer> unjoinedTablesShuffled = new ArrayList<>(unjoinedTables);
                Collections.shuffle(unjoinedTablesShuffled, ThreadLocalRandom.current());
                for (int posCtr = treeLevel + 1; posCtr < nrTables; ++posCtr) {
                    boolean foundTable = false;
                    for (int joinedTable : unjoinedTablesShuffled) {
                        if (!newlyJoined.contains(joinedTable) &&
                                query.connected(newlyJoined, joinedTable)) {
                            joinOrder[posCtr] = joinedTable;
                            newlyJoined.add(joinedTable);
                            foundTable = true;
                            break;
                        }
                    }
                    if (!foundTable) {
                        for (int joinedTable : unjoinedTablesShuffled) {
                            if (!newlyJoined.contains(joinedTable)) {
                                joinOrder[posCtr] = joinedTable;
                                newlyJoined.add(joinedTable);
                                break;
                            }
                        }
                    }
                }
            }
            else {
                child.bestJoinOrder(joinOrder, tid);
            }
            return maxRewad;
        }
        return 0;
    }

    public int nodesInLevel(int leftTable) {
        int action = -1;
        for (Integer recAction: recommendedActions) {
            if (nextTable[recAction] == leftTable) {
                action = recAction;
                break;
            }
        }
        if (action >= 0) {
            ASPNode node = childNodes[action];
            while (true) {
                int size = node.recommendedActions.size();
                if (size > 1) {
                    return size;
                }
                else {
                    int singleAction = node.recommendedActions.iterator().next();
                    node = node.childNodes[singleAction];
                }
            }
        }
        else {
            return 0;
        }
    }

    /**
     * Recursively calculate the memory consumption for the UCT tree
     *
     * @param isLocal   whether to look up statistics in the local memory.
     * @return          size for each node.
     */
    public long getSize(boolean isLocal) {
        int availableThread = 0;
        for (NodeStatistics statistics: nodeStatistics) {
            if (statistics.nrVisits > 1) {
                availableThread++;
            }
        }
        long size = isLocal ? availableThread * nrActions * 12 : nrActions * 12;
        size += 16 + (unjoinedTables.size() + joinedTables.size() + recommendedActions.size() + nextTable.length) * 4 ;
        for (ASPNode node: childNodes) {
            if (node != null) {
                size += node.getSize(isLocal);
            }
        }
        return size;
    }
}
