package joining.parallel.uct;

import config.JoinConfig;
import config.ParallelConfig;
import joining.uct.SelectionPolicy;
import joining.parallel.join.DPJoin;
import query.QueryInfo;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Represents node in data joining.parallel UCT search tree.
 *
 * @author Anonymous
 */
public class DPNode {
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
    private int nrVisits;
    /**
     * Number of times each action was tried out.
     */
    private int[] nrTries;
    /**
     * Reward accumulated for specific actions.
     */
    private double[] accumulatedReward;
    /**
     * Read write lock for reward load/update
     */
    private ReadWriteLock rewardLock;
    /**
     * Lock to protect node creation.
     */
    private ReentrantLock nodeLock;
    /**
     * node statistics that should be aligned to a cache line
     */
    public NodeStatistics[] nodeStatistics;
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
    final boolean useHeuristic;
    /**
     * Contains actions that are consistent with the "avoid
     * Cartesian products" heuristic. UCT algorithm will
     * restrict focus on such actions if heuristic flag
     * is activated.
     */
    final Set<Integer> recommendedActions;
    /**
     * concurrent priority set
     */
    private LinkedList<Integer>[] prioritySet;
//    private ConcurrentLinkedDeque<Integer> prioritySet;
    /**
     * Number of threads.
     */
    final int nrThreads;
    /**
     * UCT extension of split tables
     */
    BaseUctInner tableRoot = null;
    /**
     * the parent of current node
     */
    final DPNode parent;

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
        this.query = query;
        this.nrTables = query.nrJoined;
        this.nrThreads = nrThreads;
        createdIn = roundCtr;
        treeLevel = 0;
        nrActions = nrTables;
        childNodes = new DPNode[nrActions];
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
//            accumulatedReward[action] = 0;
            int table = nextTable[action];
            if (!query.temporaryTables.contains(table)) {
                recommendedActions.add(action);
            }
        }
        this.nodeStatistics = new NodeStatistics[nrThreads];

        for (int i = 0; i < nrThreads; i++) {
            this.nodeStatistics[i] = new NodeStatistics(nrActions);
        }

        this.prioritySet = new LinkedList[nrThreads];
        for (int i = 0; i < nrThreads; i++) {
            prioritySet[i] = new LinkedList<>();
            for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
                int table = nextTable[actionCtr];
                if (!query.temporaryTables.contains(table)) {
                    prioritySet[i].add(actionCtr);
                }
            }
        }

//        this.prioritySet = new ConcurrentLinkedDeque<>();
//        for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
//            int table = nextTable[actionCtr];
//            if (!query.temporaryTables.contains(table)) {
//                prioritySet.add(actionCtr);
//            }
//        }

        // initialize read-write lock for DPDsync
        if (ParallelConfig.PARALLEL_SPEC == 1) {
            nrTries = new int[nrActions];
            accumulatedReward = new double[nrActions];
            rewardLock = new ReentrantReadWriteLock();
            nodeLock = new ReentrantLock();
        }
    }
    /**
     * Initializes UCT node by expanding parent node.
     *
     * @param roundCtr    current round number
     * @param parent      parent node in UCT tree
     * @param joinedTable new joined table
     */
    public DPNode(long roundCtr, DPNode parent, int joinedTable,
                  int[] joinOrder, int[] cardinalities) {
        // Count node generation
        createdIn = roundCtr;
        treeLevel = parent.treeLevel + 1;
        nrActions = parent.nrActions - 1;
        nrThreads = parent.nrThreads;
        childNodes = new DPNode[nrActions];
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
        this.nodeStatistics = new NodeStatistics[nrThreads];

        for (int i = 0; i < nrThreads; i++) {
            this.nodeStatistics[i] = new NodeStatistics(nrActions);
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
                    int table = nextTable[actionCtr];
                    if (!query.temporaryTables.contains(table)) {
                        recommendedActions.add(actionCtr);
                    }
                }
            }
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
                int table = nextTable[actionCtr];
                if (!query.temporaryTables.contains(table)) {
                    recommendedActions.add(actionCtr);
                }
            }
            if (recommendedActions.isEmpty()) {
                // add all actions to recommended actions
                for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
                    recommendedActions.add(actionCtr);
                }
            }
        }

        List<Integer> priorityActions = new ArrayList<>();
        for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
            if (recommendedActions.contains(actionCtr)) {
                priorityActions.add(actionCtr);
            }
        }

        this.prioritySet = new LinkedList[nrThreads];
        for (int i = 0; i < nrThreads; i++) {
            prioritySet[i] = new LinkedList<>(priorityActions);
        }

        // initialize read-write lock for DPDsync
        if (ParallelConfig.PARALLEL_SPEC == 1) {
            nrTries = new int[nrActions];
            accumulatedReward = new double[nrActions];
            rewardLock = new ReentrantReadWriteLock();
            nodeLock = new ReentrantLock();
        }
    }


    /**
     * Select most interesting action to try next. Also updates
     * list of unvisited actions.
     *
     * @param policy	policy used to select action
     * @return index of action to try next
     */
    int selectAction(SelectionPolicy policy, int tid, DPJoin dpJoin) {
        /*
         * We apply the UCT formula as no actions are untried.
         * We iterate over all actions and calculate their
         * UCT value, updating best action and best UCT value
         * on the way. We start iterations with a randomly
         * selected action to ensure that we pick a random
         * action among the ones with maximal UCT value.
         */
        Integer priorAction = null;
//        if (!prioritySet.isEmpty()) {
//            priorAction = prioritySet.pollFirst();
//        }
        if (!prioritySet[tid].isEmpty()) {
            priorAction = prioritySet[tid].pollFirst();
        }
        if (priorAction != null) {
            return priorAction;
        }
        int nrVisits = 0;
        int[] nrTries = new int[nrActions];
        double[] accumulatedReward = new double[nrActions];
        if (ParallelConfig.PARALLEL_SPEC == 0 || ParallelConfig.PARALLEL_SPEC == 11
                || ParallelConfig.PARALLEL_SPEC == 12 || ParallelConfig.PARALLEL_SPEC == 13) {
            for (int i = 0; i < nrThreads; i++) {
                NodeStatistics threadStats = nodeStatistics[i];
                nrVisits += threadStats.nrVisits;
                for (Integer recAction : recommendedActions) {
                    int threadTries = threadStats.nrTries[recAction];
                    nrTries[recAction] += threadTries;
                    accumulatedReward[recAction] += threadStats.accumulatedReward[recAction];
                }
            }
        }
        else {
            rewardLock.readLock().lock();
            nrVisits = this.nrVisits;
            for (Integer recAction : recommendedActions) {
                nrTries[recAction] = this.nrTries[recAction];
                accumulatedReward[recAction] = this.accumulatedReward[recAction];
            }
            rewardLock.readLock().unlock();
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
        for (Integer action : recommendedActions) {
            // Calculate index of current action
            int nrTry = nrTries[action];
            if (nrTry == 0) {
                return action;
            }
            double meanReward = accumulatedReward[action] / nrTry;
            double exploration = Math.sqrt(Math.log(nrVisits) / nrTry);
            // Assess the quality of the action according to policy
            double quality = meanReward;
//            exploration *= JoinConfig.EXPLORATION_WEIGHT;
            exploration *= (nrThreads == 1 ? JoinConfig.EXPLORATION_WEIGHT : JoinConfig.PARALLEL_WEIGHT);
            quality += exploration;
//            double quality = meanReward + JoinConfig.EXPLORATION_WEIGHT * exploration;
            if (quality > bestQuality) {
                bestAction = action;
                bestQuality = quality;
            }
        }
        // For epsilon greedy, return random action with
        // probability epsilon.
//        if (policy.equals(SelectionPolicy.EPSILON_GREEDY)) {
//            if (random.nextDouble()<=JoinConfig.EPSILON) {
//                return random.nextInt(nrActions);
//            }
//        }
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
        rewardLock.writeLock().lock();
        accumulatedReward[selectedAction] += reward;
        ++nrTries[selectedAction];
        ++nrVisits;
        rewardLock.writeLock().unlock();
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
                int found = -1;
                if (!foundTable) {
                    for (int table : unjoinedTablesShuffled) {
                        if (!newlyJoined.contains(table)) {
                            found = table;
                            joinOrder[posCtr] = table;
                            newlyJoined.add(table);
                            foundTable = true;
                            break;
                        }
                    }
                }
                if (!foundTable) {
                    joinOrder[posCtr] = found;
                }
            }
        }
        else if (query.temporaryTables.size() > 0) {
            Set<Integer> newlyJoined = new HashSet<>(joinedTables);
            newlyJoined.add(lastTable);
            // Iterate over join order positions to fill
            List<Integer> unjoinedTablesShuffled = new ArrayList<>(unjoinedTables);
            Collections.shuffle(unjoinedTablesShuffled, ThreadLocalRandom.current());
            for (int posCtr = treeLevel + 1; posCtr < nrTables; ++posCtr) {
                boolean foundTable = false;
                for (int table : unjoinedTablesShuffled) {
                    if (!newlyJoined.contains(table) &&
                            (query.connected(newlyJoined, table))) {
                        joinOrder[posCtr] = table;
                        newlyJoined.add(table);
                        foundTable = true;
                        break;
                    }
                }
                int found = -1;
                if (!foundTable) {
                    for (int table : unjoinedTablesShuffled) {
                        if (!newlyJoined.contains(table)) {
                            found = table;
                            joinOrder[posCtr] = table;
                            newlyJoined.add(table);
                            foundTable = true;
                            break;
                        }
                    }
                }
                if (!foundTable) {
                    joinOrder[posCtr] = found;
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
//        System.arraycopy(new int[]{6, 11, 13, 12, 7, 2, 3, 4, 1, 9, 0, 8, 5, 10}, 0, joinOrder, 0, nrTables);
        int splitTable = getSplitTableByCard(joinOrder, joinOp.cardinalities);
        double reward = joinOp.execute(joinOrder, splitTable, (int) roundCtr);

        // Evaluate completed join order and return reward
        return reward;
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
            if (ParallelConfig.HEURISTIC_SHARING) {
                // Initialize table nodes
//                System.arraycopy(new int[]{6, 11, 13, 12, 7, 2, 3, 4, 1, 9, 0, 8, 5, 10}, 0, joinOrder, 0, nrTables);
                int splitTable = getSplitTableByCard(joinOrder, joinOp.cardinalities);
                double reward = joinOp.execute(joinOrder, splitTable, (int) roundCtr);
                return reward;
            }
            else {
                if (nrTables < 100) {
                    // Initialize table nodes
                    int splitTable = getSplitTableByCard(joinOrder, joinOp.cardinalities);
                    double reward = joinOp.execute(joinOrder, splitTable, (int) roundCtr);
                    return reward;
                }
                else {
                    int end = Math.min(5, joinOrder.length);
                    BaseUctInner root = tableRoot;
                    BaseUctNode splitNode = root.getMaxOrderedUCTChildOrder(joinOrder, end, tid);
                    if (splitNode != null) {
                        double reward = joinOp.execute(joinOrder, splitNode.label, (int) roundCtr);
                        if (!joinOp.isFinished()) {
                            splitNode.updataStatistics(reward, tid);
                            splitNode.parent.updataStatistics(reward, tid);
                        }
                        return reward;
                    }
                    else {
                        int splitTable = getSplitTableByCard(joinOrder, joinOp.cardinalities);
                        double reward = joinOp.execute(joinOrder, splitTable, (int) roundCtr);
                        return reward;
                    }
                }
            }
        } else {
            // inner node - select next action and expand tree if necessary
//            long actionStart = System.currentTimeMillis();
            int action = selectAction(policy, tid, joinOp);
//            long actionEnd = System.currentTimeMillis();
//            System.out.println(treeLevel + " action: " + (actionEnd - actionStart));
            int table = nextTable[action];
            joinOrder[treeLevel] = table;
            // grow tree if possible
            boolean canExpand = createdIn != roundCtr;
            DPNode child = childNodes[action];
            if (canExpand && child == null) {
                if (ParallelConfig.PARALLEL_SPEC == 1) {
                    nodeLock.lock();
                }
                if (childNodes[action] == null) {
                    childNodes[action] = new DPNode(roundCtr, this, table, joinOrder, joinOp.cardinalities);
                }
                if (ParallelConfig.PARALLEL_SPEC == 1) {
                    nodeLock.unlock();
                }
            }
            // evaluate via recursive invocation or via playout
            boolean isSample = child != null;
//            long sampleStart = System.currentTimeMillis();
            double reward = isSample ?
                    child.sample(roundCtr, joinOrder, joinOp, policy):
                    playout(roundCtr, joinOrder, joinOp);
//            long sampleEnd = System.currentTimeMillis();
//            if (!isSample) {
//                System.out.println(treeLevel + " playout: " + (sampleEnd - sampleStart));
//            }
            // update UCT statistics and return reward
//            reward = 0.01;
            if (ParallelConfig.PARALLEL_SPEC == 0 || ParallelConfig.PARALLEL_SPEC == 11
                    || ParallelConfig.PARALLEL_SPEC == 12 || ParallelConfig.PARALLEL_SPEC == 13) {
                if (treeLevel <= joinOp.deepIndex && !joinOp.lastState.isFinished()) {
                    nodeStatistics[tid].updateStatistics(reward, action);
                }
            }
            else {
                updateStatistics(action, reward);
            }
            return reward;
        }
    }

    public double sampleFinal(long roundCtr, int[] joinOrder, DPJoin joinOp,
                         Set<Integer> finishedTables, boolean[][] finishFlags) throws Exception {
        DPNode node = this;
        int tid = joinOp.tid;
        for (int i = 0; i < joinOrder.length; i++) {
            int table = joinOrder[i];
            int action = -1;
            for (int actionCtr = 0; actionCtr < node.nrActions; actionCtr++) {
                if (node.nextTable[actionCtr] == table) {
                    action = actionCtr;
                    break;
                }
            }
            if (action >= 0) {
                if (node.childNodes[action] == null) {
                    node.childNodes[action] = new DPNode(roundCtr, node, table, joinOrder, joinOp.cardinalities);
                }
                node = node.childNodes[action];
            }
        }
        BaseUctInner root = node.tableRoot;
        int end = Math.min(5, joinOrder.length);
        if (nrTables < 4) {
            int table = getSplitTableByCard(joinOrder, joinOp.cardinalities, finishedTables);
            double reward = joinOp.execute(joinOrder, table, (int) roundCtr, finishFlags, null);
            return reward;
        }
        else {
            BaseUctNode splitNode = root.getMaxOrderedUCTChildOrder(joinOrder, end, finishedTables, tid);
            if (splitNode != null) {
//            double reward = joinOp.execute(joinOrder, splitNode.label, (int) roundCtr);
                double reward = joinOp.execute(joinOrder, splitNode.label, (int) roundCtr, finishFlags, null);
                splitNode.updataStatistics(reward, tid);
                splitNode.parent.updataStatistics(reward, tid);
                return reward;
            }
            else {
                return -1;
            }
        }
    }



    /**
     * Get the split table candidate based on cardinalities of tables.
     *
     * @param joinOrder         join order
     * @param cardinalities     cardinalities of tables
     * @return
     */
    public int getSplitTableByCard(int[] joinOrder, int[] cardinalities) {
        if (nrThreads == 1) {
            return 0;
        }

//        if (joinOrder.length > 30) {
//            for (int i = 0; i < nrTables; i++) {
//                int table = joinOrder[i];
//                if (table < 40 && table % 4 > 1 || table == 43) {
//                    return table;
//                }
//            }
//        }
        int splitLen = 5;
        int splitSize = JoinConfig.AVOID_CARTESIAN ? ParallelConfig.PARTITION_SIZE : ParallelConfig.PARTITION_SIZE / 10;
//        int splitSize = 53000;
        int splitTable = joinOrder[0];
        if (ParallelConfig.PARALLEL_SPEC == 11) {
            return splitTable;
        }
        else if (ParallelConfig.PARALLEL_SPEC == 12) {
            int max = cardinalities[splitTable];
            for (int i = 1; i < nrTables; i++) {
                int table = joinOrder[i];
                int cardinality = cardinalities[table];
                if (cardinality > max && !query.temporaryTables.contains(table)) {
                    splitTable = table;
                }
            }
            return splitTable;
        }
        int start = nrTables <= splitLen + 1 ? 0 : 1;
        for (int i = start; i < nrTables; i++) {
            int table = joinOrder[i];
            int cardinality = cardinalities[table];
            if (cardinality >= splitSize && !query.temporaryTables.contains(table)) {
                splitTable = table;
                break;
            }
        }
        return splitTable;
    }

    public int getSplitTableByCard(int[] joinOrder, int[] cardinalities, Set<Integer> finishedTables) {
        if (nrThreads == 1) {
            return 0;
        }
        int splitLen = 5;
        int splitSize = ParallelConfig.PARTITION_SIZE;
//        int splitSize = nrThreads;
        int splitTable = -1;
        int end = Math.min(splitLen, joinOrder.length);
//        int start = nrTables <= splitLen + 1 ? 0 : 1;
        for (int i = 0; i < end; i++) {
            int table = joinOrder[i];
            int cardinality = cardinalities[table];
            if (cardinality >= splitSize && !finishedTables.contains(table) && !query.temporaryTables.contains(table)) {
                splitTable = table;
                break;
            }
        }
        return splitTable;
    }

    public long getSize() {
        long size = nodeStatistics.length * nrActions * 12 + 16 +
                (unjoinedTables.size() + joinedTables.size() + recommendedActions.size() + nextTable.length) * 4 ;
        for (DPNode node: childNodes) {
            if (node != null) {
                size += node.getSize();
            }
        }
        return size;
    }

}
