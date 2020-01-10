package joining.parallel.uct;

import config.ParallelConfig;
import joining.uct.SelectionPolicy;
import joining.parallel.join.DPJoin;
import query.QueryInfo;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Represents node in data joining.parallel UCT search tree.
 *
 * @author Ziyun Wei
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
    /**
     * Numbre of threads.
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
    public DPNode(long roundCtr, DPNode parent, int joinedTable, int[] joinOrder, int[] cardinalities) {
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
                    int table = nextTable[actionCtr];
                    recommendedActions.add(actionCtr);
                }
            }
        } // if heuristic is used
        else {
            recommendedActions = null;
        }

        List<Integer> priorityActions = new ArrayList<>();
        for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
            if (!useHeuristic || recommendedActions.contains(actionCtr)) {
                priorityActions.add(actionCtr);
            }
        }
        this.prioritySet = new LinkedList[nrThreads];
        for (int i = 0; i < nrThreads; i++) {
            prioritySet[i] = new LinkedList<>(priorityActions);
        }

        if (nrActions == 0) {
            // initialize tree node
            tableRoot =  new BaseUctInner(null, -1, nrTables);
            int end = Math.min(5, joinOrder.length);
            for (int i = 0; i < end; i++) {
                int table = joinOrder[i];
                if (cardinalities[table] >= ParallelConfig.PARTITION_SIZE) {
                    tableRoot.expand(table, true);
                }
            }
//            DPNode node = parent;
//            BaseUctNode child = null;
//            DPNode firstNode = null;
//            while (node.parent != null) {
//                int table = joinOrder[node.treeLevel - 1];
//                if (cardinalities[table] >= ParallelConfig.PARTITION_SIZE && node.treeLevel <= 5 && node.treeLevel > 1) {
//                    child = tableRoot.expandFirst(table, true);
//                    firstNode = node;
//                }
//                node = node.parent;
//            }
            // retrieve rewards
//            if (firstNode != null) {
//                DPNode nodeParent = firstNode.parent;
//                int firstTable = joinOrder[firstNode.treeLevel - 1];
//                int action = -1;
//                for (int i = 0; i < nodeParent.nextTable.length; i++) {
//                    if (nodeParent.nextTable[i] == firstTable) {
//                        action = i;
//                        break;
//                    }
//                }
//                double rewards = 0;
//                int visits = 0;
//                for (int t = 0; t < nrThreads; t++) {
//                    NodeStatistics threadStats = nodeParent.nodeStatistics[t];
//                    visits += threadStats.nrTries[action];
//                    rewards += threadStats.accumulatedReward[action];
//                }
//                child.accumulatedReward = rewards;
//                tableRoot.accumulatedReward = rewards;
//                child.nrVisits = visits;
//                tableRoot.nrVisits = visits;
//            }
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
        if (!prioritySet[tid].isEmpty()) {
            priorAction = prioritySet[tid].pollFirst();
        }
        if (priorAction != null) {
            return priorAction;
        }
        int nrVisits = 0;
        int[] nrTries = new int[nrActions];
        double[] accumulatedReward = new double[nrActions];
        if (ParallelConfig.PARALLEL_SPEC == 0) {
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
//            int action = (offset + actionCtr) % nrActions;
//            if (useHeuristic && !recommendedActions.contains(action))
//                continue;

            int nrTry = nrTries[action];
            if (nrTry == 0) {
                return action;
            }
            double meanReward = accumulatedReward[action] / nrTry;
            double exploration = Math.sqrt(Math.log(nrVisits) / nrTry);
            // Assess the quality of the action according to policy
            double quality = meanReward + 1E-10 * exploration;
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
        if (bestAction == -1) {
            System.out.println("here");
        }
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
                            if (!query.temporaryTables.contains(table)) {
                                joinOrder[posCtr] = table;
                                newlyJoined.add(table);
                                foundTable = true;
                                break;
                            }
                        }
                    }
                }
                if (!foundTable) {
                    joinOrder[posCtr] = found;
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
            int end = Math.min(5, joinOrder.length);
            // Initialize table nodes
            int splitTable = getSplitTableByCard(joinOrder, joinOp.cardinalities);
            double reward = joinOp.execute(joinOrder, splitTable, (int) roundCtr);
            return reward;
//            BaseUctInner root = tableRoot;
//            BaseUctNode splitNode = root.getMaxOrderedUCTChildOrder(joinOrder, end);
//            if (splitNode != null) {
//                double reward = joinOp.execute(joinOrder, splitNode.label, (int) roundCtr);
//                splitNode.updataStatistics(reward);
//                splitNode.parent.updataStatistics(reward);
//                return reward;
//            }
//            else {
//                System.out.println(Arrays.toString(joinOrder));
//                System.out.println(root);
//                throw new RuntimeException("not found table");
//            }
        } else {
            // inner node - select next action and expand tree if necessary
            int action = selectAction(policy, tid, joinOp);
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
            double reward = isSample ?
                    child.sample(roundCtr, joinOrder, joinOp, policy):
                    playout(roundCtr, joinOrder, joinOp);
            // update UCT statistics and return reward
            if (ParallelConfig.PARALLEL_SPEC == 0) {
                nodeStatistics[tid].updateStatistics(reward, action);
            }
            else {
                updateStatistics(action, reward);
            }
            return reward;
        }
    }

    public double sampleFinal(long roundCtr, int[] joinOrder, DPJoin joinOp,
                         SelectionPolicy policy) throws Exception {
        int tid = joinOp.tid;
        // Check if this is a (non-extendible) leaf node
        if (nrActions == 0) {
            int end = Math.min(5, joinOrder.length);
            // Initialize table nodes
            BaseUctInner root = tableRoot;
            BaseUctNode splitNode = root.getMaxOrderedUCTChildOrder(joinOrder, end);
            if (splitNode != null) {
                double reward = joinOp.execute(joinOrder, splitNode.label, (int) roundCtr);
                if (joinOp.isFinished()) {
                    reward = 0;
                }
                splitNode.updataStatistics(reward);
                splitNode.parent.updataStatistics(reward);
                return reward;
            }
            else {
                int splitTable = getSplitTableByCard(joinOrder, joinOp.cardinalities);
                double reward = joinOp.execute(joinOrder, splitTable, (int) roundCtr);
                return reward;
            }
        } else {
            // inner node - select next action and expand tree if necessary
            int table = joinOrder[treeLevel];
            int action = -1;
            for (int i = 0; i < nextTable.length; i++) {
                if (nextTable[i] == table) {
                    action = i;
                    break;
                }
            }
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
            double reward = isSample ?
                    child.sampleFinal(roundCtr, joinOrder, joinOp, policy):
                    playout(roundCtr, joinOrder, joinOp);
            // update UCT statistics and return reward
            if (ParallelConfig.PARALLEL_SPEC == 0) {
                nodeStatistics[tid].updateStatistics(reward, action);
            }
            else {
                updateStatistics(action, reward);
            }
            return reward;
        }
    }



    /**
     * Get the split table candidate based on cardinalities of tables.
     *
     * @param joinOrder         join order
     * @param cardinalities     cardinalities of tables
     * @return
     */
    int getSplitTableByCard(int[] joinOrder, int[] cardinalities) {
        int splitLen = 5;
        int splitSize = ParallelConfig.PARTITION_SIZE;
        int splitTable = joinOrder[0];
        int end = Math.min(splitLen, joinOrder.length);
        for (int i = 0; i < end; i++) {
            int table = joinOrder[i];
            int cardinality = cardinalities[table];
            if (cardinality >= splitSize) {
                splitTable = table;
                break;
            }
        }
        return splitTable;
    }
}
