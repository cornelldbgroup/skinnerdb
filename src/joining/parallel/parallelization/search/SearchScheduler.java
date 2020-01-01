package joining.parallel.parallelization.search;

import config.GeneralConfig;
import joining.parallel.join.SPJoin;
import joining.parallel.uct.SPNode;
import query.QueryInfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SearchScheduler {
    /**
     * A list of nodes to partition in the same level.
     */
    public final SPNode[] preNodes;
    /**
     * Number of times each action was tried out.
     */
    public final int[] nrTries;
    /**
     * Reward accumulated for specific actions.
     */
    public final double[] accumulatedReward;
    /**
     * Whether the search space is changed
     */
    public final boolean[] isChanged;
    /**
     * List of nodes assigned to each thread.
     */
    public List<List<Integer>> nodePerThread;
    /**
     * The level of partition nodes.
     */
    public final int treeLevel;
    /**
     * The number of threads.
     */
    public final int nrThreads;
    /**
     * List of join operators.
     */
    public final List<SPJoin> spJoins;
    /**
     * Number of adaptive count.
     */
    public int adaptiveCtr = 1;

    /**
     * Initialization of search scheduler
     *
     * @param query         query to process
     * @param root          root of UCT tree
     * @param spJoins       join operators
     * @param nrThreads     number of threads
     */
    public SearchScheduler(QueryInfo query, SPNode root, List<SPJoin> spJoins, int nrThreads) {
        // initialize nodes for the first level.
        int sid = -2;
        int MAX_SIZE = 20;
        List<SPNode> preNodes = new ArrayList<>();
        for (int i = 0 ; i < query.nrJoined; i++) {
            int action = root.prioritySet.get(i);
            int table = root.nextTable[action];
            SPNode children = new SPNode(0, root, table, action);
            children.sid = sid;
            sid--;
            root.childNodes[action] = children;
            preNodes.add(children);
        }
        root.prioritySet.clear();
        int level = 1;
        while (preNodes.size() < MAX_SIZE && level < query.nrJoined) {
            List<SPNode> nextLevel = new ArrayList<>();
            for (SPNode node : preNodes) {
                for (Integer action : node.prioritySet) {
                    int table = node.nextTable[action];
                    SPNode child = new SPNode(0, node, table, action);
                    child.sid = sid;
                    sid--;
                    node.childNodes[action] = child;
                    nextLevel.add(child);
                }
                node.prioritySet.clear();
            }
            preNodes = nextLevel;
            level++;
        }
        this.preNodes = new SPNode[preNodes.size()];
        for (int i = 0; i < preNodes.size(); i++) {
            SPNode candidate = preNodes.get(i);
            candidate.sid = i;
            this.preNodes[i] = preNodes.get(i);
        }
        this.treeLevel = level;
        this.spJoins = spJoins;
        this.nrThreads = nrThreads;
        this.nrTries = new int[preNodes.size()];
        this.accumulatedReward = new double[preNodes.size()];
        this.isChanged = new boolean[nrThreads];

        // distribute nodes to threads
        this.nodePerThread = partitionNodes(preNodes.size(), nrThreads);
        for (int i = 0; i < nrThreads; i++) {
            SPJoin spJoin = spJoins.get(i);
            spJoin.initSearchRoot(this.preNodes, nodePerThread.get(i), treeLevel);
        }
    }

    private static List<List<Integer>> partitionNodes(int nrNodes, int nrThreads) {
        int base = nrNodes / nrThreads;
        int[] nrs = new int[nrThreads];
        Arrays.fill(nrs, base);
        int remaining = nrNodes - base * nrThreads;
        int tid = 0;
        while (remaining > 0) {
            nrs[tid]++;
            tid++;
            remaining--;
        }
        List<List<Integer>> nodePerThread = new ArrayList<>(nrThreads);
        int start = 0;
        for (int i = 0; i < nrThreads; i++) {
            List<Integer> nodes = new ArrayList<>(nrs[i]);
            for (int index = start; index < start + nrs[i]; index++) {
                nodes.add(index);
            }
            nodePerThread.add(nodes);
            start += nrs[i];
            System.out.println(i + ": " + Arrays.toString(nodes.toArray()));
        }
        return nodePerThread;
    }

    /**
     * given the number of weights for different nodes,
     * allocate several nodes to different threads
     *
     * @param spaceSizes a list of number of weights for different nodes
     * @param nrThreads  the number of thread
     * @return           allocation decision
     */
    private static List<List<Integer>> partitionNodes(List<Double> spaceSizes, int nrThreads) {
        List<Integer> sortedIndices = IntStream.range(0, spaceSizes.size())
                .boxed().sorted(Comparator.comparing(spaceSizes::get)).mapToInt(ele -> ele)
                .boxed().collect(Collectors.toList());

        List<List<Integer>> nodePerThread = new ArrayList<>();
        List<Double> ordersPerThread = new ArrayList<>();
        for (int i = 0; i < nrThreads; i++) {
            nodePerThread.add(new ArrayList<>());
            ordersPerThread.add(-1.0);
        }
        int indexSize = sortedIndices.size();
        while (indexSize > 0) {
            int index = sortedIndices.get(indexSize - 1);
            double newWeight = spaceSizes.get(index);
            double minNum = Double.MAX_VALUE;
            int minThread = -1;
            for (int i = 0; i < nrThreads; i++) {
                double size = ordersPerThread.get(i);
                if (size < 0) {
                    minThread = i;
                    minNum = size;
                    break;
                }
                else if (size < minNum) {
                    minThread = i;
                    minNum = size;
                }
                else if (size == minNum && nodePerThread.get(i).size() < nodePerThread.get(minThread).size()) {
                    minThread = i;
                    minNum = size;
                }

            }
            nodePerThread.get(minThread).add(index);
            if (minNum < 0) {
                ordersPerThread.set(minThread, newWeight);
            }
            else {
                ordersPerThread.set(minThread, minNum + newWeight);
            }
            sortedIndices.remove(indexSize - 1);
            indexSize--;
        }
        for (int i = 0; i < nrThreads; i++) {
            System.out.println(i + ": " + Arrays.toString(nodePerThread.get(i).toArray()));
        }
        return nodePerThread;
    }

    /**
     * update statistics for each candidate
     *
     * @param action    action number
     * @param reward    reward gain
     */
    public void updateStatistics(int action, double reward) {
        accumulatedReward[action] += reward;
        nrTries[action]++;
    }

    /**
     * Reallocate nodes to threads based on reward statistics.
     */
    public void redistribution() {
        // a list of weights
        List<Double> weights = getRewardWeights();
        // normalized rewards
        double totalWeights = weights.stream().mapToDouble(w -> Math.pow(w, 2)).sum();
        if (totalWeights < Double.MIN_VALUE) {
            weights = weights.stream().map(w -> 1.0 / nrThreads).collect(Collectors.toList());
        }
        else {
            weights = weights.stream().map(w -> Math.pow(w, 2) / totalWeights).collect(Collectors.toList());
        }
        // partition search space based on new rewards
        this.nodePerThread = partitionNodes(weights, nrThreads);
        // activate change flag
        Arrays.fill(isChanged, true);
        adaptiveCtr++;

    }

    /**
     * Calculate average weights for each node.
     *
     * @return  a list of reward.
     */
    private List<Double> getRewardWeights() {
        List<Double> doubleWeights = new ArrayList<>();
        int nrCandidates = accumulatedReward.length;
        for (int i = 0; i < nrCandidates; i++) {
            double reward = nrTries[i] == 0 ? 0: accumulatedReward[i] / nrTries[i];
//            double reward = accumulatedReward[i];
//            reward = Math.max(reward, 10E-30);
            doubleWeights.add(reward);
        }
        return doubleWeights;
    }

}
