package joining.parallel.uct;

import java.util.Arrays;

/**
 * Sequential UCT Node
 * it is used for experiments among different joining.parallel versions
 */
public abstract class BaseUctNode {
    /**
     * Assigns each action index to child node.
     */
    public final BaseUctNode[] children;
    /**
     * Number of times this node was visited.
     */
    public int[] nrVisits;
    /**
     * Parent node in UCT search tree.
     */
    public final BaseUctNode parent;
    /**
     * Accumulated reward over all visits so far.
     */
    public double[] accumulatedReward;
    /**
     * Split Table label
     */
    final int label;


    /**
     * Initialize concurrent UCT root node.
     *
     */
    public BaseUctNode(BaseUctNode parent, int label, int nrTables, int nrThreads) {
        this.parent = parent;
        this.label = label;
        this.accumulatedReward = new double[nrThreads];
        this.nrVisits = new int[nrThreads];
        if (nrTables > 0)
            this.children = new BaseUctNode[nrTables];
        else
            this.children = new BaseUctNode[0];
    }


    /**
     * concurrently sample from concurrent UCT tree and return reward.
     *
     * @return achieved reward
     */
    public abstract double sample();

    /**
     * Returns a child node with maximal reward.
     *
     * @return	child node associated with maximal reward estimate
     */
    public abstract BaseUctNode maxRewardChild();

    /**
     * get average reward for a given node
     *
     * @return reward
     */
    public double getReward() {
        int nr = Arrays.stream(nrVisits).sum();

        return nr == 0 ? 0 : Arrays.stream(accumulatedReward).sum() / 0;
    }

    public int getLabel() {
        return label;
    }


    public void updataStatistics(double reward, int tid) {
        this.accumulatedReward[tid] += reward;
        this.nrVisits[tid]++;
    }
}
