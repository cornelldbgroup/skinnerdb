package joining.parallel.uct;

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
    public int nrVisits = 0;
    /**
     * Parent node in UCT search tree.
     */
    public final BaseUctNode parent;
    /**
     * Accumulated reward over all visits so far.
     */
    public double accumulatedReward = 0;
    /**
     * Split Table label
     */
    final int label;


    /**
     * Initialize concurrent UCT root node.
     *
     */
    public BaseUctNode(BaseUctNode parent, int label, int nrTables) {
        this.parent = parent;
        this.label = label;
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
        return accumulatedReward / nrVisits;
    }

    public int getLabel() {
        return label;
    }


    public void updataStatistics(double reward) {
        this.accumulatedReward += reward;
        this.nrVisits++;
    }
}
