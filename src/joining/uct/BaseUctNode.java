package joining.uct;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Sequential UCT Node
 * it is used for experiments among different parallel versions
 */
public abstract class BaseUctNode {
    /**
     * Assigns each action index to child node.
     */
    public final List<BaseUctNode> children = new ArrayList<>();
    /**
     * Number of times this node was visited.
     */
    public int nrVisits = 1;
    /**
     * Parent node in UCT search tree.
     */
    public final BaseUctNode parent;
    /**
     * Accumulated reward over all visits so far.
     */
    public double accumulatedReward;
    /**
     * Split Table label
     */
    final int label;


    /**
     * Initialize concurrent UCT root node.
     *
     */
    public BaseUctNode(BaseUctNode parent, int label) {
        this.parent = parent;
        this.label = label;
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

    public BaseUctNode getChild(int label) {
        return children.stream().filter(child -> child.label == label).findFirst().orElse(null);
    }

    public void updataStatistics(double reward) {
        this.accumulatedReward += reward;
        this.nrVisits++;
    }
}
