package joining.parallel.uct;

import config.JoinConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BaseUctInner extends BaseUctNode {
    /**
     * Initialize concurrent UCT root node.
     *
     * @param parent
     * @param label
     */
    public BaseUctInner(BaseUctNode parent, int label, int nrTables) {
        super(parent, label, nrTables);
    }
    /**
     * Creates and adds a new UCT child node.
     *
     */
    public BaseUctNode expand(int label, boolean addLeaf) {
        BaseUctNode newNode = addLeaf ?
                new BaseUctLeaf(this, label):
                new BaseUctInner(this, label, 0);
        children[label] = newNode;
        return newNode;
    }

    public BaseUctNode expandFirst(int label, boolean addLeaf) {
        BaseUctNode newNode = addLeaf ?
                new BaseUctLeaf(this, label):
                new BaseUctInner(this, label, 0);
        children[label] = newNode;
        return newNode;
    }

    @Override
    public double sample() {
        return 0;
    }


    public BaseUctNode getMaxOrderedUCTChildOrder(int[] joinOrder, int THRESHOLD) {
        double maxUCT = -1;
        BaseUctNode maxUCTchild = children[joinOrder[1]];
        // Iterate over child nodes in specified order
        for (int i = 1; i < THRESHOLD; i++) {
            int table = joinOrder[i];
            BaseUctNode child = children[table];
            if (child != null) {
                int childVisits = child.nrVisits;
                if (childVisits == 0) {
                    maxUCTchild = child;
                    return maxUCTchild;
                } else {
                    double childReward = child.accumulatedReward / childVisits;
                    int nrVisits = this.nrVisits;
                    double uctValue = childReward + JoinConfig.EXPLORATION_WEIGHT * Math.sqrt(Math.log(nrVisits) / childVisits);
//                    double uctValue = childReward;
                    if (uctValue > maxUCT) {
                        maxUCT = uctValue;
                        maxUCTchild = child;
                    }
                }
            }
        }
        return maxUCTchild;
    }


    @Override
    public BaseUctNode maxRewardChild() {
        double maxReward = -1;
        BaseUctNode maxRewardChild = null;
        for (BaseUctNode child : children) {
            int childVisits = child.nrVisits;
            double childReward = child.accumulatedReward;
            double reward = childVisits == 0 ? 0: childReward / childVisits;
            if (reward > maxReward) {
                maxReward = reward;
                maxRewardChild = child;
            }
        }
        return maxRewardChild;
    }

    @Override
    public String toString() {
        List<String> labels = new ArrayList<>();
        for (BaseUctNode child : children) {
            if (child == null) {
                labels.add("null");
            } else {
                labels.add(String.valueOf(child.nrVisits));
            }
        }
        return Arrays.toString(labels.toArray());
    }
}
