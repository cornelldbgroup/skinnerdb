package joining.parallel.uct;

import config.JoinConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class BaseUctInner extends BaseUctNode {
    /**
     * Initialize concurrent UCT root node.
     *
     * @param parent
     * @param label
     */
    public BaseUctInner(BaseUctNode parent, int label, int nrTables, int nrThreads) {
        super(parent, label, nrTables, nrThreads);
    }
    /**
     * Creates and adds a new UCT child node.
     *
     */
    public BaseUctNode expand(int label, boolean addLeaf, int nrThreads) {
        BaseUctNode newNode = addLeaf ?
                new BaseUctLeaf(this, label, nrThreads):
                new BaseUctInner(this, label, 0, nrThreads);
        children[label] = newNode;
        return newNode;
    }

    public BaseUctNode expandFirst(int label, boolean addLeaf, int nrThreads) {
        BaseUctNode newNode = addLeaf ?
                new BaseUctLeaf(this, label, nrThreads):
                new BaseUctInner(this, label, 0, nrThreads);
        children[label] = newNode;
        return newNode;
    }

    @Override
    public double sample() {
        return 0;
    }


    public BaseUctNode getMaxOrderedUCTChildOrder(int[] joinOrder, int THRESHOLD, int tid) {
        double maxUCT = -1;
        BaseUctNode maxUCTchild = children[joinOrder[1]];
        // Iterate over child nodes in specified order
//        int nrVisits = Arrays.stream(this.nrVisits).sum();
        int nrVisits = this.nrVisits[tid];
        for (int i = 0; i < THRESHOLD; i++) {
            int table = joinOrder[i];
            BaseUctNode child = children[table];
            if (child != null) {
//                int childVisits = Arrays.stream(child.nrVisits).sum();
                int childVisits = child.nrVisits[tid];
                if (childVisits == 0) {
                    maxUCTchild = child;
                    return maxUCTchild;
                } else {
//                    double childReward = Arrays.stream(child.accumulatedReward).sum() / childVisits;
                    double childReward = child.accumulatedReward[tid] / childVisits;
                    double uctValue = childReward +
                            JoinConfig.PARALLEL_WEIGHT * Math.sqrt(Math.log(nrVisits) / childVisits);
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

    public BaseUctNode getMaxOrderedUCTChildOrder(int[] joinOrder, int THRESHOLD,
                                                  Set<Integer> finishedTables, int tid) {
        double maxUCT = -1;
        BaseUctNode maxUCTchild = children[joinOrder[1]];
        int nrVisits = this.nrVisits[tid];
//        int nrVisits = Arrays.stream(this.nrVisits).sum();
        // Iterate over child nodes in specified order
        for (int i = 0; i < THRESHOLD; i++) {
            int table = joinOrder[i];
            BaseUctNode child = children[table];
            if (child != null && !finishedTables.contains(table)) {
//                int childVisits = Arrays.stream(child.nrVisits).sum();
                int childVisits = child.nrVisits[tid];
                if (childVisits == 0) {
                    maxUCTchild = child;
                    return maxUCTchild;
                } else {
//                    double childReward = Arrays.stream(child.accumulatedReward).sum() / childVisits;
                    double childReward = child.accumulatedReward[tid] / childVisits;
                    double uctValue = childReward +
                            JoinConfig.PARALLEL_WEIGHT * Math.sqrt(Math.log(nrVisits) / childVisits);
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
//        double maxReward = -1;
//        BaseUctNode maxRewardChild = null;
//        for (BaseUctNode child : children) {
//            int childVisits = child.nrVisits;
//            double childReward = child.accumulatedReward;
//            double reward = childVisits == 0 ? 0: childReward / childVisits;
//            if (reward > maxReward) {
//                maxReward = reward;
//                maxRewardChild = child;
//            }
//        }
//        return maxRewardChild;
        return null;
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
