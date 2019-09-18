package joining.uct;

import java.util.*;

public class BaseUctInner extends BaseUctNode {
    Map<Integer, BaseUctNode> childrenMap = new HashMap<>();
    /**
     * Initialize concurrent UCT root node.
     *
     * @param parent
     * @param label
     */
    public BaseUctInner(BaseUctNode parent, int label) {
        super(parent, label);
    }
    /**
     * Creates and adds a new UCT child node.
     *
     */
    public BaseUctNode expand(int label, boolean addLeaf) {
        BaseUctNode newNode = addLeaf ?
                new BaseUctLeaf(this, label):
                new BaseUctInner(this, label);
        children.add(newNode);
        childrenMap.put(label, newNode);
        return newNode;
    }

    public BaseUctNode expandFirst(int label, boolean addLeaf) {
        BaseUctNode newNode = addLeaf ?
                new BaseUctLeaf(this, label):
                new BaseUctInner(this, label);
        children.add(0, newNode);
        childrenMap.put(label, newNode);
        return newNode;
    }

    @Override
    public double sample() {
        double maxUCT = -1;
        BaseUctNode maxUCTchild = getMaxUCTChild();

        // Sample recursively
        double reward = maxUCTchild.sample();
        // Update statistics
        this.nrVisits += 1;
        this.accumulatedReward += reward;

        // Return newly added reward
        return reward;
    }

    public BaseUctNode getMaxUCTChild() {
        double maxUCT = -1;
        BaseUctNode maxUCTchild = null;
        List<Integer> permutation = permutatedIndex();
        // Iterate over child nodes in specified order
        for (int childIndex : permutation) {
            BaseUctNode child = children.get(childIndex);
            int childVisits = child.nrVisits;
            if (childVisits == 0) {
                maxUCT = Double.POSITIVE_INFINITY;
                maxUCTchild = child;
            } else {
                double childReward = child.accumulatedReward;
                int nrVisits = this.nrVisits;
                double uctValue = childReward + Math.sqrt(2 * Math.log(nrVisits) / childVisits);
                if (uctValue > maxUCT) {
                    maxUCT = uctValue;
                    maxUCTchild = child;
                }
            }
        }
        return maxUCTchild;
    }

    public BaseUctNode getMaxOrderedUCTChild() {
        double maxUCT = -1;
        BaseUctNode maxUCTchild = null;
        // Iterate over child nodes in specified order
        for (BaseUctNode child : children) {
            int childVisits = child.nrVisits;
            if (childVisits == 0) {
                maxUCTchild = child;
                return maxUCTchild;
            } else {
                double childReward = child.accumulatedReward;
                int nrVisits = this.nrVisits;
                double uctValue = childReward + 1E-6 * Math.sqrt(Math.log(nrVisits) / childVisits);
                if (uctValue > maxUCT) {
                    maxUCT = uctValue;
                    maxUCTchild = child;
                }
            }
        }

        return maxUCTchild;
    }

    public BaseUctNode getMaxOrderedUCTChildOrder(int[] joinOrder, int THRESHOLD) {
        double maxUCT = -1;
        BaseUctNode maxUCTchild = null;
        // Iterate over child nodes in specified order
        for (int i = 1; i < THRESHOLD; i++) {
            String table = String.valueOf(joinOrder[i]);
            if (childrenMap.containsKey(table)) {
                BaseUctNode child = childrenMap.get(table);
                int childVisits = child.nrVisits;
                if (childVisits == 0) {
                    maxUCTchild = child;
                    return maxUCTchild;
                } else {
                    double childReward = child.accumulatedReward / childVisits;
                    int nrVisits = this.nrVisits;
                    double uctValue = childReward + 1E-6 * Math.sqrt(Math.log(nrVisits) / childVisits);
                    if (uctValue > maxUCT) {
                        maxUCT = uctValue;
                        maxUCTchild = child;
                    }
                }
            }
        }
        return maxUCTchild;
    }

    public BaseUctNode getMaxOrderedExcludeUCTChild(int[] joinOrder, Set<Integer> finishTables, int THRESHOLD) {
        double maxUCT = -1;
        BaseUctNode maxUCTchild = null;
        // Iterate over child nodes in specified order
        for (int i = 1; i < THRESHOLD; i++) {
            int tableValue = joinOrder[i];
            String table = String.valueOf(tableValue);
            if (childrenMap.containsKey(table) && !finishTables.contains(tableValue)) {
                BaseUctNode child = childrenMap.get(table);
                int childVisits = child.nrVisits;
                if (childVisits == 0) {
                    maxUCTchild = child;
                    return maxUCTchild;
                } else {
                    double childReward = child.accumulatedReward / childVisits;
                    int nrVisits = this.nrVisits;
                    double uctValue = childReward + 1E-10 * Math.sqrt(2 * Math.log(nrVisits) / childVisits);
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

    private List<Integer> permutatedIndex() {
        // Determine random order in which child nodes are visited
        int nrChildren = children.size();
        List<Integer> permutation = new ArrayList<>();
        for (int i = 0; i < nrChildren; ++i) {
            permutation.add(i);
        }
        Collections.shuffle(permutation);
        return permutation;
    }
}
