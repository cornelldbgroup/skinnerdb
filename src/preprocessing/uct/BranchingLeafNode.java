package preprocessing.uct;

import operators.BudgetedFilter;
import uct.SelectionPolicy;
import uct.UCTNode;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

public class BranchingLeafNode extends UCTNode<FilterAction, BudgetedFilter>
        implements Compilable {
    private final List<Integer> chosenPreds;

    public BranchingLeafNode(BranchingNode parent,
                             int nextPred, long roundCtr) {
        super(parent.environment, 0, parent.treeLevel + 1, roundCtr,
                SelectionPolicy.UCB1);

        this.chosenPreds = new ArrayList<>();
        this.chosenPreds.addAll(parent.chosenPreds);
        this.chosenPreds.add(nextPred);
    }

    @Override
    public List<Integer> getPredicates() {
        return chosenPreds;
    }

    @Override
    public int getAddedUtility() {
        return nrVisits;
    }

    @Override
    public void addChildrenToCompile(PriorityQueue<Compilable> queue,
                                     int setSize) {}

    @Override
    protected double playout(FilterAction action, int budget) {
        throw new RuntimeException("0 Actions at this node");
    }

    @Override
    protected UCTNode<FilterAction, BudgetedFilter> createChildNode(
            int action, long roundCtr) {
        throw new RuntimeException("0 Actions at this node");
    }

    @Override
    protected void updateActionState(FilterAction actionState, int action) {
        throw new RuntimeException("0 Actions at this node");
    }
}
