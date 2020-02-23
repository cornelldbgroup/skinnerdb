package preprocessing.uct;

import operators.BudgetedFilter;
import uct.SelectionPolicy;
import uct.UCTNode;

public class AvoidBranchingNode extends UCTNode<FilterAction, BudgetedFilter> {
    public AvoidBranchingNode(UCTNode<FilterAction, BudgetedFilter> parent,
                              long roundCtr) {
        super(parent.environment, 0, parent.treeLevel + 1, roundCtr,
                SelectionPolicy.UCB1);
    }

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
