package preprocessing.uct;

import operators.BudgetedFilter;
import uct.SelectionPolicy;
import uct.UCTNode;

public class LeafNode extends UCTNode<FilterAction, BudgetedFilter> {
    public LeafNode(UCTNode<FilterAction, BudgetedFilter> parent,
                    long roundCtr) {
        super(parent.environment, 0, parent.treeLevel + 1, roundCtr,
                SelectionPolicy.UCB1);
    }
}
