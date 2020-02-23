package preprocessing.uct;

import operators.BudgetedFilter;
import uct.SelectionPolicy;
import uct.UCTNode;

public class FilterNode extends UCTNode<FilterAction, BudgetedFilter> {
    private final int indexes;
    private final int predicates;

    public FilterNode(BudgetedFilter env) {
        super(env, 1 + env.numPredicates() + env.numIndexes(), 0, 0,
                SelectionPolicy.UCB1);
        this.predicates = env.numPredicates();
        this.indexes = env.numIndexes();
    }

    @Override
    protected UCTNode<FilterAction, BudgetedFilter> createChildNode(
            int action, long roundCtr) {

        if (action == 0) {
            return new LeafNode(this, roundCtr);
        } else if (action >= 1 && action < 1 + predicates) {
            return null;
        } else { //action >= 1 + predicates && action < 1 + predicates + indexes
            return null;
        }
    }

    @Override
    protected void updateActionState(FilterAction actionState,
                                     int action) {

        if (action == 0) {
            actionState.type = FilterAction.ActionType.AVOID_BRANCHING;
        } else if (action >= 1 && action < 1 + predicates) {
            actionState.type = FilterAction.ActionType.INDEX_SCAN;
        } else { //action >= 1 + predicates && action < 1 + predicates + indexes
            actionState.type = FilterAction.ActionType.BRANCHING;
        }
    }
}
