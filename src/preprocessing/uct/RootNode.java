package preprocessing.uct;

import operators.BudgetedFilter;
import uct.SelectionPolicy;
import uct.UCTNode;

import java.util.PriorityQueue;

public class RootNode extends UCTNode<FilterAction, BudgetedFilter> {
    private final int indexes;
    private final int predicates;
    private final int[] actionToPredicate;


    public RootNode(BudgetedFilter env) {
        super(env, 1 + env.numPredicates() + env.numIndexes(), 0, 0,
                SelectionPolicy.UCB1);
        this.predicates = env.numPredicates();
        this.indexes = env.numIndexes();
        this.actionToPredicate = new int[nrActions];

        int index = 0;
        for (int action = 1; action < nrActions; action++) {
            if (action < 1 + predicates) {
                this.actionToPredicate[action] = action - 1;
            } else {
                while (environment.getIndex(index) == null) {
                    index++;
                }
                this.actionToPredicate[action] = index;
            }
        }
    }

    @Override
    protected UCTNode<FilterAction, BudgetedFilter> createChildNode(
            int action, long roundCtr) {

        if (action == 0) {
            return new AvoidBranchingNode(this, roundCtr);
        } else if (action >= 1 && action < 1 + predicates) {
            return new BranchingNode(this, actionToPredicate[action], roundCtr);
        } else { //action >= 1 + predicates && action < 1 + predicates + indexes
            return null;
        }
    }

    @Override
    protected void updateActionState(FilterAction state, int action) {

        if (action == 0) {
            state.type = FilterAction.ActionType.AVOID_BRANCHING;
        } else if (action >= 1 && action < 1 + predicates) {
            state.type = FilterAction.ActionType.BRANCHING;
            int predicate = actionToPredicate[action];
            state.order[treeLevel] = predicate;
        } else { //action >= 1 + predicates && action < 1 + predicates + indexes
            state.type = FilterAction.ActionType.INDEX_SCAN;
            int predicate = actionToPredicate[action];
            state.order[treeLevel] = predicate;
        }
    }

    public void populateInitialSet(PriorityQueue<Compilable> compile,
                                   int compileSetSize) {
        for (int a = 1; a < 1 + predicates; ++a) {
            if (this.childNodes[a] != null) {
                ((Compilable) this.childNodes[a]).addChildrenToCompile(compile,
                        compileSetSize);
            }
        }
    }
}
