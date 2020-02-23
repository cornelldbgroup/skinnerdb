package preprocessing.uct;

import operators.BudgetedFilter;
import uct.SelectionPolicy;
import uct.UCTNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;

public class RootNode extends UCTNode<FilterAction, BudgetedFilter> {
    private final int indexes;
    private final int predicates;
    private final int[] actionToPredicate;
    private final List<Integer> unchosenPreds;


    public RootNode(BudgetedFilter env, long roundCtr) {
        super(env, 1 + env.numPredicates() + env.numIndexes(), 0, roundCtr,
                SelectionPolicy.UCB1);
        this.predicates = env.numPredicates();
        this.indexes = env.numIndexes();
        this.actionToPredicate = new int[nrActions];

        for (int action = 1; action < 1 + predicates; action++) {
            this.actionToPredicate[action] = action - 1;
        }

        int pred = 0, action = predicates + 1;
        for (int i = 0; i < env.numIndexes(); i++) {
            if (env.getIndex(i) != null) {
                actionToPredicate[action] = pred;
                action++;
            }
            pred++;
        }

        this.unchosenPreds = new ArrayList<>();
        for (int i = 0; i < predicates; i++) {
            unchosenPreds.add(i);
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

    @Override
    protected double playout(FilterAction state, int budget) {
        if (state.type == FilterAction.ActionType.AVOID_BRANCHING) {
            return environment.execute(budget, state);
        } else if (state.type == FilterAction.ActionType.BRANCHING) {
            int lastPred = state.order[treeLevel];

            int posCtr = treeLevel + 1;
            Collections.shuffle(unchosenPreds);
            for (int pred : unchosenPreds) {
                if (pred == lastPred) continue;

                state.order[posCtr++] = pred;
            }

            return environment.execute(budget, state);
        } else { // state.type == FilterAction.ActionType.INDEX_SCAN
            return 0;
        }
    }
}
