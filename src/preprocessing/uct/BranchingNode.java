package preprocessing.uct;

import operators.BudgetedFilter;
import uct.SelectionPolicy;
import uct.UCTNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class BranchingNode extends UCTNode<FilterAction, BudgetedFilter> {
    private final int[] actionToPredicate;
    private final List<Integer> chosenPreds;
    private final List<Integer> unchosenPreds;

    public BranchingNode(FilterNode root, int nextPred, long roundCtr) {
        super(root.environment, root.environment.numPredicates() - 1,
                root.treeLevel + 1, roundCtr, SelectionPolicy.UCB1);
        this.chosenPreds = Arrays.asList(nextPred);
        this.unchosenPreds = new ArrayList<>();
        this.actionToPredicate = new int[nrActions];
        for (int i = 0, j = 0; i < nrActions + 1; i++) {
            if (i != nextPred) {
                unchosenPreds.add(i);
                actionToPredicate[j++] = i;
            }
        }
    }

    public BranchingNode(BranchingNode parent, int nextPred, long roundCtr) {
        super(parent.environment, parent.nrActions - 1, parent.treeLevel + 1,
                roundCtr, SelectionPolicy.UCB1);

        this.chosenPreds = new ArrayList<>();
        this.chosenPreds.addAll(parent.chosenPreds);
        this.chosenPreds.add(nextPred);

        this.unchosenPreds = new ArrayList<>();
        this.unchosenPreds.addAll(parent.unchosenPreds);
        int indexToRemove = unchosenPreds.indexOf(nextPred);
        this.unchosenPreds.remove(indexToRemove);

        this.actionToPredicate = new int[nrActions];
        for (int action = 0; action < nrActions; ++action) {
            actionToPredicate[action] = unchosenPreds.get(action);
        }
    }

    @Override
    protected UCTNode<FilterAction, BudgetedFilter> createChildNode(
            int action, long roundCtr) {
        if (nrActions == 1) {
            return new LeafNode(this, roundCtr);
        }

        return new BranchingNode(this, actionToPredicate[action], roundCtr);
    }

    @Override
    protected double playout(FilterAction state, int budget) {
        int lastPred = state.order[treeLevel];

        int posCtr = treeLevel + 1;
        Collections.shuffle(unchosenPreds);
        for (int pred : unchosenPreds) {
            if (pred == lastPred) continue;

            state.order[posCtr++] = pred;
        }

        return environment.execute(budget, state);
    }

    @Override
    protected void updateActionState(FilterAction state, int action) {
        int predicate = actionToPredicate[action];
        state.order[treeLevel] = predicate;
    }
}
