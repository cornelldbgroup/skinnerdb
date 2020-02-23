package preprocessing.uct;

import expressions.compilation.UnaryBoolEval;
import operators.BudgetedFilter;
import uct.SelectionPolicy;
import uct.UCTNode;

import java.util.*;

public class BranchingNode extends UCTNode<FilterAction, BudgetedFilter>
        implements Compilable {
    private final int[] actionToPredicate;
    public final List<Integer> chosenPreds;
    private final List<Integer> unchosenPreds;

    public BranchingNode(RootNode root, int nextPred, long roundCtr) {
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
            return new BranchingLeafNode(this, actionToPredicate[action],
                    roundCtr);
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

        UnaryBoolEval eval = environment.compileCache.get(unchosenPreds);
        if (eval != null) {
            state.cachedTil = treeLevel;
            state.cachedEval = eval;
        }
    }

    public void addChildrenToCompile(PriorityQueue<Compilable> compile,
                                     int compileSetSize) {
        for (int a = 0; a < nrActions; ++a) {
            if (this.childNodes[a] != null) {
                compile.add((Compilable) this.childNodes[a]);
                if (compile.size() >= compileSetSize) {
                    compile.poll();
                }
            }
        }
    }

    @Override
    public List<Integer> getPredicates() {
        return chosenPreds;
    }

    @Override
    public int getAddedUtility() {
        // -nrVisits*(chosenPreds.size() - 1) + nrVisits*(chosenPreds.size())
        return nrVisits;
    }
}
