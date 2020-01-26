package preprocessing.search;

import java.util.*;

public class FilterUCTNode {
    private final long createdIn;
    private final int treeLevel, nrActions, numPredicates;
    private final int nextPredicate[];
    private final FilterUCTNode[] childNodes;
    private final BudgetedFilter filterOp;

    private final Set<Integer> chosenPreds;
    private final List<Integer> unchosenPreds;

    public FilterUCTNode(BudgetedFilter filterOp, long roundCtr,
                         int numPredicates) {
        this.treeLevel = 0;
        this.createdIn = roundCtr;
        this.nrActions = numPredicates;
        this.childNodes = new FilterUCTNode[nrActions];
        this.chosenPreds = new HashSet<>();
        this.unchosenPreds = new ArrayList<>(numPredicates);
        this.nextPredicate = new int[numPredicates];
        this.numPredicates = numPredicates;

        for (int pred = 0; pred < numPredicates; ++pred) {
            unchosenPreds.add(pred);
            nextPredicate[pred] = pred;
        }

        this.filterOp = filterOp;
    }

    public FilterUCTNode(FilterUCTNode parent, long roundCtr, int nextPred) {
        this.treeLevel = parent.treeLevel + 1;
        this.createdIn = roundCtr;
        this.nrActions = parent.nrActions - 1;
        this.childNodes = new FilterUCTNode[nrActions];

        this.chosenPreds = new HashSet<>();
        this.chosenPreds.addAll(parent.chosenPreds);
        this.chosenPreds.add(nextPred);

        this.unchosenPreds = new ArrayList<>();
        this.unchosenPreds.addAll(parent.unchosenPreds);
        int indexToRemove = unchosenPreds.indexOf(nextPred);
        this.unchosenPreds.remove(indexToRemove);

        this.nextPredicate = new int[nrActions];
        this.numPredicates = parent.numPredicates;

        for (int action = 0; action < nrActions; ++action) {
            nextPredicate[action] = unchosenPreds.get(action);
        }

        this.filterOp = parent.filterOp;
    }

    private int playout(int[] order) {
        int lastPred = order[treeLevel];

        Collections.shuffle(unchosenPreds);
        Iterator<Integer> unchosenPredsIter = unchosenPreds.iterator();
        for (int posCtr = treeLevel + 1; posCtr < numPredicates; ++posCtr) {
            int nextTable = unchosenPredsIter.next();
            while (nextTable == lastPred) {
                nextTable = unchosenPredsIter.next();
            }
            order[posCtr] = nextTable;
        }

        return filterOp.executeWithBudget(1000, order);
    }

    public int sample(long roundCtr, int[] order) {
        if (treeLevel == numPredicates - 1) {
            return filterOp.executeWithBudget(1000, order);
        }

        int action = selectAction();
        int predicate = nextPredicate[action];
        order[treeLevel] = predicate;
        boolean canExpand = createdIn != roundCtr;
        if (childNodes[action] == null && canExpand) {
            childNodes[action] = new FilterUCTNode(this, roundCtr, predicate);
        }

        FilterUCTNode child = childNodes[action];
        int reward = (child != null) ?
                child.sample(roundCtr, order) :
                playout(order);

        updateStatistics(action, reward);
        return reward;
    }
}
