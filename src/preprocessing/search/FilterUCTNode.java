package preprocessing.search;

import expressions.compilation.UnaryBoolEval;
import indexing.HashIndex;
import joining.uct.SelectionPolicy;
import uct.UCTNode;

import java.util.*;

public class FilterUCTNode extends UCTNode {

    private final long createdIn;
    private final int actionToPredicate[];
    private final FilterUCTNode[] childNodes;
    private final BudgetedFilter filterOp;

    private final List<Integer> chosenPreds;
    private final List<Integer> unchosenPreds;


    private final int indexActions;
    private final int branchingActions;
    private final int numPredicates;

    private final Map<List<Integer>, UnaryBoolEval> cache;

    public FilterUCTNode(BudgetedFilter filterOp,
                         Map<List<Integer>, UnaryBoolEval> cache,
                         long roundCtr,
                         int numPredicates, List<HashIndex> indices) {
        super(numPredicates + filterOp.nrIndexes() + 1, 0);
        this.cache = cache;

        this.indexActions = filterOp.nrIndexes();
        this.branchingActions = 1;

        this.createdIn = roundCtr;
        this.numPredicates = numPredicates;
        this.childNodes = new FilterUCTNode[nrActions];
        this.chosenPreds = new ArrayList<>();
        this.unchosenPreds = new ArrayList<>(numPredicates);
        this.actionToPredicate = new int[nrActions];

        int pred = 0, action = numPredicates;
        for (HashIndex index : indices) {
            if (index != null) {
                actionToPredicate[action] = pred;
                action++;
            }
            pred++;
        }

        for (int i = 0; i < numPredicates; ++i) {
            unchosenPreds.add(i);
            actionToPredicate[i] = i;
        }

        this.filterOp = filterOp;


    }

    public FilterUCTNode(FilterUCTNode parent, long roundCtr) {
        super(0, parent.treeLevel + 1);
        this.createdIn = roundCtr;
        this.numPredicates = parent.numPredicates;
        this.childNodes = new FilterUCTNode[0];
        this.chosenPreds = new ArrayList<>();
        this.unchosenPreds = new ArrayList<>();
        this.actionToPredicate = new int[0];
        this.filterOp = parent.filterOp;
        this.branchingActions = 0;
        this.indexActions = 0;
        this.cache = parent.cache;
    }

    public FilterUCTNode(FilterUCTNode parent, long roundCtr, int nextPred) {
        super(parent.nrActions - 1 - parent.branchingActions -
                parent.indexActions, parent.treeLevel + 1);
        this.cache = parent.cache;
        this.indexActions = 0;
        this.branchingActions = 0;
        this.createdIn = roundCtr;
        this.numPredicates = parent.numPredicates;
        this.childNodes = new FilterUCTNode[nrActions];

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

        this.filterOp = parent.filterOp;
    }


    public double sample(long roundCtr, FilterState state, int budget,
                         List<Integer> order) {
        if (nrActions == 0) {
            return filterOp.executeWithBudget(budget, state);
        }

        int action = selectAction(SelectionPolicy.UCB1);
        if (action == numPredicates + indexActions) {
            state.avoidBranching = true;
            state.useIndexScan = false;
            if (childNodes[action] == null) {
                childNodes[action] = new FilterUCTNode(this, roundCtr);
            }
        } else {
            int predicate = actionToPredicate[action];
            state.order[treeLevel] = predicate;
            order.add(predicate);
            if (cache.get(order) != null) {
                state.cachedTil = treeLevel;
                state.cachedEval = cache.get(order);
            }

            if (treeLevel == 0) {
                state.avoidBranching = false;
                state.useIndexScan = this.indexActions > 0 &&
                        action >= numPredicates && action < numPredicates +
                        indexActions;
            }

            boolean canExpand = createdIn != roundCtr;
            if (childNodes[action] == null && canExpand) {
                childNodes[action] = new FilterUCTNode(this, roundCtr,
                        predicate);
            }
        }

        FilterUCTNode child = childNodes[action];
        double reward = (child != null) ?
                child.sample(roundCtr, state, budget, order) :
                playout(state, budget);

        updateStatistics(action, reward);
        return reward;
    }

    private double playout(FilterState state, int budget) {
        int lastPred = state.order[treeLevel];

        Collections.shuffle(unchosenPreds);
        Iterator<Integer> unchosenPredsIter = unchosenPreds.iterator();
        for (int posCtr = treeLevel + 1; posCtr < numPredicates; ++posCtr) {
            int nextTable = unchosenPredsIter.next();
            while (nextTable == lastPred) {
                nextTable = unchosenPredsIter.next();
            }
            state.order[posCtr] = nextTable;
        }

        return filterOp.executeWithBudget(budget, state);
    }


    public List<Integer> getPreds() {
        return chosenPreds;
    }

    public void addChildrenToCompile(PriorityQueue<FilterUCTNode> compile,
                                     int compileSetSize) {
        if (this.treeLevel == 0) {
            for (int a = 0; a < Math.min(nrActions, numPredicates); ++a) {
                if (this.childNodes[a] != null) {
                    this.childNodes[a].addChildrenToCompile(compile,
                            compileSetSize);
                }
            }
            return;
        }


        for (int a = 0; a < nrActions; ++a) {
            if (this.childNodes[a] != null) {
                this.childNodes[a].addChildrenToCompile(compile,
                        compileSetSize);
                if (compile.size() >= compileSetSize) {
                    compile.poll();
                }
            }
        }
    }

    public int getAddedSavedCalls() {
        // -nrVisits*(chosenPreds.size() - 1) + nrVisits*(chosenPreds.size())
        return nrVisits;
    }
}
