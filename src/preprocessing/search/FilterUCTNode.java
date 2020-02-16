package preprocessing.search;

import config.JoinConfig;
import expressions.ExpressionInfo;
import expressions.compilation.UnaryBoolEval;
import indexing.HashIndex;
import joining.uct.SelectionPolicy;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import query.ColumnRef;

import java.util.*;

import static operators.Filter.compilePred;

public class FilterUCTNode {
    final Random random = new Random();

    private final long createdIn;
    private final int treeLevel, nrActions, numPredicates;
    private final int actionToPredicate[];
    private final FilterUCTNode[] childNodes;
    private final BudgetedFilter filterOp;

    private final Set<Integer> chosenPreds;
    private final List<Integer> unchosenPreds;

    private final int[] nrTries;
    private final double[] accumulatedReward;
    private int nrVisits;

    private List<Integer> priorityActions;

    private final int indexActions;
    private final int branchingActions;

    private UnaryBoolEval cachedEval;

    public FilterUCTNode(BudgetedFilter filterOp, long roundCtr,
                         int numPredicates, List<HashIndex> indices) {

        int indexActions = 0;
        for (HashIndex index : indices) {
            if (index != null) {
                indexActions++;
            }
        }
        this.indexActions = indexActions;
        this.branchingActions = 1;

        this.treeLevel = 0;
        this.createdIn = roundCtr;
        this.numPredicates = numPredicates;
        this.nrActions = numPredicates + indexActions + branchingActions;
        this.childNodes = new FilterUCTNode[nrActions];
        this.chosenPreds = new HashSet<>();
        this.unchosenPreds = new ArrayList<>(numPredicates);
        this.actionToPredicate = new int[nrActions];
        priorityActions = new ArrayList<>();

        int pred = 0, action = numPredicates;
        for (HashIndex index : indices) {
            if (index != null) {
                actionToPredicate[action] = pred;
                priorityActions.add(action);
                action++;
            }
            pred++;
        }

        for (int i = 0; i < numPredicates; ++i) {
            unchosenPreds.add(i);
            actionToPredicate[i] = i;
            priorityActions.add(i);
        }
        priorityActions.add(numPredicates + indexActions);

        this.filterOp = filterOp;

        this.nrVisits = 0;
        this.nrTries = new int[nrActions];
        this.accumulatedReward = new double[nrActions];
        for (int i = 0; i < nrActions; i++) {
            nrTries[i] = 0;
            accumulatedReward[i] = 0;
        }

        cachedEval = null;
    }

    public FilterUCTNode(FilterUCTNode parent, long roundCtr) {
        this.nrActions = 0;

        this.createdIn = roundCtr;
        this.treeLevel = parent.treeLevel + 1;
        this.numPredicates = parent.numPredicates;
        this.childNodes = new FilterUCTNode[0];
        this.chosenPreds = new HashSet<>();
        this.unchosenPreds = new ArrayList<>();
        this.actionToPredicate = new int[0];
        this.filterOp = parent.filterOp;
        this.nrTries = new int[0];
        this.accumulatedReward = new double[0];
        this.branchingActions = 0;
        this.indexActions = 0;
        cachedEval = null;
    }

    public FilterUCTNode(FilterUCTNode parent, long roundCtr, int nextPred) {
        this.indexActions = 0;
        this.branchingActions = 0;
        this.treeLevel = parent.treeLevel + 1;
        this.createdIn = roundCtr;
        this.numPredicates = parent.numPredicates;
        this.nrActions = parent.nrActions - 1 -
                parent.branchingActions - parent.indexActions;
        this.childNodes = new FilterUCTNode[nrActions];

        this.chosenPreds = new HashSet<>();
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

        this.nrVisits = 0;
        this.nrTries = new int[nrActions];
        this.accumulatedReward = new double[nrActions];
        for (int i = 0; i < nrActions; i++) {
            nrTries[i] = 0;
            accumulatedReward[i] = 0;
        }

        priorityActions = new ArrayList<>();
        for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
            priorityActions.add(actionCtr);
        }
        cachedEval = null;
    }

    int selectAction(SelectionPolicy policy) {
        if (!priorityActions.isEmpty()) {
            int nrUntried = priorityActions.size();
            int actionIndex = random.nextInt(nrUntried);
            int action = priorityActions.get(actionIndex);
            // Remove from untried actions and return
            priorityActions.remove(actionIndex);
            // System.out.println("Untried action: " + action);
            return action;
        }

        int offset = random.nextInt(nrActions);
        int bestAction = -1;
        double bestQuality = -1;
        for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
            // Calculate index of current action
            int action = (offset + actionCtr) % nrActions;
            double meanReward = accumulatedReward[action] / nrTries[action];
            double exploration =
                    Math.sqrt(Math.log(nrVisits) / nrTries[action]);
            // Assess the quality of the action according to policy
            double quality = -1;
            switch (policy) {
                case UCB1:
                    quality = meanReward +
                            FilterSearchConfig.EXPLORATION_FACTOR * exploration;
                    break;
                case MAX_REWARD:
                case EPSILON_GREEDY:
                    quality = meanReward;
                    break;
                case RANDOM:
                    quality = random.nextDouble();
                    break;
                case RANDOM_UCB1:
                    if (treeLevel == 0) {
                        quality = random.nextDouble();
                    } else {
                        quality = meanReward +
                                FilterSearchConfig.EXPLORATION_FACTOR * exploration;
                    }
                    break;
            }

            if (quality > bestQuality) {
                bestAction = action;
                bestQuality = quality;
            }
        }
        // For epsilon greedy, return random action with
        // probability epsilon.
        if (policy.equals(SelectionPolicy.EPSILON_GREEDY)) {
            if (random.nextDouble() <= JoinConfig.EPSILON) {
                return random.nextInt(nrActions);
            }
        }
        // Otherwise: return best action.
        return bestAction;
    }

    public double sample(long roundCtr, FilterState state, int budget,
                         ExpressionInfo unaryPred,
                         Map<ColumnRef, ColumnRef> colMap,
                         List<Expression> predicates) throws Exception {
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
            if (this.treeLevel == 1) {
                state.cachedTil = treeLevel;
                Expression expr = null;
                for (int i = 0; i <= treeLevel; i++) {
                    if (expr == null) {
                        expr = predicates.get(state.order[i]);
                    } else {
                        expr = new AndExpression(expr,
                                predicates.get(state.order[i]));
                    }
                }
                state.cachedEval = compilePred(unaryPred, expr, colMap);
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
                child.sample(roundCtr, state, budget, unaryPred, colMap,
                        predicates) :
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

    private void updateStatistics(int selectedAction, double reward) {
        ++nrVisits;
        ++nrTries[selectedAction];
        accumulatedReward[selectedAction] += reward;
    }

    public void setCompiled(UnaryBoolEval eval) {
        this.cachedEval = eval;
    }
}
