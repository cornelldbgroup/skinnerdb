package preprocessing.search;

import config.JoinConfig;
import joining.uct.SelectionPolicy;

import java.util.*;

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
    private final int[] accumulatedReward;
    private int nrVisits;

    private List<Integer> priorityActions;

    public FilterUCTNode(BudgetedFilter filterOp, long roundCtr,
                         int numPredicates) {
        this.treeLevel = 0;
        this.createdIn = roundCtr;
        this.numPredicates = numPredicates;
        this.nrActions = numPredicates;
        this.childNodes = new FilterUCTNode[nrActions];
        this.chosenPreds = new HashSet<>();
        this.unchosenPreds = new ArrayList<>(numPredicates);
        this.actionToPredicate = new int[numPredicates];

        for (int pred = 0; pred < numPredicates; ++pred) {
            unchosenPreds.add(pred);
            actionToPredicate[pred] = pred;
        }

        this.filterOp = filterOp;

        this.nrVisits = 0;
        this.nrTries = new int[nrActions];
        this.accumulatedReward = new int[nrActions];
        for (int i = 0; i < nrActions; i++) {
            nrTries[i] = 0;
            accumulatedReward[i] = 0;
        }

        // Sort untried actions by
        priorityActions = new ArrayList<>();
        for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
            priorityActions.add(actionCtr);
        }
    }

    public FilterUCTNode(FilterUCTNode parent, long roundCtr, int nextPred) {
        this.treeLevel = parent.treeLevel + 1;
        this.createdIn = roundCtr;
        this.numPredicates = parent.numPredicates;
        this.nrActions = parent.nrActions - 1;
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
        this.accumulatedReward = new int[nrActions];
        for (int i = 0; i < nrActions; i++) {
            nrTries[i] = 0;
            accumulatedReward[i] = 0;
        }

        priorityActions = new ArrayList<>();
        for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
            priorityActions.add(actionCtr);
        }
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
                            JoinConfig.EXPLORATION_WEIGHT * exploration;
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
                                JoinConfig.EXPLORATION_WEIGHT * exploration;
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

    public int sample(long roundCtr, int[] order) {
        if (nrActions == 0) {
            return filterOp.executeWithBudget(250, order);
        }

        int action = selectAction(SelectionPolicy.UCB1);
        int predicate = actionToPredicate[action];
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

        return filterOp.executeWithBudget(250, order);
    }

    void updateStatistics(int selectedAction, int reward) {
        ++nrVisits;
        ++nrTries[selectedAction];
        accumulatedReward[selectedAction] += reward;
    }
}
