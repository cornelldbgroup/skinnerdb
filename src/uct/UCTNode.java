package uct;

import config.JoinConfig;
import joining.uct.SelectionPolicy;
import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.eclipse.collections.impl.factory.primitive.IntLists;
import config.SearchPreprocessorConfig;

import java.util.Random;
import java.util.stream.IntStream;

public abstract class UCTNode {
    private final UCTNode[] childNodes;
    private final int[] nrTries;
    private final double[] accumulatedReward;
    private final MutableIntList priorityActions;
    private final int nrActions;
    private final int treeLevel;
    private int nrVisits;
    final Random random;

    public UCTNode(int nrActions, int treeLevel) {
        this.random = new Random();
        this.treeLevel = treeLevel;
        this.nrActions = nrActions;
        this.nrTries = new int[nrActions];
        this.accumulatedReward = new double[nrActions];
        this.childNodes = new UCTNode[nrActions];
        for (int i = 0; i < nrActions; i++) {
            nrTries[i] = 0;
            accumulatedReward[i] = 0;
            childNodes[i] = null;
        }
        this.nrVisits = 0;

        priorityActions = IntLists.mutable.ofAll(IntStream.range(0, nrActions));
    }

    protected int selectAction(SelectionPolicy policy) {
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
                            SearchPreprocessorConfig.EXPLORATION_FACTOR * exploration;
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
                                SearchPreprocessorConfig.EXPLORATION_FACTOR *
                                        exploration;
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

    protected void updateStatistics(int selectedAction, double reward) {
        ++nrVisits;
        ++nrTries[selectedAction];
        accumulatedReward[selectedAction] += reward;
    }
}
