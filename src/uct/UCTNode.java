package uct;

import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.eclipse.collections.impl.factory.primitive.IntLists;

import java.util.Random;
import java.util.stream.IntStream;

public abstract class UCTNode<T extends Action, E extends Environment<T>> {
    public final E environment;
    protected final UCTNode[] childNodes;
    private final int[] nrTries;
    private final double[] accumulatedReward;
    private final MutableIntList priorityActions;
    protected final int nrActions;
    public final int treeLevel;
    protected int nrVisits;
    private final long createdIn;
    protected final Random random;
    private final SelectionPolicy policy;
    private double EXPLORATION_FACTOR = 1e-5;

    public UCTNode(E env, int nrActions, int treeLevel,
                   long roundCtr, SelectionPolicy policy) {
        this.policy = policy;
        this.environment = env;
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
        this.createdIn = roundCtr;
        priorityActions = IntLists.mutable.ofAll(IntStream.range(0, nrActions));
    }

    private int selectAction(SelectionPolicy policy) {
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
                    quality = meanReward + EXPLORATION_FACTOR * exploration;
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
                        quality = meanReward + EXPLORATION_FACTOR * exploration;
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
            if (random.nextDouble() <= SelectionPolicy.EPSILON) {
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

    public double sample(long roundCtr, T action, int budget) {
        if (nrActions == 0) {
            return environment.execute(budget, action);
        }

        boolean canExpand = createdIn != roundCtr;
        int actionIndex = selectAction(policy);
        if (childNodes[actionIndex] == null && canExpand) {
            childNodes[actionIndex] = createChildNode(actionIndex, roundCtr);
        }

        updateActionState(action, actionIndex);

        UCTNode child = childNodes[actionIndex];
        double reward = (child != null) ?
                child.sample(roundCtr, action, budget) :
                playout(action, budget);
        updateStatistics(actionIndex, reward);
        return reward;
    }

    protected abstract double playout(T action, int budget);

    protected abstract UCTNode<T, E> createChildNode(int action, long roundCtr);

    protected abstract void updateActionState(T actionState, int action);
}
