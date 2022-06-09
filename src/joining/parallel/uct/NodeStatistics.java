package joining.parallel.uct;

import java.util.Arrays;

/**
 * Represents node statistics in UCT search tree.
 *
 * @author Anonymous
 */
public class NodeStatistics {
    /**
     * Number of times this node was visited.
     */
    public int nrVisits;
    /**
     * Number of times each action was tried out.
     */
    public final int[] nrTries;
    /**
     * Reward accumulated for specific actions.
     */
    public final double[] accumulatedReward;
    /**
     * Number of update for each table.
     */
    public final int[] nrVisited;
    /**
     * Number of indexed size for each table.
     */
    public final int[] nrIndexed;
    /**
     * Progress for each action.
     */
    public final double[] progress;

    public NodeStatistics(int nrActions) {
        this.nrVisits = 1;
        this.nrTries = new int[nrActions];
        this.accumulatedReward = new double[nrActions];
        this.nrVisited = new int[nrActions];
        this.nrIndexed = new int[nrActions];
        this.progress = new double[nrActions];
    }

    /**
     * Update reward and visit statistics to given action.
     *
     * @param reward    action reward.
     * @param action    selected action.
     */
    public void updateStatistics(double reward, int action) {
        this.nrTries[action]++;
        this.nrVisits++;
        this.accumulatedReward[action] += reward;
    }

    public void updateStatistics(double reward, int nrVisited, int nrIndexed, int action) {
        this.nrTries[action]++;
        this.nrVisits++;
        this.accumulatedReward[action] += reward;
        this.nrVisited[action] += nrVisited;
        this.nrIndexed[action] += nrIndexed;
    }

    public void updateProgress(int action, double progress) {
        this.progress[action] = Math.max(this.progress[action], progress);
    }

    /**
     * Forget the statistics collected before.
     */
    public void clear() {
        Arrays.fill(nrTries, 0);
        Arrays.fill(accumulatedReward, 0);
        this.nrVisits = 1;
    }

}
