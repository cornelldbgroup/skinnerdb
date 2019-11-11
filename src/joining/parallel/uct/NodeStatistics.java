package joining.parallel.uct;
/**
 * Represents node statistics in UCT search tree.
 *
 * @author Ziyun Wei
 */
public class NodeStatistics {
    /**
     * Number of times this node was visited.
     */
    public int nrVisits;
    /**
     * Number of times each action was tried out.
     */
    public int[] nrTries;
    /**
     * Reward accumulated for specific actions.
     */
    public double[] accumulatedReward;
    /**
     * encode number of visits
     */
    public long statsEncode;

    public NodeStatistics(int nrActions) {
        this.nrVisits = 1;
        this.nrTries = new int[nrActions];
        for (int i = 0; i < nrActions; i++) {
            this.nrTries[i] = 0;
        }
        this.accumulatedReward = new double[nrActions];
    }

    public void updateStatistics(double reward, int action) {
        this.nrTries[action]++;
        this.nrVisits++;
        this.accumulatedReward[action] += reward;
    }

}
