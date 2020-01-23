package search;

import java.util.Collection;

public abstract class SearchNode<T extends Action> {
    private int numVisits;
    private double accumulatedReward;

    public SearchNode() {

    }

    public void updateStatistics(double reward) {
        numVisits++;
        accumulatedReward += reward;
    }

    public abstract boolean isLeafNode();
    public abstract T selectAction();
    public abstract SearchNode applyAction(T action);
    public abstract void expand(T action);
    public abstract Collection<T> playout();
}
