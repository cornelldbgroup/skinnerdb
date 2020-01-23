package search;

import java.util.List;

public abstract class MCTreeSearch<T extends Action> {
    protected SearchNode<T> root;
    protected Agent agent;

    public MCTreeSearch(SearchNode<T> root, Agent agent) {
        this.root = root;
        this.agent = agent;
    }

    public void execute() {
        List<Action> trace = sample(root);
        double reward = agent.simulate(trace);
        expandTree(root, trace);
        updateStatistics(root, trace, reward);
    }

    protected abstract List<Action> sample(SearchNode<T> root);

    protected abstract void expandTree(SearchNode<T> root, List<Action> trace);

    protected abstract void updateStatistics(SearchNode<T> root,
                                             List<Action> trace,
                                             double reward);
}
