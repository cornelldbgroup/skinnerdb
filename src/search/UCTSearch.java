package search;

import java.util.ArrayList;
import java.util.List;

public class UCTSearch<T extends Action> extends MCTreeSearch<T> {
    public UCTSearch(SearchNode<T> root, Agent agent) {
        super(root, agent);
    }

    @Override
    protected List<Action> sample(SearchNode<T> root) {
        SearchNode<T> node = root, parent = null;
        List<Action> actions = new ArrayList<>();
        while (node != null) {
            T action = node.selectAction();
            actions.add(action);
            parent = node;
            node = node.applyAction(action);
        }

        if (!parent.isLeafNode()) {
            actions.addAll(node.playout());
        }

        return actions;
    }

    @Override
    protected void expandTree(SearchNode<T> root, List<Action> trace) {
        SearchNode node = root;

        for (Action action : trace) {
            SearchNode next = node.applyAction(action);
            if (next == null) {
                node.expand(action);
                return;
            }
        }
    }

    @Override
    protected void updateStatistics(SearchNode<T> root, List<Action> trace,
                                    double reward) {
        SearchNode node = root;
        for (Action action : trace) {
            node.updateStatistics(reward);
            SearchNode next = node.applyAction(action);
            if (next == null) {
                return;
            }
        }
    }
}
