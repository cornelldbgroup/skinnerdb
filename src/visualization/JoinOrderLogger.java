package visualization;

import catalog.CatalogManager;
import joining.uct.UctNode;
import query.QueryInfo;

import java.util.Map;

public class JoinOrderLogger implements EpisodeDataConsumer {
    int iteration = 0;
    String[] names;

    public void init(QueryInfo query) {
        names = new String[query.nrJoined];
        for (int i = 0; i < query.nrJoined; i++) {
            names[i] = query.aliasToTable.get(query.aliases[i]);
        }
    }

    void print(UctNode node, int tab) {
        for (int i = 0; i < tab; i++) {
            System.err.print(" ");
        }
        if (!node.joinedTables.isEmpty()) {
            System.err.print(names[node.joinedTables.get(node.joinedTables.size() - 1)]);
        }
        System.err.println();

        for (int i = 0; i < node.childNodes.length; i++) {
            if (node.childNodes[i] != null) {
                print(node.childNodes[i], tab + 1);
            }
        }
    }

    public void update(UctNode node) {
        System.err.println("Iteration " + iteration);
        print(node, 0);
        System.err.println();
        iteration++;
    }

    public void update(int[] joinOrder, double reward, int[] tupleIndices,
                       int[] tableCardinality) {
        System.err.print(reward);
        for (int i = 0; i < joinOrder.length; i++) {
            System.err.print(" ");
            System.err.print(joinOrder[i]);
        }
        System.err.print(" ;");
        for (int i = 0; i < tupleIndices.length; i++) {
            System.err.print(" ");
            System.err.print(tupleIndices[i]);
        }
        System.err.println();
    }
}
