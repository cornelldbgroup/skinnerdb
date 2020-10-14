package visualization;

import catalog.CatalogManager;
import joining.uct.UctNode;
import query.QueryInfo;

import java.util.Map;

public class JoinOrderLogger implements EpisodeDataConsumer {
    int iteration = 0;

    public void init(QueryInfo query) {
        for (int i = 0; i < query.nrJoined; i++) {
            System.err.println(query.aliasToTable.get(query.aliases[i]));;
        }
    }

    void print(UctNode node, int tab) {
        for (int i = 0; i < tab; i++) {
            System.err.print(" ");
        }
        if (!node.joinedTables.isEmpty()) {
            System.err.print(node.joinedTables.get(node.joinedTables.size() - 1));
            System.err.print(" ");
            System.err.print(node.createdIn);
        }
        System.err.println();

        for (int i = 0; i < node.childNodes.length; i++) {
            if (node.childNodes[i] != null) {
                print(node.childNodes[i], tab + 1);
            }
        }
    }

    public void finalPlan(UctNode node) {
        print(node, 0);
    }

    public void update(UctNode node) {}

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
