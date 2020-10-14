package visualization;

import catalog.CatalogManager;
import query.QueryInfo;

import java.util.Map;

public class JoinOrderLogger implements EpisodeDataConsumer {
    public void init(QueryInfo query) {
        for (int i = 0; i < query.nrJoined; i++) {
            System.err.println(query.aliasToTable.get(query.aliases[i]));
        }
    }

    public void update(int[] joinOrder, double reward, int[] tupleIndices,
                       int[] tableCardinality) {
        System.err.print(reward);
        for (int i = 0; i < joinOrder.length; i++) {
            System.err.print(joinOrder[i]);
        }
        System.err.println();
    }
}
