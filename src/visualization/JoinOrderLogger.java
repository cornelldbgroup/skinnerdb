package visualization;

import catalog.CatalogManager;
import query.QueryInfo;

import java.util.Map;

public class JoinOrderLogger implements EpisodeDataConsumer {
    public void init(QueryInfo query) {
        for (Map.Entry<String,Integer> entry : query.aliasToIndex.entrySet()) {
            String alias = entry.getKey();
            int index = entry.getValue();
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
