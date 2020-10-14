package visualization;

import query.QueryInfo;

public interface EpisodeDataConsumer {
    void init(QueryInfo info);
    void update(int[] joinOrder, double reward, int[] tupleIndices,
                       int[] tableCardinality);
}
