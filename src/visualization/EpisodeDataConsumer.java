package visualization;

import joining.uct.UctNode;
import query.QueryInfo;

public interface EpisodeDataConsumer {
    void init(QueryInfo info);
    default void update(int[] joinOrder, double reward, int[] tupleIndices,
                       int[] tableCardinality) {}
    default void update(UctNode node) {}
    default void finalPlan(UctNode node) {}
}
