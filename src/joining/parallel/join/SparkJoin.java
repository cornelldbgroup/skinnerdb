package joining.parallel.join;

import preprocessing.Context;
import query.QueryInfo;

/**
 * Join batches of data by using a certain
 * order of tables in the spark engine. The
 * order is broadcast by the master thread.
 *
 * @author Ziyun Wei
 */
public class SparkJoin {
    /**
     * The query for which join orders are evaluated.
     */
    public final QueryInfo query;
    /**
     * Summarizes pre-processing steps.
     */
    public final Context preSummary;
    /**
     * ID of the join operator.
     */
    public final int eid;

    /**
     * Initializes join algorithm for given input query.
     *
     * @param query			query to process
     * @param preSummary	summary of pre-processing
     * @param eid		    executor id
     */
    public SparkJoin(QueryInfo query, Context preSummary, int eid) {
        this.query = query;
        this.preSummary = preSummary;
        this.eid = eid;
    }
}
