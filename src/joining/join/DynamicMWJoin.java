package joining.join;

import preprocessing.Context;
import query.QueryInfo;

/**
 * A multi-way join operator that executes joins in a
 * sequence of small time slices. The difference to
 * {@link joning.join.MultiWayJoin} is that join orders
 * can be changed in each invocation. 
 * 
 * @author immanueltrummer
 *
 */
public abstract class DynamicMWJoin extends MultiWayJoin {
	/**
	 * Initializes dynamic multi-way join for specific
	 * query and execution context.
	 * 
	 * @param query				query whose tables we join
	 * @param executionContext	query evaluation context
	 * @throws Exception
	 */
    public DynamicMWJoin(QueryInfo query, 
    		Context executionContext) throws Exception {
		super(query, executionContext);
	}
	/**
     * Executes given join order for a given number of steps.
     * 
     * @param order		execute this join order
     * @return			reward (higher reward means faster progress)
     * @throws Exception 
     */
    public abstract double execute(int[] order) throws Exception;
}
