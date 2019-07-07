package joining;

import java.util.Arrays;

import config.JoinConfig;
import joining.join.OldJoin;
import joining.uct.SelectionPolicy;
import joining.uct.UctNode;
import preprocessing.Context;
import query.QueryInfo;

/**
 * This variant of the join processor searches
 * the join order space in parallel via multiple
 * threads.
 * 
 * @author immanueltrummer
 *
 */
public class ParallelSearchJP {
	/**
	 * Executes the join phase and stores result in relation.
	 * Also updates mapping from query column references to
	 * database columns.
	 * 
	 * @param query		query to process
	 * @param context	query execution context
	 */
	public static void process(QueryInfo query, 
			Context context) throws Exception {
		// Initialize multi-way join operator
		OldJoin joinOp = new OldJoin(query, context, 
				JoinConfig.BUDGET_PER_EPISODE);
		// Initialize UCT join order search tree
		UctNode root = new UctNode(0, query, true, joinOp);
		// Expand first tree level
		int nrActions = root.nrActions;
		for (int actionCtr=0; actionCtr<nrActions; ++actionCtr) {
			int joinedTable = root.nextTable[actionCtr];
			root.childNodes[actionCtr] = new UctNode(
					0, root, joinedTable);
		}
		// Initialize counters and variables
		int[] joinOrder = new int[query.nrJoined];
		// Iterate until join result was generated
		Arrays.stream(root.childNodes).parallel().forEach(c -> {
			long roundCtr = 1;
			try {
				while (!joinOp.isFinished()) {
					++roundCtr;
					c.sample(roundCtr, joinOrder, SelectionPolicy.UCB1);
				}							
			} catch (Exception e) {
				e.printStackTrace();
			}
		});
		// Materialize result and update column mappings
		SequentialJP.finalizeJoinPhase(query, context, joinOp);
	}
}
