package joining.uct;

import config.ParallelConfig;
import joining.join.DPJoin;
import joining.join.MultiWayJoin;
import query.QueryInfo;

import java.util.Set;

/**
 * Represents node in UCT search tree for data parallel.
 *
 * @author immanueltrummer
 */
public class DPNode extends UctNode {
    /**
     * Evaluates a given join order and accumulates results.
     */
    final DPJoin joinOp;
    /**
     * Initialize UCT root node.
     *
     * @param roundCtr     	current round number
     * @param query        	the query which is optimized
     * @param useHeuristic 	whether to avoid Cartesian products
     * @param joinOp		multi-way join operator allowing fast join order switching
     */
    public DPNode(long roundCtr, QueryInfo query, boolean useHeuristic, DPJoin joinOp) {
        super(roundCtr, query, useHeuristic, joinOp);
        this.joinOp = joinOp;
    }
    /**
     * Initializes UCT node by expanding parent node.
     *
     * @param roundCtr    current round number
     * @param parent      parent node in UCT tree
     * @param joinedTable new joined table
     */
    public DPNode(long roundCtr, DPNode parent, int joinedTable) {
        super(roundCtr, parent, joinedTable);
        this.joinOp = parent.joinOp;
    }
}
