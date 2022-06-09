package predicate;

import indexing.Index;
import joining.parallel.indexing.PartitionIndex;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import query.QueryInfo;

import java.util.List;

/**
 * The tree structure of nonEquiJoin predicates.
 * By parsing the complex predicates into tree structure,
 * the conjunction of predicates is able to be evaluated.
 *
 * @author  Anonymous
 */
public class PreNode {
    /**
     * The node translated from left expression.
     */
    final PreNode left;
    /**
     * The node translated from right expression.
     */
    final PreNode right;
    /**
     * The index corresponding to left expression.
     */
    public final PartitionIndex index;
    /**
     * The operator of this predicate.
     * It could be AND, OR, ADDITION, ...
     */
    public final Operator operator;
    /**
     * Meta data without materialization
     */
    public final int[] rows;
    /**
     * Estimated cardinality.
     */
    public final int cardinality;
    /**
     * Best expression to be indexed
     */
    public final Expression expression;


    public PreNode(PreNode left, PreNode right,
                   PartitionIndex index, Operator operator,
                   int[] rows, int cardinality, Expression expression) {
        this.left = left;
        this.right = right;
        this.index = index;
        this.operator = operator;
        this.rows = rows;
        this.cardinality = cardinality;
        this.expression = expression;
    }
}
