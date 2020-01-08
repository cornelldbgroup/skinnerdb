package expressions.normalization;

import java.util.HashSet;
import java.util.Set;

import query.ColumnRef;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

/**
 * Collects all tables and columns referenced in a given expression.
 * Also collects LIKE expression and aggregates.
 * 
 * @author immanueltrummer
 *
 */
public class CollectReferencesVisitor extends PlainVisitor {
	/**
	 * Contains the set of table aliases that the expression references.
	 */
	public final Set<String> mentionedTables = new HashSet<String>();
	/**
	 * Contains the set of columns (with table name) the the
	 * expression refers to.
	 */
	public final Set<ColumnRef> mentionedColumns = new HashSet<ColumnRef>();
	/**
	 * Set of SQL LIKE expressions found in the given expression.
	 */
	public final Set<Expression> likeExpressions = new HashSet<Expression>();
	/**
	 * Set of aggregation functions with parameters used in given expression.
	 */
	public final Set<Function> aggregates = new HashSet<Function>();
	@Override
	public void visit(Column tableColumn) {
		Table table = tableColumn.getTable();
		String tableName = table==null||table.getName()==null?
				"":table.getName();
		String columnName = tableColumn.getColumnName();
		mentionedTables.add(tableName);
		mentionedColumns.add(new ColumnRef(tableName, columnName));
	}
	@Override
	public void visit(LikeExpression arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		likeExpressions.add(arg0.getRightExpression());
	}
	@Override
	public void visit(Function arg0) {
		// Recursive collection
		if (arg0.getParameters() != null && arg0.getParameters().getExpressions() != null) {
			for (Expression param : arg0.getParameters().getExpressions()) {
				param.accept(this);
			}
		}
		// Is it an aggregation function?
		String functionName = arg0.getName().toLowerCase();
		for (String aggName : new String[] {
				"min", "max", "sum", "count", "avg"}) {
			if (aggName.equals(functionName)) {
				aggregates.add(arg0);
				return;
			}
		}
	}
}
