package query.select;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import expressions.normalization.CollectReferencesVisitor;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import query.SQLexception;

/**
 * Utility functions that refer to entries
 * in the SQL select clause.
 * 
 * @author immanueltrummer
 *
 */
public class SelectUtil {
	/**
	 * Generate an alias for columns in the SELECT clause for which
	 * no alias is specified. Alias name is based on the expression
	 * type, to ensure unique aliases, a number is optionally appended. 
	 * 
	 * @param expression	expression for which to introduce alias
	 * @param priorAliases	set of all previously used aliases
	 * @return	unique alias
	 */
	static String generateAlias(Expression expression, 
			Set<String> priorAliases) {
		// Generate alias prefix
		String prefix = "default";
		if (expression instanceof Column) {
			Column column = (Column)expression;
			prefix = column.getColumnName();
		} else if (expression instanceof Function) {
			Function function = (Function)expression;
			prefix = function.getName();
		}
		// Add number suffix if necessary
		String alias = prefix;
		int aliasCtr = 1;
		while (priorAliases.contains(alias)) {
			aliasCtr++;
			alias = prefix + aliasCtr;
		}
		return alias;
	}
	/**
	 * Assigns names to each item in the given select clause.
	 * Returns a mapping from select item expressions to names.
	 * 
	 * @param selectItems	items in select clause
	 * @return				mappin from select expressions to names
	 * @throws Exception
	 */
	public static Map<Expression, String> assignAliases(
			List<SelectItem> selectItems) throws SQLexception {
		// Initialize method result
		Map<Expression, String> exprToAlias = new HashMap<>();
		// Set of previously used aliases
		Set<String> priorAliases = new HashSet<>();
		// Add select items and assign aliases
		for (SelectItem selectItem : selectItems) {
			// Do we support this type of select item?
			if (selectItem instanceof SelectExpressionItem) {
				SelectExpressionItem exprItem = 
						(SelectExpressionItem)selectItem;
				Expression expr = exprItem.getExpression();
				Alias queryAlias = exprItem.getAlias();
				String alias = queryAlias!=null?
						queryAlias.getName():
							generateAlias(expr, priorAliases);
				// Check whether alias was used before
				if (priorAliases.contains(alias)) {
					throw new SQLexception("Error - alias " + 
							alias + " used multiple times");
				}
				priorAliases.add(alias);
				exprToAlias.put(expr, alias);
			} else {
				throw new SQLexception("Error - unsupported " +
						"type of select item: " + selectItem);
			}

		}
		return exprToAlias;
	}
	/**
	 * Returns true iff the given list of select items contains
	 * aggregates (i.e., the associated query is an aggregate
	 * query).
	 * 
	 * @param selectItems	list of items in query SELECT clause
	 * @return				true iff at least one select item is an aggregate
	 */
	public static boolean hasAggregates(List<SelectItem> selectItems) 
			throws SQLexception {
		CollectReferencesVisitor collector = new CollectReferencesVisitor();
		for (SelectItem selectItem : selectItems) {
			if (selectItem instanceof SelectExpressionItem) {
				SelectExpressionItem exprItem = 
						(SelectExpressionItem)selectItem;
				Expression expr = exprItem.getExpression();
				expr.accept(collector);
				if (!collector.aggregates.isEmpty()) {
					return true;
				}
			} else {
				throw new SQLexception("Error - unsupported type of "
						+ "select expression: " + selectItem);
			}
		}
		return false;
	}
}
