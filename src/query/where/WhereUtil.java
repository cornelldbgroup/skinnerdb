package query.where;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import expressions.normalization.CollectReferencesVisitor;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import query.ColumnRef;
import query.from.FromUtil;

/**
 * Utility functions for analyzing the WHERE clause.
 * 
 * @author immanueltrummer
 *
 */
public class WhereUtil {
	/**
	 * Extracts all conjuncts from a nested AND expression
	 * via recursive calls. The result will be stored in
	 * the second parameter.
	 * 
	 * @param condition	the remaining condition (no conjuncts extracted yet)
	 * @param conjuncts	stores the resulting conjuncts
	 */
	public static void extractConjuncts(Expression condition, 
			List<Expression> conjuncts) {
		if (condition instanceof Parenthesis) {
			Parenthesis parenthesis = (Parenthesis)condition;
			Expression inParen = parenthesis.getExpression();
			// Break up nested AND expression if possible
			if (inParen instanceof AndExpression) {
				extractConjuncts(parenthesis.getExpression(), conjuncts);
			} else {
				conjuncts.add(condition);
			}
		} else if (condition instanceof ExistsExpression) {
			ExistsExpression exist = (ExistsExpression) condition;
			if (!exist.isNot()) {
//				PlainSelect subSelect = (PlainSelect) ((SubSelect)exist.getRightExpression()).getSelectBody();
//				List<SelectItem> selectItems = new ArrayList<>();
//				Function function = new Function();
//				function.setName("COUNT");
//				function.setAllColumns(true);
//				selectItems.add(new SelectExpressionItem(function));
//				subSelect.setSelectItems(selectItems);
//				GreaterThan greaterThan = new GreaterThan();
//				greaterThan.setLeftExpression(exist.getRightExpression());
//				greaterThan.setRightExpression(new LongValue(0));
//				conjuncts.add(greaterThan);
				conjuncts.add(condition);
			}
			else {
//				PlainSelect subSelect = (PlainSelect) ((SubSelect)exist.getRightExpression()).getSelectBody();
//				List<SelectItem> selectItems = new ArrayList<>();
//				Function function = new Function();
//				function.setName("COUNT");
//				function.setAllColumns(true);
//				selectItems.add(new SelectExpressionItem(function));
//				subSelect.setSelectItems(selectItems);
//				InExpression inExpression = new InExpression();
//				List<Expression> subConjuncts = new ArrayList<>();
//				WhereUtil.extractConjuncts(subSelect.getWhere(), subConjuncts);
//				CollectReferencesVisitor collectReferencesVisitor = new CollectReferencesVisitor();
//				List<FromItem> fromItems = FromUtil.allFromItems(subSelect);
//				Set<String> inTables = new HashSet<>();
//				// Iterate over base tables in FROM clause
//				for (FromItem fromItem : fromItems) {
//					if (fromItem instanceof Table) {
//						// Extract table and alias name (defaults to table name)
//						Table table = (Table)fromItem;
//						String tableAlias = table.getAlias().getName();
//						inTables.add(tableAlias);
//					}
//				}
//				for (Expression sub: subConjuncts) {
//					if (sub instanceof EqualsTo) {
//						sub.accept(collectReferencesVisitor);
//						for (ColumnRef columnRef: collectReferencesVisitor.mentionedColumns) {
//							if (!inTables.contains(columnRef.aliasName)) {
//								inExpression.setLeftExpression(
//										new Column(new Table(columnRef.aliasName), columnRef.columnName));
//							}
//						}
//					}
//				}
//				SubSelect select = new SubSelect();
//				select.setSelectBody(subSelect);
//				inExpression.setRightItemsList(select);
//				inExpression.setNot(true);
//				conjuncts.add(inExpression);
				conjuncts.add(condition);
			}
		}
		else if (condition instanceof AndExpression) {
			AndExpression and = (AndExpression)condition;
			if (and.isNot()) {
				conjuncts.add(and);
			}
			else {
				extractConjuncts(and.getLeftExpression(), conjuncts);
				extractConjuncts(and.getRightExpression(), conjuncts);
			}
		} else {
			conjuncts.add(condition);
		}
	}
	/**
	 * Combines given conditions with conjunctions and returns
	 * resulting expression. Returns null if the list of
	 * conditions is empty.
	 * 
	 * @param conjuncts	conditions to connect in a conjunction
	 * @return			conjunction or null (if no conditions specified)
	 */
	public static Expression conjunction(List<Expression> conjuncts) {
		if (conjuncts.isEmpty()) {
			// Null pointer represents empty WHERE clause
			return null;
		} else {
			int nrConjuncts = conjuncts.size();
			Expression conjunction = conjuncts.get(0);
			for (int conjunctCtr=1; conjunctCtr<nrConjuncts; ++conjunctCtr) {
				Expression curConjunct = conjuncts.get(conjunctCtr);
				conjunction = new AndExpression(conjunction, curConjunct);
			}
			return conjunction;
		}
	}
}
