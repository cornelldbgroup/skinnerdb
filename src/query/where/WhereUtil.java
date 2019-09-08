package query.where;

import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;

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
		} else if (condition instanceof AndExpression) {
			AndExpression and = (AndExpression)condition;
			extractConjuncts(and.getLeftExpression(), conjuncts);
			extractConjuncts(and.getRightExpression(), conjuncts);
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
