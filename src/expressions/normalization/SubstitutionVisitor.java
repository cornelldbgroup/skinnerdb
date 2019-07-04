package expressions.normalization;

import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;

/**
 * Substitutes aliases by expressions in a given expression.
 * 
 * @author immanueltrummer
 *
 */
public class SubstitutionVisitor extends CopyVisitor {
	/**
	 * Maps aliases to expressions for substitution.
	 */
	public final Map<String, Expression> aliasToExpr;
	/**
	 * Initializes substitution map.
	 * 
	 * @param aliasToExpr	maps aliases to expressions for substitution
	 */
	public SubstitutionVisitor(Map<String, Expression> aliasToExpr) {
		this.aliasToExpr = aliasToExpr;
	}
	@Override
	public void visit(Column arg0) {
		String alias = arg0.toString().toLowerCase();
		if (aliasToExpr.containsKey(alias)) {
			exprStack.push(aliasToExpr.get(alias));
		} else {
			exprStack.push(arg0);			
		}
	}
}
