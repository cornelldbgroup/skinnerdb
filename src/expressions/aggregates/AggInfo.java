package expressions.aggregates;

import expressions.ExpressionInfo;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import query.QueryInfo;

/**
 * Information about an aggregate expression.
 * 
 * @author immanueltrummer
 *
 */
public class AggInfo {
	/**
	 * Function used for aggregation.
	 */
	public final SQLaggFunction aggFunction;
	/**
	 * Input to aggregation function.
	 */
	public final ExpressionInfo aggInput;
	/**
	 * SQL representation of aggregate with input.
	 */
	public final String SQLstring;
	/**
	 * Initialize aggregate info from given expression.
	 * 
	 * @param query		query in which the aggregate appears
	 * @param agg		aggregation expression
	 */
	public AggInfo(QueryInfo query, Function agg) throws Exception {
		Expression param = agg.getParameters().getExpressions().get(0);
		this.aggInput = new ExpressionInfo(query, param);
		String fctName = agg.getName().toLowerCase();
		this.aggFunction = SQLfunction(fctName);
		this.SQLstring = agg.toString();
	}
	/**
	 * Translates an SQL function name into the internal representation.
	 * 
	 * @param fctName	name of function to translate
	 * @return			associated internal identifier
	 */
	static SQLaggFunction SQLfunction(String fctName) {
		String[] fctNames = new String[] {"min", "max", "avg", "count", "sum"};
		SQLaggFunction[] fcts = new SQLaggFunction[] {SQLaggFunction.MIN, 
				SQLaggFunction.MAX, SQLaggFunction.AVG, SQLaggFunction.COUNT,
				SQLaggFunction.SUM};
		for (int fctCtr=0; fctCtr<5; ++fctCtr) {
			if (fctName.equals(fctNames[fctCtr])) {
				return fcts[fctCtr];
			}
		}
		return null;
	}
	@Override
	public String toString() {
		return SQLstring;
	}
}
