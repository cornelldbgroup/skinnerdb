package expressions.normalization;

import net.sf.jsqlparser.expression.StringValue;

/**
 * Adds Postgres-specific specifications for string ordering
 * to query. This is important for comparing Skinner results
 * against Postgres results for debugging. 
 * 
 * @author immanueltrummer
 *
 */
public class CollationVisitor extends CopyVisitor {
	@Override
	public void visit(StringValue arg0) {
		exprStack.push(new CollatedStringValue(arg0.getValue(), "C"));
	}
}
