package expressions;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.ExpressionVisitor;
import query.SQLexception;

/**
 * Common super-class of all classes that traverse SQL expressions
 * recursively which may produce errors.
 * 
 * @author immanueltrummer
 *
 */
public abstract class SkinnerVisitor implements ExpressionVisitor {
	/**
	 * List of exceptions produced while traversing expression.
	 * If this list is empty, it means that no error occurred.
	 */
	public List<SQLexception> sqlExceptions = new ArrayList<>();
}
