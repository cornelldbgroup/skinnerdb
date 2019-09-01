package expressions;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectVisitor;

/**
 * Some generic utility methods 
 * for the Skinner visitor.
 * 
 * @author immanueltrummer
 *
 */
public class VisitorUtil {
	/**
	 * Visits the given expression using the given visitor within a try-catch
	 * block. Check whether visitor stored the root cause of the exception and
	 * throw corresponding exception in that case. Otherwise throw original
	 * exception (or execute peacefully). This is needed since the original
	 * expression visitor interface does not consider exceptions.
	 * 
	 * @param expression	expression to visit
	 * @param visitor		visit expression via this visitor
	 * @throws Exception
	 */
	public static void tryVisit(Expression expression, 
			SkinnerVisitor visitor) throws Exception {
		try {
			expression.accept(visitor);
		} catch (Exception e) {
			// Was exception caused by another exception that
			// was not yet thrown?
			if (!visitor.sqlExceptions.isEmpty()) {
				throw visitor.sqlExceptions.get(0);
			} else {
				throw e;
			}
		}
		// Check whether any errors occurred -
		// throw corresponding exceptions if so.
		if (!visitor.sqlExceptions.isEmpty()) {
			throw visitor.sqlExceptions.get(0);
		}
	}
	/**
	 * Visits the given query using the given visitor within a try-catch
	 * block. Check whether visitor stored the root cause of the exception and
	 * throw corresponding exception in that case. Otherwise throw original
	 * exception (or execute peacefully). This is needed since the original
	 * expression visitor interface does not consider exceptions.
	 * 
	 * @param plainSelect	query to visit
	 * @param visitor		visit query via this visitor
	 * @throws Exception
	 */
	public static void tryVisit(PlainSelect plainSelect, 
			SkinnerVisitor visitor) throws Exception {
		try {
			SelectVisitor selectVisitor = (SelectVisitor)visitor;
			plainSelect.accept(selectVisitor);
		} catch (Exception e) {
			// Was exception caused by another exception that
			// was not yet thrown?
			if (!visitor.sqlExceptions.isEmpty()) {
				throw visitor.sqlExceptions.get(0);
			} else {
				throw e;
			}
		}
		// Check whether any errors occurred -
		// throw corresponding exceptions if so.
		if (!visitor.sqlExceptions.isEmpty()) {
			throw visitor.sqlExceptions.get(0);
		}
	}
}
