package expressions;

import net.sf.jsqlparser.expression.ExpressionVisitor;
import query.SQLexception;

import java.util.ArrayList;
import java.util.List;

/**
 * Common super-class of all classes that traverse SQL expressions
 * recursively which may produce errors.
 *
 * @author immanueltrummer
 */
public abstract class SkinnerVisitor implements ExpressionVisitor {
    /**
     * List of exceptions produced while traversing expression.
     * If this list is empty, it means that no error occurred.
     */
    public List<SQLexception> sqlExceptions = new ArrayList<>();

    protected void throwException(String e) {
        sqlExceptions.add(new SQLexception(e));
    }
}
