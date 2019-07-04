package expressions;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.DateTimeLiteralExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.ExtractExpression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.HexValue;
import net.sf.jsqlparser.expression.IntervalExpression;
import net.sf.jsqlparser.expression.JdbcNamedParameter;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.JsonExpression;
import net.sf.jsqlparser.expression.KeepExpression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.MySQLGroupConcat;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.NumericBind;
import net.sf.jsqlparser.expression.OracleHierarchicalExpression;
import net.sf.jsqlparser.expression.OracleHint;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.RowConstructor;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeKeyExpression;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.UserVariable;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.WithinGroupExpression;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Modulo;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.JsonOperator;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.expression.operators.relational.RegExpMatchOperator;
import net.sf.jsqlparser.expression.operators.relational.RegExpMySQLOperator;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;
import query.SQLexception;

/**
 * Interface implemented by components that visit expressions
 * within SQL queries - differs from the original version by
 * allowing to throw SQL exceptions during processing (e.g.,
 * if tables are referenced that do not appear in the catalog).
 * 
 * @author immanueltrummer
 *
 */
public interface SkinnerDeprVisitor {

    void visit(NullValue nullValue) throws SQLexception;

    void visit(Function function) throws SQLexception;

    void visit(SignedExpression signedExpression) throws SQLexception;

    void visit(JdbcParameter jdbcParameter) throws SQLexception;

    void visit(JdbcNamedParameter jdbcNamedParameter) throws SQLexception;

    void visit(DoubleValue doubleValue) throws SQLexception;

    void visit(LongValue longValue) throws SQLexception;

    void visit(HexValue hexValue) throws SQLexception;

    void visit(DateValue dateValue) throws SQLexception;

    void visit(TimeValue timeValue) throws SQLexception;

    void visit(TimestampValue timestampValue) throws SQLexception;

    void visit(Parenthesis parenthesis) throws SQLexception;

    void visit(StringValue stringValue) throws SQLexception;

    void visit(Addition addition) throws SQLexception;

    void visit(Division division) throws SQLexception;

    void visit(Multiplication multiplication) throws SQLexception;

    void visit(Subtraction subtraction) throws SQLexception;

    void visit(AndExpression andExpression) throws SQLexception;

    void visit(OrExpression orExpression) throws SQLexception;

    void visit(Between between) throws SQLexception;

    void visit(EqualsTo equalsTo) throws SQLexception;

    void visit(GreaterThan greaterThan) throws SQLexception;

    void visit(GreaterThanEquals greaterThanEquals) throws SQLexception;

    void visit(InExpression inExpression) throws SQLexception;

    void visit(IsNullExpression isNullExpression) throws SQLexception;

    void visit(LikeExpression likeExpression) throws SQLexception;

    void visit(MinorThan minorThan) throws SQLexception;

    void visit(MinorThanEquals minorThanEquals) throws SQLexception;

    void visit(NotEqualsTo notEqualsTo) throws SQLexception;

    void visit(Column tableColumn) throws SQLexception;

    void visit(SubSelect subSelect) throws SQLexception;

    void visit(CaseExpression caseExpression) throws SQLexception;

    void visit(WhenClause whenClause) throws SQLexception;

    void visit(ExistsExpression existsExpression) throws SQLexception;

    void visit(AllComparisonExpression allComparisonExpression) throws SQLexception;

    void visit(AnyComparisonExpression anyComparisonExpression) throws SQLexception;

    void visit(Concat concat) throws SQLexception;

    void visit(Matches matches) throws SQLexception;

    void visit(BitwiseAnd bitwiseAnd) throws SQLexception;

    void visit(BitwiseOr bitwiseOr) throws SQLexception;

    void visit(BitwiseXor bitwiseXor) throws SQLexception;

    void visit(CastExpression cast) throws SQLexception;

    void visit(Modulo modulo) throws SQLexception;

    void visit(AnalyticExpression aexpr) throws SQLexception;

    void visit(WithinGroupExpression wgexpr) throws SQLexception;

    void visit(ExtractExpression eexpr) throws SQLexception;

    void visit(IntervalExpression iexpr) throws SQLexception;

    void visit(OracleHierarchicalExpression oexpr) throws SQLexception;

    void visit(RegExpMatchOperator rexpr) throws SQLexception;

    void visit(JsonExpression jsonExpr) throws SQLexception;

    void visit(JsonOperator jsonExpr) throws SQLexception;

    void visit(RegExpMySQLOperator regExpMySQLOperator) throws SQLexception;

    void visit(UserVariable var) throws SQLexception;

    void visit(NumericBind bind) throws SQLexception;

    void visit(KeepExpression aexpr) throws SQLexception;

    void visit(MySQLGroupConcat groupConcat) throws SQLexception;

    void visit(RowConstructor rowConstructor) throws SQLexception;

    void visit(OracleHint hint) throws SQLexception;

    void visit(TimeKeyExpression timeKeyExpression) throws SQLexception;

    void visit(DateTimeLiteralExpression literal) throws SQLexception;

    public void visit(NotExpression aThis) throws SQLexception;
}
