package expressions.normalization;

import java.util.List;

import expressions.SkinnerDeprVisitor;
import expressions.SkinnerVisitor;
import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.DateTimeLiteralExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
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
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
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

/**
 * Visits each node in an expression without any action. This
 * class serves as super class for specialized classes that
 * only need to treat specific node types (the corresponding
 * methods are overridden). 
 * 
 * @author immanueltrummer
 *
 */
public class PlainVisitor extends SkinnerVisitor {

	@Override
	public void visit(NullValue nullValue) {
		
	}

	@Override
	public void visit(Function function) {
		ExpressionList paramList = function.getParameters();
		if (paramList != null) {
			for (Expression parameterExpression :
				paramList.getExpressions()) {
				parameterExpression.accept(this);
			}			
		}
	}

	@Override
	public void visit(SignedExpression signedExpression) {
		signedExpression.getExpression().accept(this);
	}

	@Override
	public void visit(JdbcParameter jdbcParameter) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(JdbcNamedParameter jdbcNamedParameter) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(DoubleValue doubleValue) {
		
	}

	@Override
	public void visit(LongValue longValue) {
		
	}

	@Override
	public void visit(HexValue hexValue) {
		
	}

	@Override
	public void visit(DateValue dateValue) {
		
	}

	@Override
	public void visit(TimeValue timeValue) {
		
	}

	@Override
	public void visit(TimestampValue timestampValue) {
		
	}

	@Override
	public void visit(Parenthesis parenthesis) {
		parenthesis.getExpression().accept(this);
	}

	@Override
	public void visit(StringValue stringValue) {
		
	}
	
	void treatBinary(BinaryExpression binaryExpr) {
		binaryExpr.getLeftExpression().accept(this);
		binaryExpr.getRightExpression().accept(this);
	}

	@Override
	public void visit(Addition addition) {
		treatBinary(addition);
	}

	@Override
	public void visit(Division division) {
		treatBinary(division);
	}

	@Override
	public void visit(Multiplication multiplication) {
		treatBinary(multiplication);
	}

	@Override
	public void visit(Subtraction subtraction) {
		treatBinary(subtraction);
	}

	@Override
	public void visit(AndExpression andExpression) {
		treatBinary(andExpression);
	}

	@Override
	public void visit(OrExpression orExpression) {
		treatBinary(orExpression);
	}

	@Override
	public void visit(Between between) {
		between.getLeftExpression().accept(this);
		between.getBetweenExpressionStart().accept(this);
		between.getBetweenExpressionEnd().accept(this);
	}

	@Override
	public void visit(EqualsTo equalsTo) {
		treatBinary(equalsTo);
	}

	@Override
	public void visit(GreaterThan greaterThan) {
		treatBinary(greaterThan);
	}

	@Override
	public void visit(GreaterThanEquals greaterThanEquals) {
		treatBinary(greaterThanEquals);
	}

	@Override
	public void visit(InExpression inExpression) {
		inExpression.getLeftExpression().accept(this);
		List<Expression> expressionsInList = ((ExpressionList)
				inExpression.getRightItemsList()).getExpressions();
		for (Expression expr : expressionsInList) {
			expr.accept(this);
		}
	}

	@Override
	public void visit(IsNullExpression isNullExpression) {
		isNullExpression.getLeftExpression().accept(this);
	}

	@Override
	public void visit(LikeExpression likeExpression) {
		treatBinary(likeExpression);
	}

	@Override
	public void visit(MinorThan minorThan) {
		treatBinary(minorThan);
	}

	@Override
	public void visit(MinorThanEquals minorThanEquals) {
		treatBinary(minorThanEquals);
	}

	@Override
	public void visit(NotEqualsTo notEqualsTo) {
		treatBinary(notEqualsTo);
	}

	@Override
	public void visit(Column tableColumn) {
		
	}

	@Override
	public void visit(SubSelect subSelect) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(CaseExpression caseExpression) {
		if (caseExpression.getSwitchExpression() != null) {
			caseExpression.getSwitchExpression().accept(this);
		}
		if (caseExpression.getElseExpression() != null) {
			caseExpression.getElseExpression().accept(this);
		}
		for (Expression expr : caseExpression.getWhenClauses()) {
			expr.accept(this);
		}
	}

	@Override
	public void visit(WhenClause whenClause) {
		whenClause.getWhenExpression().accept(this);
		whenClause.getThenExpression().accept(this);
	}

	@Override
	public void visit(ExistsExpression existsExpression) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(AllComparisonExpression allComparisonExpression) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(AnyComparisonExpression anyComparisonExpression) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Concat concat) {
		treatBinary(concat);
	}

	@Override
	public void visit(Matches matches) {
		treatBinary(matches);
	}

	@Override
	public void visit(BitwiseAnd bitwiseAnd) {
		treatBinary(bitwiseAnd);
	}

	@Override
	public void visit(BitwiseOr bitwiseOr) {
		treatBinary(bitwiseOr);
	}

	@Override
	public void visit(BitwiseXor bitwiseXor) {
		treatBinary(bitwiseXor);
	}

	@Override
	public void visit(CastExpression cast) {
		cast.getLeftExpression().accept(this);
	}

	@Override
	public void visit(Modulo modulo) {
		treatBinary(modulo);
	}

	@Override
	public void visit(AnalyticExpression aexpr) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(WithinGroupExpression wgexpr) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(ExtractExpression eexpr) {
		eexpr.getExpression().accept(this);
	}

	@Override
	public void visit(IntervalExpression iexpr) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(OracleHierarchicalExpression oexpr) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(RegExpMatchOperator rexpr) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(JsonExpression jsonExpr) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(JsonOperator jsonExpr) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(RegExpMySQLOperator regExpMySQLOperator) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(UserVariable var) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(NumericBind bind) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(KeepExpression aexpr) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(MySQLGroupConcat groupConcat) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(RowConstructor rowConstructor) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(OracleHint hint) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(TimeKeyExpression timeKeyExpression) {
		
	}

	@Override
	public void visit(DateTimeLiteralExpression literal) {
		
	}

	@Override
	public void visit(NotExpression aThis) {
		aThis.getExpression().accept(this);
	}

}
