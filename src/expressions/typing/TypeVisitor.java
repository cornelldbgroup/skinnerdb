package expressions.typing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import buffer.BufferManager;
import catalog.CatalogManager;
import catalog.info.ColumnInfo;
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
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.select.SubSelect;
import query.ColumnRef;
import query.QueryInfo;
import query.SQLexception;
import types.SQLtype;
import types.TypeUtil;

/**
 * Assigns types to each expression part and adds casts
 * if necessary.
 * 
 * @author immanueltrummer
 *
 */
public class TypeVisitor extends SkinnerVisitor {
	/**
	 * Contains required information for mapping columns to types.
	 */
	final QueryInfo queryInfo;
	/**
	 * Maps each expression to its output type.
	 */
	public Map<Expression, SQLtype> outputType =
			new HashMap<Expression, SQLtype>();
	/**
	 * Maps each expression to its output scope.
	 */
	public Map<Expression, ExpressionScope> outputScope =
			new HashMap<Expression, ExpressionScope>();
	/**
	 * Initializes query information.
	 * 
	 * @param queryInfo	information about query
	 */
	public TypeVisitor(QueryInfo queryInfo) {
		this.queryInfo = queryInfo;
	}
	/**
	 * Creates a new cast expression with given input,
	 * target type, and target scope. Updates type
	 * and scope data structures accordingly.
	 * 
	 * @param input			cast input expression
	 * @param targetType	target type for casting
	 * @param targetScope	scope of cast output
	 * @return				newly created cast expression
	 */
	CastExpression newCast(Expression input, SQLtype targetType, 
			ExpressionScope targetScope) {
		CastExpression cast = new CastExpression();
		cast.setLeftExpression(input);
		ColDataType colDataType = new ColDataType();
		colDataType.setDataType(targetType.toString());
		cast.setType(colDataType);
		outputType.put(cast, targetType);
		outputScope.put(cast, targetScope);
		return cast;
	}
	/**
	 * Add casts for inputs of binary expression if necessary.
	 * 
	 * @param binaryExpression	binary expression
	 */
	void castInputs(BinaryExpression binaryExpression) {
		Expression expression1 = binaryExpression.getLeftExpression();
		Expression expression2 = binaryExpression.getRightExpression();
		SQLtype type1 = outputType.get(expression1);
		SQLtype type2 = outputType.get(expression2);
		ExpressionScope scope = outputScope.get(expression1);
		SQLtype commonType = TypeUtil.commonType(type1, type2);
		if (commonType == null) {
			sqlExceptions.add(new SQLexception("Error - "
					+ "failed to add automated casts to "
					+ "unify types " + type1 + " and " + type2
					+ " in expression " + binaryExpression.toString()));
		}
		if (type1 != commonType) {
			CastExpression cast = newCast(expression1, commonType, scope);
			binaryExpression.setLeftExpression(cast);
		}
		if (type2 != commonType) {
			CastExpression cast = newCast(expression2, commonType, scope);
			binaryExpression.setRightExpression(cast);
		}
	}
	/**
	 * Treats a binary arithmetic expression.
	 * 
	 * @param binaryExpression	a binary expression
	 */
	void treatBinaryArithmetic(BinaryExpression binaryExpression) {
		// Add output types for binary expression and components
		Expression expression1 = binaryExpression.getLeftExpression();
		Expression expression2 = binaryExpression.getRightExpression();
		expression1.accept(this);
		expression2.accept(this);
		SQLtype type1 = outputType.get(expression1);
		SQLtype type2 = outputType.get(expression2);
		SQLtype commonType = TypeUtil.commonType(type1, type2);
		outputType.put(binaryExpression, commonType);
		// Take care of output scope
		ExpressionScope scope = outputScope.get(expression1);
		outputScope.put(binaryExpression, scope);
		// Add cast expressions if necessary
		castInputs(binaryExpression);
	}
	
	/**
	 * Treats a binary comparison operation.
	 * 
	 * @param binaryCmp	binary comparison operation
	 */
	void treatBinaryComparison(BinaryExpression binaryCmp) {
		// Add output types for binary expression and components
		Expression expression1 = binaryCmp.getLeftExpression();
		Expression expression2 = binaryCmp.getRightExpression();
		expression1.accept(this);
		expression2.accept(this);
		outputType.put(binaryCmp, SQLtype.BOOL);
		// No changes to output scope
		ExpressionScope scope = outputScope.get(expression1);
		outputScope.put(binaryCmp, scope);
		// Add casts if necessary
		castInputs(binaryCmp);
	}

	@Override
	public void visit(NullValue arg0) {
		outputType.put(arg0, SQLtype.ANY_TYPE);
		// TODO: this does not work in some cases
		outputScope.put(arg0, ExpressionScope.PER_TUPLE);
	}

	@Override
	public void visit(Function arg0) {
		// Treat parameter expressions
		List<Expression> paramExprs = arg0.getParameters().getExpressions();
		for (Expression paramExpr : paramExprs) {
			paramExpr.accept(this);
		}
		// Calculate common type
		SQLtype commonType = outputType.get(paramExprs.get(0));
		for (Expression paramExpr : paramExprs) {
			SQLtype type = outputType.get(paramExpr);
			commonType = TypeUtil.commonType(commonType, type);
		}
		outputType.put(arg0, commonType);
		// Calculate output scope
		ExpressionScope paramScope = outputScope.get(paramExprs.get(0));
		if (paramExprs.size() == 1) {
			// Check whether this is an aggregation function
			switch (arg0.getName().toLowerCase()) {
			case "max":
			case "min":
			case "count":
			case "sum":
			case "avg":
				outputScope.put(arg0, ExpressionScope.PER_GROUP);
				break;
			default:
				outputScope.put(arg0, paramScope);
			}
		} else {
			outputScope.put(arg0, paramScope);			
		}
		// Add cast expressions if necessary
		List<Expression> newParamExprs = new ArrayList<Expression>();
		for (Expression paramExpr : paramExprs) {
			SQLtype type = outputType.get(paramExpr);
			if (type == commonType) {
				// No casting necessary
				newParamExprs.add(paramExpr);
			} else {
				// Need to add casting
				CastExpression cast = new CastExpression();
				ColDataType colDataType = new ColDataType();
				colDataType.setDataType(commonType.toString());
				cast.setType(colDataType);
				cast.setLeftExpression(paramExpr);
				outputType.put(cast, commonType);
				newParamExprs.add(cast);
			}
		}
		arg0.setParameters(new ExpressionList(newParamExprs));
	}

	@Override
	public void visit(SignedExpression arg0) {
		arg0.getExpression().accept(this);
		SQLtype type = outputType.get(arg0.getExpression());
		ExpressionScope scope = outputScope.get(arg0.getExpression());
		outputType.put(arg0, type);
		outputScope.put(arg0, scope);
	}

	@Override
	public void visit(JdbcParameter arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(JdbcNamedParameter arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(DoubleValue arg0) {
		outputType.put(arg0, SQLtype.DOUBLE);
		outputScope.put(arg0, ExpressionScope.PER_TUPLE);
	}

	@Override
	public void visit(LongValue arg0) {
		long value = arg0.getValue();
		if (value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE) {
			outputType.put(arg0, SQLtype.INT);
		} else {
			outputType.put(arg0, SQLtype.LONG);
		}
		outputScope.put(arg0, ExpressionScope.PER_TUPLE);
	}

	@Override
	public void visit(HexValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(DateValue arg0) {
		outputType.put(arg0, SQLtype.DATE);
		outputScope.put(arg0, ExpressionScope.PER_TUPLE);
	}

	@Override
	public void visit(TimeValue arg0) {
		outputType.put(arg0, SQLtype.TIME);
		outputScope.put(arg0, ExpressionScope.PER_TUPLE);
	}

	@Override
	public void visit(TimestampValue arg0) {
		outputType.put(arg0, SQLtype.TIMESTAMP);
		outputScope.put(arg0, ExpressionScope.PER_TUPLE);
	}

	@Override
	public void visit(Parenthesis arg0) {
		arg0.getExpression().accept(this);
		SQLtype type = outputType.get(arg0.getExpression());
		ExpressionScope scope = outputScope.get(arg0.getExpression());
		outputType.put(arg0, type);
		outputScope.put(arg0, scope);
	}

	@Override
	public void visit(StringValue arg0) {
		// Treat as encoded string value is possible
		if (CatalogManager.currentDB.compressed) {
			int code = BufferManager.dictionary.getCode(arg0.getValue());
			// Use compressed encoding if string in dictionary
			if (code >= 0) {
				outputType.put(arg0, SQLtype.STRING_CODE);
			} else {
				outputType.put(arg0, SQLtype.STRING);
			}
		} else {
			outputType.put(arg0, SQLtype.STRING);
		}
		outputScope.put(arg0, ExpressionScope.PER_TUPLE);
	}

	@Override
	public void visit(Addition arg0) {
		treatBinaryArithmetic(arg0);
	}

	@Override
	public void visit(Division arg0) {
		treatBinaryArithmetic(arg0);
	}

	@Override
	public void visit(Multiplication arg0) {
		treatBinaryArithmetic(arg0);
	}

	@Override
	public void visit(Subtraction arg0) {
		treatBinaryArithmetic(arg0);
	}

	@Override
	public void visit(AndExpression arg0) {
		treatBinaryArithmetic(arg0);
	}

	@Override
	public void visit(OrExpression arg0) {
		treatBinaryArithmetic(arg0);
	}

	@Override
	public void visit(Between arg0) {
		Expression expression1 = arg0.getLeftExpression();
		Expression expression2 = arg0.getBetweenExpressionStart();
		Expression expression3 = arg0.getBetweenExpressionEnd();
		expression1.accept(this);
		expression2.accept(this);
		expression3.accept(this);
		outputType.put(arg0, SQLtype.BOOL);
		ExpressionScope scope1 = outputScope.get(expression1);
		outputScope.put(arg0, scope1);
	}

	@Override
	public void visit(EqualsTo arg0) {
		treatBinaryComparison(arg0);
	}

	@Override
	public void visit(GreaterThan arg0) {
		treatBinaryComparison(arg0);
	}

	@Override
	public void visit(GreaterThanEquals arg0) {
		treatBinaryComparison(arg0);
	}

	@Override
	public void visit(InExpression arg0) {
		// Type components
		arg0.getLeftExpression().accept(this);
		for (Expression expr : ((ExpressionList)
				arg0.getRightItemsList()).getExpressions()) {
			expr.accept(this);
		}
		// IN expressions results in Boolean
		outputType.put(arg0, SQLtype.BOOL);
		// Scope does not change, compared to components
		outputScope.put(arg0, outputScope.get(arg0.getLeftExpression()));
	}

	@Override
	public void visit(IsNullExpression arg0) {
		arg0.getLeftExpression().accept(this);
		outputType.put(arg0, SQLtype.BOOL);
		ExpressionScope scope = outputScope.get(arg0.getLeftExpression());
		outputScope.put(arg0, scope);
	}

	@Override
	public void visit(LikeExpression arg0) {
		// Process left expression
		Expression leftExpression = arg0.getLeftExpression();
		leftExpression.accept(this);
		SQLtype leftType = outputType.get(leftExpression);
		ExpressionScope scope = outputScope.get(leftExpression);
		// Cast from string codes to strings may be necessary
		if (leftType.equals(SQLtype.STRING_CODE)) {
			CastExpression cast = new CastExpression();
			cast.setLeftExpression(leftExpression);
			ColDataType colDataType = new ColDataType();
			colDataType.setDataType("text");
			cast.setType(colDataType);
			arg0.setLeftExpression(cast);
			outputType.put(cast, SQLtype.STRING);
			outputScope.put(cast, scope);
		}
		// Process right expression
		arg0.getRightExpression().accept(this);
		// Set result type and scope
		outputType.put(arg0, SQLtype.BOOL);
		outputScope.put(arg0, scope);
	}

	@Override
	public void visit(MinorThan arg0) {
		treatBinaryComparison(arg0);
	}

	@Override
	public void visit(MinorThanEquals arg0) {
		treatBinaryComparison(arg0);
	}

	@Override
	public void visit(NotEqualsTo arg0) {
		treatBinaryComparison(arg0);
	}

	@Override
	public void visit(Column arg0) {
		String alias = arg0.getTable().getName();
		String tableName = queryInfo.aliasToTable.get(alias);
		// Check whether table was resolved
		if (tableName==null) {
			sqlExceptions.add(new SQLexception("Error - "
					+ "table alias " + alias + " not known"));
		}
		String columnName = arg0.getColumnName();
		ColumnRef columnRef = new ColumnRef(tableName, columnName);
		ColumnInfo column = CatalogManager.getColumn(columnRef);
		// Check whether column was resolved
		if (column==null) {
			sqlExceptions.add(new SQLexception("Error - "
					+ "column reference " + columnRef + " "
							+ "was not resolved"));
		}
		outputType.put(arg0, column.type);
		outputScope.put(arg0, ExpressionScope.PER_TUPLE);
	}

	@Override
	public void visit(SubSelect arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(CaseExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(WhenClause arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(ExistsExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(AllComparisonExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(AnyComparisonExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Concat arg0) {
		treatBinaryArithmetic(arg0);
	}

	@Override
	public void visit(Matches arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BitwiseAnd arg0) {
		treatBinaryArithmetic(arg0);
	}

	@Override
	public void visit(BitwiseOr arg0) {
		treatBinaryArithmetic(arg0);
	}

	@Override
	public void visit(BitwiseXor arg0) {
		treatBinaryArithmetic(arg0);
	}

	@Override
	public void visit(CastExpression arg0) {
		arg0.getLeftExpression().accept(this);
		SQLtype type = TypeUtil.parseString(
				arg0.getType().getDataType());
		ExpressionScope scope = outputScope.get(
				arg0.getLeftExpression());
		outputType.put(arg0, type);		
		outputScope.put(arg0, scope);
	}

	@Override
	public void visit(Modulo arg0) {
		treatBinaryArithmetic(arg0);
	}

	@Override
	public void visit(AnalyticExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(WithinGroupExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(ExtractExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(IntervalExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(OracleHierarchicalExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(RegExpMatchOperator arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(JsonExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(JsonOperator arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(RegExpMySQLOperator arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(UserVariable arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(NumericBind arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(KeepExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(MySQLGroupConcat arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(RowConstructor arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(OracleHint arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(TimeKeyExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(DateTimeLiteralExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(NotExpression arg0) {
		arg0.getExpression().accept(this);
		outputType.put(arg0, SQLtype.BOOL);
		ExpressionScope scope = outputScope.get(arg0.getExpression());
		outputScope.put(arg0, scope);
	}
}
