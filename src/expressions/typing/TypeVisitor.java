package expressions.typing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import buffer.BufferManager;
import catalog.CatalogManager;
import catalog.info.ColumnInfo;
import config.NamingConfig;
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
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
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
	 * Creates a new cast expression with given input
	 * and target type. Updates type and scope data
	 * structures accordingly.
	 * 
	 * @param input			cast input expression
	 * @param targetType	target type for casting
	 * @return				newly created cast expression
	 */
	CastExpression newCast(Expression input, SQLtype targetType) {
		CastExpression cast = new CastExpression();
		cast.setLeftExpression(input);
		ColDataType colDataType = new ColDataType();
		colDataType.setDataType(targetType.toString());
		cast.setType(colDataType);
		outputType.put(cast, targetType);
		propagateScope(input, cast);
		return cast;
	}
	/**
	 * Tries to unify two different scopes, returns the
	 * unified scope if successful and null otherwise.
	 * 
	 * @param scope1	first scope to unify
	 * @param scope2	second scope to unify
	 * @return			unified scope or null if unification impossible
	 */
	ExpressionScope unifyScope(ExpressionScope scope1, 
			ExpressionScope scope2) {
		if (scope1.equals(scope2)) {
			return scope1;
		} else if (scope1.equals(ExpressionScope.ANY_SCOPE)) {
			return scope2;
		} else if (scope2.equals(ExpressionScope.ANY_SCOPE)) {
			return scope1;
		} else {
			return null;
		}
	}
	/**
	 * Propagate scope of first expression to scope of
	 * second expression - try to unify scopes if second
	 * expression is already assigned to a scope.
	 * 
	 * @param from	propagate scope of this expression
	 * @param to	propagate scope to this expression
	 */
	void propagateScope(Expression from, Expression to) {
		ExpressionScope fromScope = outputScope.get(from);
		if (fromScope == null) {
			sqlExceptions.add(new SQLexception("Error - "
					+ "no scope set for " + from));
		} else {
			ExpressionScope priorToScope = outputScope.get(to);
			// Was a scope set previously for the target expression?
			if (priorToScope == null) {
				// No prior scope - simply adopt source scope
				outputScope.put(to, fromScope);
			} else {
				// Prior scope was set - try to unify
				ExpressionScope unifiedScope = unifyScope(
						fromScope, priorToScope);
				// Throw exception if unsuccessful
				if (unifiedScope == null) {
					sqlExceptions.add(new SQLexception("Error - "
							+ "cannot unify scopes for "
							+ from + " and " + to));
				} else {
					// Register unified scope
					outputScope.put(to, unifiedScope);
				}
			}
		}
	}
	/**
	 * Add casts for inputs of binary expression if necessary.
	 * 
	 * @param binaryExpression	binary expression
	 */
	void castInputs(BinaryExpression binaryExpression) {
		Expression expression1 = binaryExpression.getLeftExpression();
		Expression expression2 = binaryExpression.getRightExpression();
		// Try to unify operand types
		SQLtype type1 = outputType.get(expression1);
		SQLtype type2 = outputType.get(expression2);
		SQLtype commonType = TypeUtil.commonType(type1, type2);
		if (commonType == null) {
			sqlExceptions.add(new SQLexception("Error - "
					+ "failed to add automated casts to "
					+ "unify types " + type1 + " and " + type2
					+ " in expression " + binaryExpression.toString()));
		}
		// Cast operands if necessary
		if (type1 != commonType) {
			CastExpression cast = newCast(expression1, commonType);
			binaryExpression.setLeftExpression(cast);
		}
		if (type2 != commonType) {
			CastExpression cast = newCast(expression2, commonType);
			binaryExpression.setRightExpression(cast);
		}
	}
	/**
	 * Returns true iff the first type is a date time type and
	 * the second type an interval type. 
	 * 
	 * @param type1		first type (potentially date or time type)
	 * @param type2		second type (potentially interval type)
	 * @return		true iff date time and interval types
	 */
	boolean datetimeInterval(SQLtype type1, SQLtype type2) {
		return (type1.equals(SQLtype.TIMESTAMP) ||
				type1.equals(SQLtype.DATE) ||
				type1.equals(SQLtype.TIME)) && 
			(type2.equals(SQLtype.DT_INTERVAL) ||
			type2.equals(SQLtype.YM_INTERVAL));	
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
		// Take care of output scope
		propagateScope(expression1, binaryExpression);
		propagateScope(expression2, binaryExpression);
		// Distinguish special case of time intervals
		if (TypeUtil.isInterval(type1) || TypeUtil.isInterval(type2)) {
			// Distinguish type of operation
			if (binaryExpression instanceof Addition) {
				if (type1.equals(type2)) {
					// Addition between two intervals of same type
					outputType.put(binaryExpression, type1);
				} else if (datetimeInterval(type1, type2)) {
					outputType.put(binaryExpression, type1);
				} else if (datetimeInterval(type2, type1)) {
					outputType.put(binaryExpression, type2);
				} else {
					sqlExceptions.add(new SQLexception("Error - "
							+ "incompatible types for interval "
							+ "addition in " + binaryExpression));
				}
			} else if (binaryExpression instanceof Subtraction) {
				if (type1.equals(type2)) {
					// Subtraction between two intervals of same type
					outputType.put(binaryExpression, type1);
				} else if (datetimeInterval(type1, type2)) {
					outputType.put(binaryExpression, type1);
				} else {
					sqlExceptions.add(new SQLexception("Error - "
							+ "incompatible types for interval "
							+ "subtraction in " + binaryExpression));
				}
			} else {
				sqlExceptions.add(new SQLexception("Error - "
						+ "expression " + binaryExpression + " is "
								+ "not admissible (interval operands "
								+ "only allow addition and subtraction)"));
			}
		} else {
			// No time intervals involved -> proceed as usual
			SQLtype commonType = TypeUtil.commonType(type1, type2);
			outputType.put(binaryExpression, commonType);
			// Add cast expressions if necessary
			castInputs(binaryExpression);			
		}
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
		propagateScope(expression1, binaryCmp);
		propagateScope(expression2, binaryCmp);
		// Add casts if necessary
		castInputs(binaryCmp);
	}

	@Override
	public void visit(NullValue arg0) {
		outputType.put(arg0, SQLtype.ANY_TYPE);
		outputScope.put(arg0, ExpressionScope.ANY_SCOPE);
	}

	@Override
	public void visit(Function arg0) {
		// Treat parameter expressions
		if (arg0.getName().equalsIgnoreCase("count")) {
			outputType.put(arg0, SQLtype.INT);
			outputScope.put(arg0, ExpressionScope.PER_GROUP);
		} else {
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
	}

	@Override
	public void visit(SignedExpression arg0) {
		Expression input = arg0.getExpression();
		input.accept(this);
		// Set output type
		SQLtype type = outputType.get(input);
		outputType.put(arg0, type);
		// Set output scope
		propagateScope(input, arg0);
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
		outputScope.put(arg0, ExpressionScope.ANY_SCOPE);
	}

	@Override
	public void visit(LongValue arg0) {
		long value = arg0.getValue();
		if (value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE) {
			outputType.put(arg0, SQLtype.INT);
		} else {
			outputType.put(arg0, SQLtype.LONG);
		}
		outputScope.put(arg0, ExpressionScope.ANY_SCOPE);
	}

	@Override
	public void visit(HexValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(DateValue arg0) {
		outputType.put(arg0, SQLtype.DATE);
		outputScope.put(arg0, ExpressionScope.ANY_SCOPE);
	}

	@Override
	public void visit(TimeValue arg0) {
		outputType.put(arg0, SQLtype.TIME);
		outputScope.put(arg0, ExpressionScope.ANY_SCOPE);
	}

	@Override
	public void visit(TimestampValue arg0) {
		outputType.put(arg0, SQLtype.TIMESTAMP);
		outputScope.put(arg0, ExpressionScope.ANY_SCOPE);
	}

	@Override
	public void visit(Parenthesis arg0) {
		Expression input = arg0.getExpression();
		input.accept(this);
		// Set result type
		SQLtype type = outputType.get(input);
		outputType.put(arg0, type);
		// Set result scope
		propagateScope(input, arg0);
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
		outputScope.put(arg0, ExpressionScope.ANY_SCOPE);
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
		propagateScope(expression1, arg0);
		propagateScope(expression2, arg0);
		propagateScope(expression3, arg0);
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
		// Check for support
		ItemsList rightList = arg0.getRightItemsList();
		if (!(rightList instanceof ExpressionList)) {
			sqlExceptions.add(new SQLexception("Error - "
					+ "no support for IN statement of RH type "
					+ rightList.getClass()));
		}
		// Treat component expressions
		Expression left = arg0.getLeftExpression();
		left.accept(this);
		for (Expression expr : ((ExpressionList)
				rightList).getExpressions()) {
			expr.accept(this);
		}
		// IN expressions results in Boolean
		outputType.put(arg0, SQLtype.BOOL);
		// Set output scope
		propagateScope(left, arg0);
		for (Expression expr : ((ExpressionList)
				rightList).getExpressions()) {
			propagateScope(expr, arg0);
		}
	}

	@Override
	public void visit(IsNullExpression arg0) {
		Expression left = arg0.getLeftExpression();
		left.accept(this);
		outputType.put(arg0, SQLtype.BOOL);
		propagateScope(left, arg0);
	}

	@Override
	public void visit(LikeExpression arg0) {
		// Process left expression
		Expression leftExpression = arg0.getLeftExpression();
		leftExpression.accept(this);
		SQLtype leftType = outputType.get(leftExpression);
		// Cast from string codes to strings may be necessary
		if (leftType.equals(SQLtype.STRING_CODE)) {
			CastExpression cast = new CastExpression();
			cast.setLeftExpression(leftExpression);
			ColDataType colDataType = new ColDataType();
			colDataType.setDataType("text");
			cast.setType(colDataType);
			arg0.setLeftExpression(cast);
			outputType.put(cast, SQLtype.STRING);
			propagateScope(leftExpression, cast);
		}
		// Process right expression
		Expression rightExpression = arg0.getRightExpression();
		rightExpression.accept(this);
		// Set result type and scope
		outputType.put(arg0, SQLtype.BOOL);
		propagateScope(leftExpression, arg0);
		propagateScope(rightExpression, arg0);
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
		// Check for special case: columns representing 
		// unnested sub-queries.
		if (tableName.startsWith(NamingConfig.SUBQUERY_PRE)) {
			outputScope.put(arg0, ExpressionScope.ANY_SCOPE);
		} else {
			outputScope.put(arg0, ExpressionScope.PER_TUPLE);
		}
	}

	@Override
	public void visit(SubSelect arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(CaseExpression arg0) {
		// Type sub-expressions and determine common output type
		SQLtype resultType = null;
		for (Expression expr : arg0.getWhenClauses()) {
			expr.accept(this);
			// Check for type and scope consistency
			SQLtype thisType = outputType.get(expr);
			if (resultType == null) {
				resultType = thisType;
			} else {
				resultType = TypeUtil.commonType(thisType, resultType);
			}
		}
		Expression elseExpr = arg0.getElseExpression();
		if (elseExpr != null) {
			elseExpr.accept(this);
			SQLtype elseType = outputType.get(elseExpr);
			resultType = TypeUtil.commonType(elseType, resultType);
		}
		// Raise exception if no common type is found
		if (resultType == null) {
			sqlExceptions.add(new SQLexception("Error - "
					+ "incompatible result types in case"
					+ "statement: " + arg0));
		}
		// Add casts to as required type if necessary
		List<Expression> castedWhens = new ArrayList<>();
		for (Expression expr : arg0.getWhenClauses()) {
			SQLtype thisType = outputType.get(expr);
			if (!thisType.equals(resultType)) {
				Expression cast = newCast(expr, resultType);
				castedWhens.add(cast);
			} else {
				castedWhens.add(expr);
			}
		}
		arg0.setWhenClauses(castedWhens);
		if (elseExpr != null) {
			SQLtype elseType = outputType.get(elseExpr);
			if (!elseType.equals(resultType)) {
				Expression cast = newCast(elseExpr, resultType);
				elseExpr = cast;
				arg0.setElseExpression(cast);
			}
		}
		// Treat switch expression if any
		Expression switchExpr = arg0.getSwitchExpression();
		if (switchExpr != null) {
			switchExpr.accept(this);
		}
		// Add result type for case block
		outputType.put(arg0, resultType);
		// Determine output scope
		for (Expression when : arg0.getWhenClauses()) {
			propagateScope(when, arg0);
		}
		if (switchExpr != null) {
			propagateScope(switchExpr, arg0);
		}
		if (elseExpr != null) {
			propagateScope(arg0.getElseExpression(), arg0);
		}
	}

	@Override
	public void visit(WhenClause arg0) {
		// Treat when expression
		Expression whenExpr = arg0.getWhenExpression();
		whenExpr.accept(this);
		// Ensure that when expression is Boolean
		if (!outputType.get(whenExpr).equals(SQLtype.BOOL)) {
			sqlExceptions.add(new SQLexception("Error - " + 
					"when expression " + whenExpr + 
					" is not Boolean"));
		}
		// Treat then expression
		Expression thenExpr = arg0.getThenExpression();
		thenExpr.accept(this);
		// Assign type for surrounding expression
		SQLtype thenType = outputType.get(thenExpr);
		outputType.put(arg0, thenType);
		// Assign scope for surrounding expression
		propagateScope(whenExpr, arg0);
		propagateScope(thenExpr, arg0);
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
		Expression input = arg0.getLeftExpression();
		input.accept(this);
		// Determine output type
		SQLtype type = TypeUtil.parseString(
				arg0.getType().getDataType());
		outputType.put(arg0, type);
		// Determine output scope
		propagateScope(input, arg0);
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
		Expression input = arg0.getExpression();
		input.accept(this);
		outputType.put(arg0, SQLtype.INT);
		propagateScope(input, arg0);
	}

	@Override
	public void visit(IntervalExpression arg0) {
		// Determine result type
		SQLtype type = null;
		String timeUnit = arg0.getIntervalType().toLowerCase();
		switch (timeUnit) {
		case "year":
		case "month":
			type = SQLtype.YM_INTERVAL;
			break;
		case "day":
		case "hour":
		case "minute":
		case "second":
			type = SQLtype.DT_INTERVAL;
			break;
		default:
			sqlExceptions.add(new SQLexception("Error - "
					+ "unknown time unit " + timeUnit + ". "
							+ "Allowed units are year, month, "
							+ "day, hour, minute, second."));
		}
		outputType.put(arg0, type);
		// Determine output scope
		outputScope.put(arg0, ExpressionScope.ANY_SCOPE);
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
		switch (arg0.getType()) {
		case DATE:
			outputType.put(arg0, SQLtype.DATE);
			break;
		case TIME:
			outputType.put(arg0, SQLtype.TIME);
			break;
		case TIMESTAMP:
			outputType.put(arg0, SQLtype.TIMESTAMP);
			break;
		}
		outputScope.put(arg0, ExpressionScope.ANY_SCOPE);
	}

	@Override
	public void visit(NotExpression arg0) {
		Expression input = arg0.getExpression();
		input.accept(this);
		outputType.put(arg0, SQLtype.BOOL);
		propagateScope(input, arg0);
	}
}
