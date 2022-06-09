package operators.parallel;

import buffer.BufferManager;
import data.ColumnData;
import data.DoubleData;
import data.IntData;
import expressions.SkinnerVisitor;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.SubSelect;
import query.ColumnRef;
import query.QueryInfo;
import query.SQLexception;
import types.SQLtype;

import java.util.*;

/**
 * Uses all applicable indices to evaluate a unary
 * predicate and returns set of qualifying row
 * indices. The index filter should only be applied
 * to expressions that pass the IndexTest.
 *
 * @author Anonymous
 *
 */
public class JNIParser extends SkinnerVisitor {
	/**
	 * Query to which index filter is applied.
	 */
	final QueryInfo query;
	/**
	 * A mapping from columns as they appear in the expression
	 * to columns in the database (this assignment changes
	 * over different evaluation stages of the query).
	 */
	final Map<ColumnRef, ColumnRef> columnMapping;
	/**
	 * AST information for unary predicate.
	 */
	public final List<String> jniAST = new ArrayList<>();
	/**
	 * A list of int data mentioned in the predicate.
	 */
	public final List<int[]> intColumns = new ArrayList<>();
	/**
	 * A list of double data mentioned in the predicate.
	 */
	public final List<double[]> doubleColumns = new ArrayList<>();
	/**
	 * Mapping column alias to index of data columns.
	 */
	public final Map<ColumnRef, String> dataIndexMap = new HashMap<>();
	/**
	 * Contains index of sub expression.
	 */
	public final Deque<Integer> nodeIndexes =
			new ArrayDeque<>();
	/**
	 * Contains last extracted column index.
	 */
	public final Deque<String> extractedIndex =
			new ArrayDeque<>();
	/**
	 * Contains last extracted integer constants.
	 */
	public final Deque<String> extractedConstants =
			new ArrayDeque<>();
	/**
	 * Contains last extracted number constants.
	 */
	public final Deque<Number> extractedNumbers =
			new ArrayDeque<>();
	/**
	 * Whether to treat select columns
	 */
	public boolean isSelect = false;
	/**
	 * Whether to treat having columns
	 */
	public boolean isHaving = false;
	/**
	 * Having columns
	 */
	public String havingCol = "";
	/**
	 * Whether to treat select columns
	 */
	public boolean isDouble = false;
	/**
	 * Initialize JNI filter for given query.
	 *
	 * @param query	meta-data on evaluated query
	 */
	public JNIParser(QueryInfo query, Map<ColumnRef, ColumnRef> columnMapping) {
		this.query = query;
		this.columnMapping = columnMapping;
	}

	@Override
	public void visit(AndExpression and) {
		and.getLeftExpression().accept(this);
		int leftIndex = nodeIndexes.pop();
		and.getRightExpression().accept(this);
		int rightIndex = nodeIndexes.pop();
		String expressionNode = "E(" + leftIndex + ")" + "-O(AND)-" +
				"E(" + rightIndex + ")";
		nodeIndexes.push(jniAST.size());
		jniAST.add(expressionNode);
	}

	@Override
	public void visit(OrExpression or) {
		or.getLeftExpression().accept(this);
		int leftIndex = nodeIndexes.pop();
		or.getRightExpression().accept(this);
		int rightIndex = nodeIndexes.pop();
//		String[] expressionNode = new String[]{"E(" + leftIndex + ")", "O(OR)",
//				"E(" + rightIndex + ")"};
		String expressionNode = "E(" + leftIndex + ")" + "-O(OR)-" +
				"E(" + rightIndex + ")";
		nodeIndexes.push(jniAST.size());
		jniAST.add(expressionNode);
	}

	@Override
	public void visit(Between between) {

	}

	@Override
	public void visit(EqualsTo equalsTo) {
		equalsTo.getLeftExpression().accept(this);
		equalsTo.getRightExpression().accept(this);
		String columnIndex = extractedIndex.pop();
		String constant = extractedConstants.pop();
		extractedNumbers.pop();
//		String[] expressionNode = new String[]{columnIndex, "O(EQ)",
//				constant};
		String expressionNode = columnIndex + "-O(EQ)-" + constant;
		nodeIndexes.push(jniAST.size());
		jniAST.add(expressionNode);
	}

	@Override
	public void visit(NullValue nullValue) {

	}

	@Override
	public void visit(Function function) {
		String fct = function.getName().toLowerCase();
		switch (fct) {
			case "min":
			case "max":
			case "avg":
			case "sum":
				// We have an aggregation function -
				// we expect that such expressions have been
				// evaluated before compilation is invoked.
				List<Expression> list = function.getParameters().getExpressions();
				Expression expression = list.get(0);
				expression.accept(this);
				String expressionNode = "";
				if (!nodeIndexes.isEmpty()) {
					int leftIndex = nodeIndexes.pop();
					expressionNode = "E(" + leftIndex + ")" + "-F(" + fct + ")";
				}
				else {
					expressionNode = extractedIndex.pop() + "-F(" + fct + ")";
				}
				nodeIndexes.push(jniAST.size());
				jniAST.add(expressionNode);
				break;
			case "count": {
				expressionNode = "F(" + fct + ")";
				nodeIndexes.push(jniAST.size());
				jniAST.add(expressionNode);
				break;
			}
			default:
		}
	}

	@Override
	public void visit(SignedExpression signedExpression) {

	}

	@Override
	public void visit(JdbcParameter jdbcParameter) {

	}

	@Override
	public void visit(JdbcNamedParameter jdbcNamedParameter) {

	}

	@Override
	public void visit(DoubleValue doubleValue) {
		double value = doubleValue.getValue();
		extractedNumbers.push(value);
		extractedConstants.push("V(" + value + ")");
		isDouble = true;
	}

	@Override
	public void visit(LongValue longValue) {
		int value = (int)longValue.getValue();
		extractedNumbers.push(value);
		extractedConstants.push("V(" + value + ")");
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
	public void visit(DateTimeLiteralExpression literal) {
		DateValue dateVal = new DateValue(literal.getValue());
		int unixTime = (int)(dateVal.getValue().getTime()/1000);
		extractedNumbers.push(unixTime);
		extractedConstants.push("V(" + unixTime + ")");
	}

	@Override
	public void visit(NotExpression notExpression) {
		notExpression.getExpression().accept(this);
		int leftIndex = nodeIndexes.pop();
		String expressionNode = "N(" + leftIndex + ")";
		nodeIndexes.push(jniAST.size());
		jniAST.add(expressionNode);
	}

	@Override
	public void visit(IntervalExpression interval) {
		// Extract parameter value
		String param = interval.getParameter();
		String strVal = param.substring(1, param.length()-1);
		int intVal = Integer.parseInt(strVal);
		// Treat according to interval type
		String intervalType = interval.getIntervalType().toLowerCase();
		int value = 0;
		switch (intervalType) {
			case "year":
				value = intVal * 12;
				break;
			case "month":
				value = intVal;
				break;
			case "day":
				value = 24 * 60 * 60 * intVal;
				break;
			case "hour":
				value = 60 * 60 * intVal;
				break;
			case "minute":
				value = 60 * intVal;
				break;
			case "second":
				value = intVal;
				break;
			default:
				System.out.println("Error - unknown interval type");
		}
		extractedNumbers.push(value);
		extractedConstants.push("V(" + value + ")");
	}

	@Override
	public void visit(OracleHierarchicalExpression oracleHierarchicalExpression) {

	}

	@Override
	public void visit(RegExpMatchOperator regExpMatchOperator) {

	}

	@Override
	public void visit(JsonExpression jsonExpression) {

	}

	@Override
	public void visit(JsonOperator jsonOperator) {

	}

	@Override
	public void visit(RegExpMySQLOperator regExpMySQLOperator) {

	}

	@Override
	public void visit(UserVariable userVariable) {

	}

	@Override
	public void visit(NumericBind numericBind) {

	}

	@Override
	public void visit(KeepExpression keepExpression) {

	}

	@Override
	public void visit(MySQLGroupConcat mySQLGroupConcat) {

	}

	@Override
	public void visit(RowConstructor rowConstructor) {

	}

	@Override
	public void visit(OracleHint oracleHint) {

	}

	@Override
	public void visit(TimeKeyExpression timeKeyExpression) {

	}

	@Override
	public void visit(StringValue stringValue) {
		// String must be in dictionary due to index test
		String strVal = stringValue.getValue();
		int code = BufferManager.dictionary.getCode(strVal);
		extractedNumbers.push(code);
		extractedConstants.push("V(" + code + ")");
	}

	@Override
	public void visit(Column column) {
		// Resolve column reference
		String aliasName = column.getTable().getName();
		String columnName = column.getColumnName();
		ColumnRef colRef = new ColumnRef(aliasName, columnName);
		colRef = columnMapping.get(colRef);
		// Check for available data column
		if (!dataIndexMap.containsKey(colRef)) {
			try {
				ColumnData data = BufferManager.getData(colRef);
				int size = 0;
				if (data instanceof IntData) {
					size = intColumns.size();
					IntData intData = (IntData) data;
					intColumns.add(intData.data);
					String value = "IC(" + size + ")";
					dataIndexMap.put(colRef, value);
					extractedIndex.push(value);
				}
				else {
					size = doubleColumns.size();
					DoubleData doubleData = (DoubleData) data;
					doubleColumns.add(doubleData.data);
					String value = "DC(" + size + ")";
					dataIndexMap.put(colRef, value);
					extractedIndex.push(value);
					isDouble = true;
				}

			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		else {
			extractedIndex.push(dataIndexMap.get(colRef));
		}
	}

	@Override
	public void visit(SubSelect subSelect) {

	}

	@Override
	public void visit(CaseExpression caseExpression) {

	}

	@Override
	public void visit(WhenClause whenClause) {

	}

	@Override
	public void visit(ExistsExpression existsExpression) {

	}

	@Override
	public void visit(AllComparisonExpression allComparisonExpression) {

	}

	@Override
	public void visit(AnyComparisonExpression anyComparisonExpression) {

	}

	@Override
	public void visit(Concat concat) {

	}

	@Override
	public void visit(Matches matches) {

	}

	@Override
	public void visit(BitwiseAnd bitwiseAnd) {

	}

	@Override
	public void visit(BitwiseOr bitwiseOr) {

	}

	@Override
	public void visit(BitwiseXor bitwiseXor) {

	}

	@Override
	public void visit(GreaterThan greaterThan) {
		if (isHaving) {
			Expression leftExpression = greaterThan.getLeftExpression();
			Expression rightExpression = greaterThan.getRightExpression();
			rightExpression.accept(this);
			String rightExp = rightExpression instanceof Column ?
					extractedIndex.pop() : extractedConstants.pop();
			String expressionNode = havingCol + "-O(G)-" + rightExp;
			nodeIndexes.push(jniAST.size());
			jniAST.add(expressionNode);
		}
		else {
			greaterThan.getLeftExpression().accept(this);
			String leftExp = extractedIndex.pop();
			greaterThan.getRightExpression().accept(this);
			String rightExp = greaterThan.getRightExpression() instanceof Column ?
					extractedIndex.pop() : extractedConstants.pop();
			String expressionNode = leftExp + "-O(G)-" + rightExp;
			nodeIndexes.push(jniAST.size());
			jniAST.add(expressionNode);
		}
	}

	@Override
	public void visit(GreaterThanEquals greaterThanEquals) {
		greaterThanEquals.getLeftExpression().accept(this);
		String leftExp = extractedIndex.pop();
		greaterThanEquals.getRightExpression().accept(this);
		String rightExp = greaterThanEquals.getRightExpression() instanceof Column ?
				extractedIndex.pop() : extractedConstants.pop();
//		String[] expressionNode = new String[]{leftExp, "O(GE)",
//				rightExp};
		String expressionNode = leftExp + "-O(GE)-" + rightExp;
		nodeIndexes.push(jniAST.size());
		jniAST.add(expressionNode);
	}

	@Override
	public void visit(MinorThan minorThan) {
		minorThan.getLeftExpression().accept(this);
		String leftExp = extractedIndex.pop();
		minorThan.getRightExpression().accept(this);
		String rightExp = minorThan.getRightExpression() instanceof Column ?
				extractedIndex.pop() : extractedConstants.pop();
//		String[] expressionNode = new String[]{leftExp, "O(M)",
//				rightExp};
		String expressionNode = leftExp + "-O(M)-" + rightExp;
		nodeIndexes.push(jniAST.size());
		jniAST.add(expressionNode);
	}

	@Override
	public void visit(MinorThanEquals minorThanEquals) {
		minorThanEquals.getLeftExpression().accept(this);
		String leftExp = extractedIndex.pop();
		minorThanEquals.getRightExpression().accept(this);
		String rightExp = minorThanEquals.getRightExpression() instanceof Column ?
				extractedIndex.pop() : extractedConstants.pop();
//		String[] expressionNode = new String[]{leftExp, "O(ME)",
//				rightExp};
		String expressionNode = leftExp + "-O(ME)-" + rightExp;
		nodeIndexes.push(jniAST.size());
		jniAST.add(expressionNode);
	}

	@Override
	public void visit(InExpression inExpression) {

	}

	@Override
	public void visit(IsNullExpression isNullExpression) {
		// TODO
		isNullExpression.getLeftExpression().accept(this);
		String leftExp = extractedIndex.pop();
		String operator = isNullExpression.isNot() ? "-O(NN)" : "-O(N)";
		String expressionNode = leftExp + operator;
		nodeIndexes.push(jniAST.size());
		jniAST.add(expressionNode);
	}

	@Override
	public void visit(LikeExpression likeExpression) {
		likeExpression.getLeftExpression().accept(this);
		String columnIndex = "S" + extractedIndex.pop().substring(1);
		StringValue stringValue = (StringValue) likeExpression.getRightExpression();
		String strVal = stringValue.getValue();
		String like = "V(" + strVal + ")";
		String operator = likeExpression.isNot() ? "-O(NLIKE)-" : "-O(LIKE)-";
		String expressionNode = columnIndex + operator + like;
		nodeIndexes.push(jniAST.size());
		jniAST.add(expressionNode);
	}

	@Override
	public void visit(NotEqualsTo notEqualsTo) {
		notEqualsTo.getLeftExpression().accept(this);
		notEqualsTo.getRightExpression().accept(this);
		String columnIndex = extractedIndex.pop();
		String constant = extractedConstants.pop();
		extractedNumbers.pop();
//		extractedNumbers.pop();
//		String[] expressionNode = new String[]{columnIndex, "O(NEQ)",
//				constant};
		String expressionNode = columnIndex + "-O(NEQ)-" + constant;
		nodeIndexes.push(jniAST.size());
		jniAST.add(expressionNode);
	}

	/**
	 * Returns true iff the left operand is either a date or
	 * a timestamp and the right operand is a year-month
	 * time interval.
	 *
	 * @param leftType		left type of binary expression
	 * @param rightType		right type of binary expression
	 * @return			true iff the operands require special treatment
	 */
	boolean dateYMintervalOp(SQLtype leftType, SQLtype rightType) {
		return (leftType.equals(SQLtype.DATE) ||
				leftType.equals(SQLtype.TIMESTAMP)) &&
				(rightType.equals(SQLtype.YM_INTERVAL));
	}

	SQLtype getDateType(Expression expression) {
		SQLtype sqLtype = SQLtype.ANY_TYPE;
		if (expression instanceof DateTimeLiteralExpression) {
			DateTimeLiteralExpression arg0 = (DateTimeLiteralExpression) expression;
			switch (arg0.getType()) {
				case DATE:
					sqLtype = SQLtype.DATE;
					break;
				case TIME:
					sqLtype = SQLtype.TIME;
					break;
				case TIMESTAMP:
					sqLtype = SQLtype.TIMESTAMP;
					break;
			}
		}
		else if (expression instanceof IntervalExpression) {
			IntervalExpression arg0 = (IntervalExpression) expression;
			String timeUnit = arg0.getIntervalType().toLowerCase();
			switch (timeUnit) {
				case "year":
				case "month":
					sqLtype = SQLtype.YM_INTERVAL;
					break;
				case "day":
				case "hour":
				case "minute":
				case "second":
					sqLtype = SQLtype.DT_INTERVAL;
					break;
				default:
					sqlExceptions.add(new SQLexception("Error - "
							+ "unknown time unit " + timeUnit + ". "
							+ "Allowed units are year, month, "
							+ "day, hour, minute, second."));
			}
		}
		return sqLtype;
	}
	/**
	 * Adds given number of months to a date, represented
	 * according to Unix time format.
	 *
	 * @param dateSecs	seconds since January 1st 1970
	 * @param months	number of months to add (can be negative)
	 * @return			seconds since January 1st 1970 after addition
	 */
	public static int addMonths(int dateSecs, int months) {
		Calendar calendar = Calendar.getInstance();
		try {
			calendar.setTimeInMillis((long)dateSecs * (long)1000);
			calendar.add(Calendar.MONTH, months);
		} catch (Exception e) {
			System.out.println("dateSecs: " + dateSecs);
			System.out.println("months: " + months);
			System.out.println(calendar);
			e.printStackTrace();
		}
		return (int)(calendar.getTimeInMillis()/(long)1000);
	}

	/**
	 * Adds code to treat the addition or subtraction of a
	 * number of months from a date or timestamp value.
	 * Returns true iff the given expression is indeed
	 * of that type.
	 *
	 * @param arg0	expression potentially involving months arithmetic
	 * @return		true if code for expression was added
	 */
	boolean treatAsMonthArithmetic(BinaryExpression arg0) {
		Expression left = arg0.getLeftExpression();
		Expression right = arg0.getRightExpression();

		SQLtype leftType = getDateType(left);
		SQLtype rightType = getDateType(right);
		boolean leftDate = dateYMintervalOp(leftType, rightType);
		boolean rightDate = dateYMintervalOp(rightType, leftType);

		if (leftDate || rightDate) {
			// Swap input parameters if required
			if (rightDate) {
				right = arg0.getLeftExpression();
				left = arg0.getRightExpression();
			}

			left.accept(this);
			int leftConstant = extractedNumbers.pop().intValue();
			extractedConstants.pop();
			right.accept(this);
			int rightConstant = extractedNumbers.pop().intValue();
			extractedConstants.pop();

			// Multiply number of months by -1 for subtraction
			if (arg0 instanceof Subtraction) {
				rightConstant = rightConstant * -1;
			}
			int value = addMonths(leftConstant, rightConstant);
			extractedNumbers.push(value);
			extractedConstants.push("V(" + value + ")");
			return true;
		} else {
			return false;
		}
	}

	private String buildExpressionNode() {
		String expressionNode = "";
		if (!extractedIndex.isEmpty()) {
			expressionNode = extractedIndex.pop();
		}
		else if (!extractedNumbers.isEmpty()) {
			Number n = extractedNumbers.pop();
			extractedConstants.pop();
			expressionNode = "V(" + n.toString() + ")";
		}
		else if (!nodeIndexes.isEmpty()) {
			int nodeExp = nodeIndexes.pop();
			expressionNode = "E(" + nodeExp + ")";
		}
		return expressionNode;
	}

	@Override
	public void visit(Addition addition) {
		if (!treatAsMonthArithmetic(addition)) {
			if (isSelect) {
				addition.getLeftExpression().accept(this);
				String expressionNode = buildExpressionNode();
				addition.getRightExpression().accept(this);
				expressionNode = expressionNode + "-O(+)-" + buildExpressionNode();
				nodeIndexes.push(jniAST.size());
				jniAST.add(expressionNode);
			}
			else {
				addition.getLeftExpression().accept(this);
				Number n1 = extractedNumbers.pop();
				extractedConstants.pop();
				addition.getRightExpression().accept(this);
				Number n2 = extractedNumbers.pop();
				extractedConstants.pop();
				Number n3 = n1.doubleValue() + n2.doubleValue();
				extractedNumbers.push(n3);
				extractedConstants.push("V(" + n3 + ")");
			}
		}
	}

	@Override
	public void visit(Division division) {
		if (!treatAsMonthArithmetic(division)) {
			if (isSelect) {
				division.getLeftExpression().accept(this);
				String expressionNode = buildExpressionNode();
				division.getRightExpression().accept(this);
				expressionNode = expressionNode + "-O(/)-" + buildExpressionNode();
				nodeIndexes.push(jniAST.size());
				jniAST.add(expressionNode);
			}
			else {
				division.getLeftExpression().accept(this);
				Number n1 = extractedNumbers.pop();
				extractedConstants.pop();
				division.getRightExpression().accept(this);
				Number n2 = extractedNumbers.pop();
				extractedConstants.pop();
				Number n3 = n1.doubleValue() / n2.doubleValue();
				extractedNumbers.push(n3);
				extractedConstants.push("V(" + n3 + ")");
			}
		}
	}

	@Override
	public void visit(Multiplication multiplication) {
		if (!treatAsMonthArithmetic(multiplication)) {
			// Left and Right are constant
			Expression leftExpression = multiplication.getLeftExpression();
			Expression rightExpression = multiplication.getRightExpression();
			if (isSelect) {
				leftExpression.accept(this);
				String expressionNode = buildExpressionNode();
				rightExpression.accept(this);
				expressionNode = expressionNode + "-O(*)-" + buildExpressionNode();
				nodeIndexes.push(jniAST.size());
				jniAST.add(expressionNode);
			}
			else {
				multiplication.getLeftExpression().accept(this);
				Number n1 = extractedNumbers.pop();
				extractedConstants.pop();
				multiplication.getRightExpression().accept(this);
				Number n2 = extractedNumbers.pop();
				extractedConstants.pop();
				Number n3 = n1.doubleValue() * n2.doubleValue();
				extractedNumbers.push(n3);
				extractedConstants.push("V(" + n3 + ")");
			}
		}
	}

	@Override
	public void visit(Subtraction subtraction) {
		if (!treatAsMonthArithmetic(subtraction)) {
			if (isSelect) {
				subtraction.getLeftExpression().accept(this);
				String expressionNode = buildExpressionNode();
				subtraction.getRightExpression().accept(this);
				expressionNode = expressionNode + "-O(-)-" + buildExpressionNode();
				nodeIndexes.push(jniAST.size());
				jniAST.add(expressionNode);
			}
			else {
				subtraction.getLeftExpression().accept(this);
				Number n1 = extractedNumbers.pop();
				extractedConstants.pop();
				subtraction.getRightExpression().accept(this);
				Number n2 = extractedNumbers.pop();
				extractedConstants.pop();
				Number n3 = n1.doubleValue() - n2.doubleValue();
				extractedNumbers.push(n3);
				extractedConstants.push("V(" + n3 + ")");
			}
		}
	}

	@Override
	public void visit(CastExpression castExpression) {
		castExpression.getLeftExpression().accept(this);
	}

	@Override
	public void visit(Modulo modulo) {

	}

	@Override
	public void visit(AnalyticExpression analyticExpression) {

	}

	@Override
	public void visit(WithinGroupExpression withinGroupExpression) {

	}

	@Override
	public void visit(ExtractExpression extractExpression) {
		// TODO:
		Expression expression = extractExpression.getExpression();
		String name = extractExpression.getName();
		expression.accept(this);
		String expressionNode = extractedIndex.pop() + "-T(" + name + ")";
		nodeIndexes.push(jniAST.size());
		jniAST.add(expressionNode);
	}
}
