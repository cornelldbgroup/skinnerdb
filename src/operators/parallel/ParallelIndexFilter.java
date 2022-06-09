package operators.parallel;

import buffer.BufferManager;
import com.google.common.primitives.Ints;
import config.ParallelConfig;
import expressions.ExpressionInfo;
import expressions.normalization.PlainVisitor;
import indexing.Index;
import joining.parallel.indexing.IntPartitionIndex;
import joining.parallel.indexing.PartitionIndex;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import predicate.NonEquiNode;
import predicate.Operator;
import predicate.PreNode;
import query.ColumnRef;
import query.QueryInfo;
import query.SQLexception;
import types.SQLtype;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Uses all applicable indices to evaluate a unary
 * predicate and returns set of qualifying row
 * indices. The index filter should only be applied
 * to expressions that pass the IndexTest.
 * 
 * @author Anonymous
 *
 */
public class ParallelIndexFilter extends PlainVisitor {
	/**
	 * Query to which index filter is applied.
	 */
	final QueryInfo query;
	/**
	 * Contains indexes of all rows satisfying
	 * the predicate.
	 */
	public final Deque<List<Integer>> qualifyingRows =
			new ArrayDeque<>();
	/**
	 * Contains last extracted integer constants.
	 */
	final Deque<Integer> extractedConstants =
			new ArrayDeque<>();
	/**
	 * Contains nodes for preprocessing.
	 */
	public final Deque<PreNode> preNodes =
			new ArrayDeque<>();
	/**
	 * Contains indexes applicable
	 * for sub-expressions.
	 */
	final Deque<Index> applicableIndices =
			new ArrayDeque<>();
	/**
	 * Contains indexes applicable
	 * for sub-expressions.
	 */
	final Deque<Boolean> fullResults =
			new ArrayDeque<>();
	/**
	 * The type of returned results:
	 * 0: full results.
	 * 1: range.
	 * 2: equal positions.
	 */
	public int resultType = 0;
	/**
	 * Last filtering index.
	 */
	public Index lastIndex;
	/**
	 * Remaining unary predicate information
	 */
	public ExpressionInfo remainingInfo;

	/**
	 * Initialize index filter for given query.
	 *
	 * @param query	meta-data on evaluated query
	 */
	public ParallelIndexFilter(QueryInfo query) {
		this.query = query;
	}
	
	@Override
	public void visit(AndExpression and) {
		and.getLeftExpression().accept(this);
		PreNode leftNode = preNodes.pop();
		and.getRightExpression().accept(this);
		PreNode rightNode = preNodes.pop();
		int[] rows = new int[1];
		boolean leftBetter = leftNode.cardinality < rightNode.cardinality;
		PartitionIndex index = leftBetter ? leftNode.index : rightNode.index;
		rows[0] = leftBetter ? leftNode.rows[0] : rightNode.rows[0];
		int cardinality = Math.min(leftNode.cardinality, rightNode.cardinality);
		Expression expression = leftBetter ? leftNode.expression : rightNode.expression;
		if (rightNode.operator == Operator.GreaterThan && leftNode.operator == Operator.GreaterThan) {
			rows = new int[2];
			int first = Math.max(leftNode.rows[0], rightNode.rows[0]);
			int last = Math.min(leftNode.rows[1], rightNode.rows[1]);
			rows[0] = first;
			rows[1] = last;
			cardinality = last - first;
			index = leftNode.index;
			expression = and;
		}
		PreNode preNode = new PreNode(leftNode, rightNode, index, Operator.AND, rows, cardinality, expression);
		preNodes.push(preNode);
	}
	
	@Override
	public void visit(OrExpression or) {
		or.getLeftExpression().accept(this);
		PreNode leftNode = preNodes.pop();
		or.getRightExpression().accept(this);
		PreNode rightNode = preNodes.pop();
		int[] rows = new int[0];
		int cardinality = leftNode.cardinality + rightNode.cardinality;
		PreNode preNode = new PreNode(leftNode, rightNode, null, Operator.OR, rows, cardinality, or);
		preNodes.push(preNode);
	}
	
	@Override
	public void visit(EqualsTo equalsTo) {
		equalsTo.getLeftExpression().accept(this);
		equalsTo.getRightExpression().accept(this);
		// We assume predicate passed the index test so
		// there must be one index and one constant.
		int constant = extractedConstants.pop();
		IntPartitionIndex index = (IntPartitionIndex) applicableIndices.pop();
		int startPos = index.keyToPositions.getOrDefault(constant, -1);
		int cardinality = index.unique ? 1 : index.positions[startPos];
		int[] rows = new int[0];
		// Check size
		if (startPos >= 0) {
			rows = new int[]{startPos};
		}
		else {
			qualifyingRows.push(new ArrayList<>());
		}
		PreNode preNode = new PreNode(null, null, index, Operator.EqualsTo,
				rows, cardinality, equalsTo);
		preNodes.push(preNode);
	}
	
	@Override
	public void visit(LongValue longValue) {
		extractedConstants.push((int)longValue.getValue());
	}

	@Override
	public void visit(DateTimeLiteralExpression literal) {
		DateValue dateVal = new DateValue(literal.getValue());
		int unixTime = (int)(dateVal.getValue().getTime()/1000);
		extractedConstants.push(unixTime);
	}

	@Override
	public void visit(IntervalExpression interval) {
		// Extract parameter value
		String param = interval.getParameter();
		String strVal = param.substring(1, param.length()-1);
		int intVal = Integer.parseInt(strVal);
		// Treat according to interval type
		String intervalType = interval.getIntervalType().toLowerCase();
		switch (intervalType) {
			case "year":
				extractedConstants.push(intVal * 12);
				break;
			case "month":
				extractedConstants.push(intVal);
				break;
			case "day":
				int daySecs = 24 * 60 * 60 * intVal;
				extractedConstants.push(daySecs);
				break;
			case "hour":
				int hourSecs = 60 * 60 * intVal;
				extractedConstants.push(hourSecs);
				break;
			case "minute":
				int minuteSecs = 60 * intVal;
				extractedConstants.push(minuteSecs);
				break;
			case "second":
				extractedConstants.push(intVal);
				break;
			default:
				System.out.println("Error - unknown interval type");
		}
	}
	
	@Override
	public void visit(StringValue stringValue) {
		// String must be in dictionary due to index test
		String strVal = stringValue.getValue();
		int code = BufferManager.dictionary.getCode(strVal);
		extractedConstants.push(code);
	}
	
	@Override
	public void visit(Column column) {
		// Resolve column reference
		String aliasName = column.getTable().getName();
		String tableName = query.aliasToTable.get(aliasName);
		String columnName = column.getColumnName();
		ColumnRef colRef = new ColumnRef(tableName, columnName);
		// Check for available index
		Index index = BufferManager.colToIndex.get(colRef);
		if (index != null) {
			applicableIndices.push(index);
		}
	}

	@Override
	public void visit(GreaterThan greaterThan) {
		findRange(greaterThan, 0);
		fullResults.push(false);
	}

	@Override
	public void visit(GreaterThanEquals greaterThanEquals) {
		findRange(greaterThanEquals, 1);
		fullResults.push(false);
	}

	@Override
	public void visit(MinorThan minorThan) {
		findRange(minorThan, 2);
		fullResults.push(false);
	}

	@Override
	public void visit(MinorThanEquals minorThanEquals) {
		findRange(minorThanEquals, 3);
		fullResults.push(false);
	}

	public void findRange(ComparisonOperator comparisonOperator, int op) {
		comparisonOperator.getLeftExpression().accept(this);
		comparisonOperator.getRightExpression().accept(this);
		// We assume predicate passed the index test so
		// there must be one index and one constant.
		int constant = extractedConstants.pop();
		PartitionIndex index = (PartitionIndex) applicableIndices.pop();
		// Collect indices of satisfying rows via index
		int last = index.cardinality;
		int lowerBound = 0;
		int upperBound = last - 1;
		int first = 0;
		int[] data;
		data = ((IntPartitionIndex)index).intData.data;
		while (upperBound - lowerBound > 1) {
			int middle = lowerBound + (upperBound - lowerBound) / 2;
			int rid = index.sortedRow[middle];
			if (data[rid] > constant) {
				upperBound = middle;
			} else if (data[rid] < constant) {
				lowerBound = middle;
			}
			else if (op == 1 || op == 2){
				upperBound = middle;
			}
			else {
				lowerBound = middle;
			}
		}
		boolean found = false;
		// Get next tuple
		for (int pos = lowerBound; pos <= upperBound; ++pos) {
			int rid = index.sortedRow[pos];
			int value = data[rid];
			switch (op) {
				case 0: {
					if (value > constant) {
						first = pos;
						found = true;
					}
					break;
				}
				case 1: {
					if (value >= constant) {
						first = pos;
						found = true;
					}
					break;
				}
				case 2: {
					if (value < constant) {
						last = pos + 1;
						found = true;
					}
					break;
				}
				default: {
					if (value <= constant) {
						last = pos + 1;
						found = true;
					}
					break;
				}
			}
		}
		int[] rows = new int[0];
		int cardinality = 0;
		if (found) {
			cardinality = last - first;
			rows = new int[]{first, last};
		}
		PreNode preNode = new PreNode(null, null,
				index, Operator.GreaterThan, rows, cardinality, comparisonOperator);
		preNodes.push(preNode);
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
			int leftConstant = extractedConstants.pop();
			right.accept(this);
			int rightConstant = extractedConstants.pop();

			// Multiply number of months by -1 for subtraction
			if (arg0 instanceof Subtraction) {
				rightConstant = rightConstant * -1;
			}
			extractedConstants.push(addMonths(leftConstant, rightConstant));
			return true;
		} else {
			return false;
		}
	}

	@Override
	public void visit(Addition addition) {
		if (!treatAsMonthArithmetic(addition)) {
			addition.getLeftExpression().accept(this);
			int c1 = extractedConstants.pop();
			addition.getRightExpression().accept(this);
			int c2 = extractedConstants.pop();
			extractedConstants.push(c1 + c2);
		}
	}

	@Override
	public void visit(Division division) {
		division.getLeftExpression().accept(this);
		int c1 = extractedConstants.pop();
		division.getRightExpression().accept(this);
		int c2 = extractedConstants.pop();
		extractedConstants.push(c1 / c2);
	}

	@Override
	public void visit(Multiplication multiplication) {
		multiplication.getLeftExpression().accept(this);
		int c1 = extractedConstants.pop();
		multiplication.getRightExpression().accept(this);
		int c2 = extractedConstants.pop();
		extractedConstants.push(c1 * c2);
	}

	@Override
	public void visit(Subtraction subtraction) {
		if (!treatAsMonthArithmetic(subtraction)) {
			subtraction.getLeftExpression().accept(this);
			int c1 = extractedConstants.pop();
			subtraction.getRightExpression().accept(this);
			int c2 = extractedConstants.pop();
			extractedConstants.push(c1 - c2);
		}
	}

	@Override
	public void visit(CastExpression castExpression) {
		// Get source and target type
		PreNode preNode = new PreNode(null, null, null,
				Operator.Constant, new int[0], Integer.MAX_VALUE, castExpression);
		preNodes.push(preNode);
	}
}
