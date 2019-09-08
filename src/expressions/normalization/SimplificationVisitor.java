package expressions.normalization;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import buffer.BufferManager;
import data.Dictionary;
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
import query.where.WhereUtil;

/**
 * Rewrites the original SQL query into a simplified query.
 * Tasks include;
 * - Rewriting certain SQL constructs (e.g., IN, BETWEEN) in terms
 * 		of simpler constructs that are directly supported by the
 * 		predicate compiler.
 * - Pre-calculating expressions that contain only constants
 * 		(avoids recalculating them for each tuple at run time).
 * 
 * 
 * @author immanueltrummer
 *
 */
public class SimplificationVisitor extends SkinnerVisitor {
	/**
	 * Rewritten query fragments - finally contains rewritten query.
	 */
	public Deque<Expression> opStack = new ArrayDeque<>(); 

	@Override
	public void visit(NullValue arg0) {
		opStack.push(arg0);
	}

	@Override
	public void visit(Function arg0) {
		// Visit function parameter expressions
		List<Expression> newParams = new ArrayList<Expression>();
		ExpressionList paramList = arg0.getParameters();
		if (paramList != null) {
			List<Expression> params = paramList.getExpressions();
			for (Expression param : params) {
				param.accept(this);
			}
			// Combine rewritten operands in expression list
			int nrParams = params.size();
			for (int i=0; i<nrParams; ++i) {
				newParams.add(0, opStack.pop());
			}			
		}
		// Create new function expression and push on the stack
		Function newFunction = new Function();
		newFunction.setDistinct(arg0.isDistinct());
		newFunction.setEscaped(arg0.isEscaped());
		newFunction.setKeep(arg0.getKeep());
		// Certain standard functions are rewritten into base functions
		String fctName = arg0.getName().toLowerCase();
		if (fctName.equals("count")) {
			newFunction.setName("sum");
			newFunction.setAllColumns(false);
			List<Expression> sumParams = new ArrayList<>();
			// Do we count all rows?
			if (arg0.isAllColumns()) {
				sumParams.add(new LongValue(1));
			} else {
				IsNullExpression countRowCondition = new IsNullExpression();
				countRowCondition.setNot(true);
				countRowCondition.setLeftExpression(newParams.get(0));
				WhenClause whenClause = new WhenClause();
				whenClause.setWhenExpression(countRowCondition);
				List<Expression> whenExprs = new ArrayList<>();
				whenExprs.add(whenClause);
				whenClause.setThenExpression(new LongValue(1));
				CaseExpression caseClause = new CaseExpression();
				caseClause.setWhenClauses(whenExprs);
				caseClause.setElseExpression(new LongValue(0));
				sumParams.add(caseClause);
			}
			newFunction.setParameters(new ExpressionList(sumParams));
			// Need to encapsulate to treat special case of zero count
			IsNullExpression isResultNull = new IsNullExpression();
			isResultNull.setLeftExpression(newFunction);
			WhenClause whenResultNull = new WhenClause();
			whenResultNull.setWhenExpression(isResultNull);
			whenResultNull.setThenExpression(new LongValue(0));
			List<Expression> resultNullWhens = new ArrayList<>();
			resultNullWhens.add(whenResultNull);
			CaseExpression caseResultNull = new CaseExpression();
			caseResultNull.setWhenClauses(resultNullWhens);
			caseResultNull.setElseExpression(newFunction);
			opStack.push(caseResultNull);
		} else if (fctName.equals("avg")) {
			// Sum over average input expression and cast to double
			newFunction.setName("sum");
			newFunction.setAllColumns(false);
			newFunction.setParameters(new ExpressionList(newParams));
			CastExpression newCast = new CastExpression();
			newCast.setLeftExpression(newFunction);
			ColDataType doubleType = new ColDataType();
			doubleType.setDataType("double");
			newCast.setType(doubleType);
			// Divide by the count of average input
			Function divisorFct = new Function();
			divisorFct.setAllColumns(false);
			divisorFct.setDistinct(false);
			divisorFct.setEscaped(false);
			divisorFct.setName("count");
			divisorFct.setParameters(new ExpressionList(newParams));
			Division division = new Division();
			division.setLeftExpression(newCast);
			division.setRightExpression(divisorFct);
			// Still need to rewrite the count statement
			division.accept(this);
		} else {
			newFunction.setName(arg0.getName());
			newFunction.setAllColumns(arg0.isAllColumns());	
			newFunction.setParameters(new ExpressionList(newParams));
			opStack.push(newFunction);
		}
	}

	@Override
	public void visit(SignedExpression arg0) {
		arg0.getExpression().accept(this);
		opStack.push(new SignedExpression(
				arg0.getSign(), opStack.pop()));
	}

	@Override
	public void visit(JdbcParameter arg0) {
		opStack.push(arg0);
	}

	@Override
	public void visit(JdbcNamedParameter arg0) {
		opStack.push(arg0);
	}

	@Override
	public void visit(DoubleValue arg0) {
		opStack.push(arg0);
	}

	@Override
	public void visit(LongValue arg0) {
		opStack.push(arg0);
	}

	@Override
	public void visit(HexValue arg0) {
		opStack.push(arg0);
	}

	@Override
	public void visit(DateValue arg0) {
		opStack.push(arg0);
	}

	@Override
	public void visit(TimeValue arg0) {
		opStack.push(arg0);
	}

	@Override
	public void visit(TimestampValue arg0) {
		opStack.push(arg0);
	}

	@Override
	public void visit(Parenthesis arg0) {
		arg0.getExpression().accept(this);
		Parenthesis newParenthesis = new Parenthesis(opStack.pop());
		opStack.push(newParenthesis);
	}

	@Override
	public void visit(StringValue arg0) {
		opStack.push(arg0);
	}
	/**
	 * Tries to resolve an arithmetic expression with
	 * constant operands (to avoid recalculating it
	 * for each tuple at run time).
	 * 
	 * @param oldBinaryOp	original binary operation expression
	 * @param newBinaryOp	new binary operation expression -
	 * 						used to infer type of arithmetic
	 * 						operation and as default result.
	 */
	void treatBinaryArithmetic(BinaryExpression oldBinaryOp, 
			BinaryExpression newBinaryOp) {
		// Recursive invocation fills operand stack
		oldBinaryOp.getLeftExpression().accept(this);
		oldBinaryOp.getRightExpression().accept(this);
		// Obtain rewritten operands from stack
		Expression op2 = opStack.pop();
		Expression op1 = opStack.pop();
		// Try to resolve operations on constants
		if (op1 instanceof NullValue || op2 instanceof NullValue) {
			opStack.push(new NullValue());
		} else if (op1 instanceof LongValue && op2 instanceof LongValue) {
			// Resolve operation on two constants of type long
			long longVal1 = ((LongValue)op1).getValue();
			long longVal2 = ((LongValue)op2).getValue();
			if (newBinaryOp instanceof Addition) {
				opStack.push(new LongValue(
						longVal1 + longVal2));
			} else if (newBinaryOp instanceof Subtraction) {
				opStack.push(new LongValue(
						longVal1 - longVal2));
			} else if (newBinaryOp instanceof Multiplication) {
				opStack.push(new LongValue(
						longVal1 * longVal2));
			} else if (newBinaryOp instanceof Division) {
				opStack.push(new LongValue(
						longVal1 / longVal2));
			} else if (newBinaryOp instanceof Modulo) {
				opStack.push(new LongValue(
						longVal1 % longVal2));
			} else {
				newBinaryOp.setLeftExpression(op1);
				newBinaryOp.setRightExpression(op2);
				opStack.push(newBinaryOp);
			}
		} else if (op1 instanceof DoubleValue && op2 instanceof DoubleValue) {
			// Resolve operation on two constants of type double
			double doubleVal1 = ((DoubleValue)op1).getValue();
			double doubleVal2 = ((DoubleValue)op2).getValue();
			DoubleValue result = new DoubleValue("0");
			if (newBinaryOp instanceof Addition) {
				result.setValue(doubleVal1 + doubleVal2);
				opStack.push(result);
			} else if (newBinaryOp instanceof Subtraction) {
				result.setValue(doubleVal1 - doubleVal2);
				opStack.push(result);
			} else if (newBinaryOp instanceof Multiplication) {
				result.setValue(doubleVal1 * doubleVal2);
				opStack.push(result);
			} else if (newBinaryOp instanceof Division) {
				result.setValue(doubleVal1 / doubleVal2);
				opStack.push(result);
			} else if (newBinaryOp instanceof Modulo) {
				result.setValue(doubleVal1 % doubleVal2);
				opStack.push(result);
			} else {
				newBinaryOp.setLeftExpression(op1);
				newBinaryOp.setRightExpression(op2);
				opStack.push(newBinaryOp);
			}
		} else {
			newBinaryOp.setLeftExpression(op1);
			newBinaryOp.setRightExpression(op2);
			opStack.push(newBinaryOp);
		}
	}

	@Override
	public void visit(Addition arg0) {
		Addition newAddition = new Addition();
		treatBinaryArithmetic(arg0, newAddition);
	}

	@Override
	public void visit(Division arg0) {
		Division newDivision = new Division();
		treatBinaryArithmetic(arg0, newDivision);
	}

	@Override
	public void visit(Multiplication arg0) {
		Multiplication newMultiplication = new Multiplication();
		treatBinaryArithmetic(arg0, newMultiplication);
	}

	@Override
	public void visit(Subtraction arg0) {
		Subtraction newSubtraction = new Subtraction();
		treatBinaryArithmetic(arg0, newSubtraction);
	}

	@Override
	public void visit(AndExpression arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		// Obtain rewritten expressions from stack
		Expression op2 = opStack.pop();
		Expression op1 = opStack.pop();
		// Try to resolve constants before run time
		if (op1 instanceof LongValue && op2 instanceof LongValue) {
			long val1 = ((LongValue)op1).getValue();
			long val2 = ((LongValue)op2).getValue();
			opStack.push(new LongValue(val1 * val2));
		} else if (op1 instanceof NullValue && op2 instanceof NullValue) {
			opStack.push(new NullValue());
		} else {
			opStack.push(new AndExpression(op1, op2));
		}
	}
	/**
	 * Separates a list of input expressions into expressions
	 * that match one element in another given list of expressions
	 * (textual matching) and the remaining elements.
	 * 
	 * @param inputs		triage those expressions
	 * @param comparisons	compare input against those expressions
	 * @param matches		will contain input matching comparison list
	 * @param noMatches		will contain input not matching comparison
	 */
	void triageByComparison(List<Expression> inputs, List<Expression> comparisons, 
			List<Expression> matches, List<Expression> nonMatches) {
		// Prepare fast string comparisons
		Set<String> comparisonStrs = new HashSet<>();
		for (Expression comparison : comparisons) {
			comparisonStrs.add(comparison.toString());
		}
		// Triage input expressions
		Iterator<Expression> inputIter = inputs.iterator();
		while (inputIter.hasNext()) {
			Expression input = inputIter.next();
			if (comparisonStrs.contains(input.toString())) {
				matches.add(input);
			} else {
				nonMatches.add(input);
			}
		}
	}

	@Override
	public void visit(OrExpression arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		// Obtain rewritten expressions from stack
		Expression op2 = opStack.pop();
		Expression op1 = opStack.pop();
		// Try to resolve constants before run time
		if (op1 instanceof LongValue && op2 instanceof LongValue) {
			long val1 = ((LongValue)op1).getValue();
			long val2 = ((LongValue)op2).getValue();
			opStack.push(new LongValue(Math.max(val1, val2)));
		} else if (op1 instanceof NullValue && op2 instanceof NullValue) {
			opStack.push(new NullValue());
		} else {
			// Check whether we can extract common conjuncts
			// that appear on both sides of disjunction.
			List<Expression> leftConjuncts = new ArrayList<>();
			List<Expression> rightConjuncts = new ArrayList<>();
			WhereUtil.extractConjuncts(op1, leftConjuncts);
			WhereUtil.extractConjuncts(op2, rightConjuncts);
			// Triage conjuncts into common and unique expressions
			List<Expression> leftUnique = new ArrayList<>();
			List<Expression> leftCommon = new ArrayList<>();
			List<Expression> rightUnique = new ArrayList<>();
			List<Expression> rightCommon = new ArrayList<>();
			triageByComparison(leftConjuncts, rightConjuncts, 
					leftCommon, leftUnique);
			triageByComparison(rightConjuncts, leftConjuncts, 
					rightCommon, rightUnique);
			// Did we find common expressions?
			if (leftCommon.isEmpty()) {
				opStack.push(new OrExpression(op1, op2));				
			} else {
				Expression andLeft = WhereUtil.conjunction(leftUnique);
				Expression andRight = WhereUtil.conjunction(rightUnique);
				// Create conjunction between non-unique predicates,
				// simplify if possible.
				Expression newOr = null;
				if (andLeft != null && andRight != null) {
					newOr = new OrExpression(andLeft, andRight);
				} else if (andLeft == null && andRight != null) {
					newOr = andRight;
				} else if (andLeft != null && andRight == null) {
					newOr = andLeft;
				}
				// Create conjunction between unique and non-uniqe
				// parts, simplify if possible.
				Expression andCommon = WhereUtil.conjunction(leftCommon);
				Expression newAnd = newOr==null?andCommon:
						new AndExpression(andCommon, newOr);
				opStack.push(newAnd);
			}
		}
	}

	@Override
	public void visit(Between arg0) {
		GreaterThanEquals gte = new GreaterThanEquals();
		gte.setLeftExpression(arg0.getLeftExpression());
		gte.setRightExpression(arg0.getBetweenExpressionStart());
		MinorThanEquals mte = new MinorThanEquals();
		mte.setLeftExpression(arg0.getLeftExpression());
		mte.setRightExpression(arg0.getBetweenExpressionEnd());
		AndExpression and = new AndExpression(gte, mte);
		and.accept(this);
	}
	/**
	 * Rewrites a binary comparison expression and tries
	 * to resolve comparisons with constants.
	 * 
	 * @param oldBinaryCmp	old (=non-rewritten) comparison
	 * @param newBinaryCmp	empty shell for producing new comparison
	 */
	void treatBinaryComparison(BinaryExpression oldBinaryCmp, 
			BinaryExpression newBinaryCmp) {
		// Rewrite operands
		oldBinaryCmp.getLeftExpression().accept(this);
		oldBinaryCmp.getRightExpression().accept(this);
		Expression op2 = opStack.pop();
		Expression op1 = opStack.pop();
		// Try to resolve to constant expression
		if (op1 instanceof NullValue || op2 instanceof NullValue) {
			opStack.push(new NullValue());
		} else if (op1 instanceof LongValue && op2 instanceof LongValue) {
			long val1 = ((LongValue)op1).getValue();
			long val2 = ((LongValue)op2).getValue();
			if (newBinaryCmp instanceof EqualsTo) {
				opStack.push(new LongValue(val1 == val2?1:0));
			} else if (newBinaryCmp instanceof GreaterThan) {
				opStack.push(new LongValue(val1 > val2?1:0));
			} else if (newBinaryCmp instanceof GreaterThanEquals) {
				opStack.push(new LongValue(val1 >= val2?1:0));
			} else if (newBinaryCmp instanceof MinorThan) {
				opStack.push(new LongValue(val1 < val2?1:0));
			} else if (newBinaryCmp instanceof MinorThanEquals) {
				opStack.push(new LongValue(val1 <= val2?1:0));
			} else if (newBinaryCmp instanceof NotEqualsTo) {
				opStack.push(new LongValue(val1 != val2?1:0));
			} else {
				newBinaryCmp.setLeftExpression(op1);
				newBinaryCmp.setRightExpression(op2);
				opStack.push(newBinaryCmp);
			}
		} else if (op1 instanceof DoubleValue && op2 instanceof DoubleValue) {
			double val1 = ((DoubleValue)op1).getValue();
			double val2 = ((DoubleValue)op2).getValue();
			if (newBinaryCmp instanceof EqualsTo) {
				opStack.push(new LongValue(val1 == val2?1:0));
			} else if (newBinaryCmp instanceof GreaterThan) {
				opStack.push(new LongValue(val1 > val2?1:0));
			} else if (newBinaryCmp instanceof GreaterThanEquals) {
				opStack.push(new LongValue(val1 >= val2?1:0));
			} else if (newBinaryCmp instanceof MinorThan) {
				opStack.push(new LongValue(val1 < val2?1:0));
			} else if (newBinaryCmp instanceof MinorThanEquals) {
				opStack.push(new LongValue(val1 <= val2?1:0));
			} else if (newBinaryCmp instanceof NotEqualsTo) {
				opStack.push(new LongValue(val1 != val2?1:0));
			} else {
				newBinaryCmp.setLeftExpression(op1);
				newBinaryCmp.setRightExpression(op2);
				opStack.push(newBinaryCmp);
			}
		} else if (oldBinaryCmp instanceof EqualsTo && 
				((op1 instanceof StringValue && op2 instanceof Column) ||
						(op1 instanceof Column && op2 instanceof StringValue))) {
			StringValue stringVal = (StringValue)(op1 instanceof StringValue?op1:op2);
			// Is string value not in dictionary (if available)?
			Dictionary curDic = BufferManager.dictionary;
			if (curDic != null && curDic.getCode(stringVal.getValue())<0) {
				opStack.push(new LongValue(0));
			} else {
				newBinaryCmp.setLeftExpression(op1);
				newBinaryCmp.setRightExpression(op2);
				opStack.push(newBinaryCmp);
			}
		} else {
			newBinaryCmp.setLeftExpression(op1);
			newBinaryCmp.setRightExpression(op2);
			opStack.push(newBinaryCmp);
		}
	}

	@Override
	public void visit(EqualsTo arg0) {
		EqualsTo newEquals = new EqualsTo();
		treatBinaryComparison(arg0, newEquals);
	}

	@Override
	public void visit(GreaterThan arg0) {
		GreaterThan newGt = new GreaterThan();
		treatBinaryComparison(arg0, newGt);
	}

	@Override
	public void visit(GreaterThanEquals arg0) {
		GreaterThanEquals newGte = new GreaterThanEquals();
		treatBinaryComparison(arg0, newGte);
	}
	/**
	 * We transform an in expression into nested OR expressions.
	 */
	@Override
	public void visit(InExpression arg0) {
		ItemsList rightItems = arg0.getRightItemsList();
		if (rightItems instanceof ExpressionList) {
			List<Expression> exps = ((ExpressionList)rightItems).getExpressions();
			if (exps.isEmpty()) {
				// Empty list -> Always false
				opStack.push(new LongValue(0));
			} else {
				Expression prev = null;
				for (Expression exp : exps) {
					EqualsTo eq = new EqualsTo();
					eq.setLeftExpression(arg0.getLeftExpression());
					eq.setRightExpression(exp);
					if (prev != null) {
						prev = new OrExpression(prev, eq);
					} else {
						prev = eq;
					}
				}
				Parenthesis parenthesis = new Parenthesis(prev);
				parenthesis.accept(this);
			}
		} else {
			System.err.println("Unsupported IN expression");
		}
	}

	@Override
	public void visit(IsNullExpression arg0) {
		arg0.getLeftExpression().accept(this);
		Expression newLeft = opStack.pop();
		if (newLeft instanceof NullValue) {
			opStack.push(new NullValue());
		} else {
			IsNullExpression isNull = new IsNullExpression();
			isNull.setLeftExpression(newLeft);
			isNull.setNot(arg0.isNot());
			opStack.push(isNull);
		}
	}

	@Override
	public void visit(LikeExpression arg0) {
		arg0.getLeftExpression().accept(this);
		Expression newLeft = opStack.pop();
		arg0.getRightExpression().accept(this);
		Expression newRight = opStack.pop();
		LikeExpression newLike = new LikeExpression();
		newLike.setLeftExpression(newLeft);
		newLike.setRightExpression(newRight);
		newLike.setNot(arg0.isNot());
		newLike.setCaseInsensitive(arg0.isCaseInsensitive());
		newLike.setEscape(arg0.getEscape());
		opStack.push(newLike);
	}

	@Override
	public void visit(MinorThan arg0) {
		MinorThan newMt = new MinorThan();
		treatBinaryComparison(arg0, newMt);
	}

	@Override
	public void visit(MinorThanEquals arg0) {
		MinorThanEquals newMte = new MinorThanEquals();
		treatBinaryComparison(arg0, newMte);
	}

	@Override
	public void visit(NotEqualsTo arg0) {
		NotEqualsTo newNe = new NotEqualsTo();
		treatBinaryComparison(arg0, newNe);
	}

	@Override
	public void visit(Column arg0) {
		opStack.push(arg0);
	}

	@Override
	public void visit(SubSelect arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(CaseExpression arg0) {
		// Copy case expression
		CaseExpression caseCopy = new CaseExpression();
		// Copy when clauses
		List<Expression> whenExprsCopy = new ArrayList<>();
		for (Expression whenExpr : arg0.getWhenClauses()) {
			whenExpr.accept(this);
			whenExprsCopy.add(opStack.pop());
		}
		caseCopy.setWhenClauses(whenExprsCopy);
		// Copy switch expression if any
		Expression switchExpr = arg0.getSwitchExpression();
		if (switchExpr != null) {
			switchExpr.accept(this);
			Expression switchCopy = opStack.pop();
			caseCopy.setSwitchExpression(switchCopy);
		}
		// Copy else expression if any
		Expression elseExpr = arg0.getElseExpression();
		if (elseExpr != null) {
			elseExpr.accept(this);
			Expression elseCopy = opStack.pop();
			caseCopy.setElseExpression(elseCopy);
		}
		// Put copy on stack
		opStack.push(caseCopy);
	}

	@Override
	public void visit(WhenClause arg0) {
		arg0.getWhenExpression().accept(this);
		Expression whenCopy = opStack.pop();
		arg0.getThenExpression().accept(this);
		Expression thenCopy = opStack.pop();
		WhenClause clauseCopy = new WhenClause();
		clauseCopy.setWhenExpression(whenCopy);
		clauseCopy.setThenExpression(thenCopy);
		opStack.push(clauseCopy);
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
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		Expression op2 = opStack.pop();
		Expression op1 = opStack.pop();
		if (op1 instanceof NullValue || op2 instanceof NullValue) {
			opStack.push(new NullValue());
		} else if (op1 instanceof StringValue && op2 instanceof StringValue) {
			String s1 = ((StringValue)op1).getValue();
			String s2 = ((StringValue)op2).getValue();
			opStack.push(new StringValue(s1.concat(s2)));
		} else {
			Concat newConcat = new Concat();
			newConcat.setLeftExpression(op1);
			newConcat.setRightExpression(op2);
			opStack.push(newConcat);			
		}
	}

	@Override
	public void visit(Matches arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BitwiseAnd arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BitwiseOr arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BitwiseXor arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(CastExpression arg0) {
		// Simplify left expression in cast
		arg0.getLeftExpression().accept(this);
		Expression newLeft = opStack.pop();
		// Generate new casting object
		CastExpression newCast = new CastExpression();
		newCast.setLeftExpression(newLeft);
		newCast.setType(arg0.getType());
		newCast.setUseCastKeyword(arg0.isUseCastKeyword());
		opStack.push(newCast);
	}

	@Override
	public void visit(Modulo arg0) {
		Modulo newModulo = new Modulo();
		treatBinaryArithmetic(arg0, newModulo);
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
		String name = arg0.getName();
		arg0.getExpression().accept(this);
		Expression newExpression = opStack.pop();
		ExtractExpression newExtract = new ExtractExpression();
		newExtract.setName(name);
		newExtract.setExpression(newExpression);
		opStack.push(newExtract);
	}

	@Override
	public void visit(IntervalExpression arg0) {
		opStack.push(arg0);
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
		opStack.push(arg0);		
	}

	@Override
	public void visit(NotExpression arg0) {
		arg0.getExpression().accept(this);
		Expression newExp = opStack.pop();
		if (newExp instanceof NullValue) {
			opStack.push(new NullValue());
		} else if (newExp instanceof LongValue) {
			long val = ((LongValue)newExp).getValue();
			opStack.push(new LongValue(1 - val));
		} else {
			opStack.push(new NotExpression(newExp));
		}
	}

}
