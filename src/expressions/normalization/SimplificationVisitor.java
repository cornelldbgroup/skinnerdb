package expressions.normalization;

import buffer.BufferManager;
import data.Dictionary;
import expressions.SkinnerVisitor;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.select.SubSelect;
import query.where.WhereUtil;

import java.sql.Date;
import java.util.*;

/**
 * Rewrites the original SQL query into a simplified query.
 * Tasks include;
 * - Rewriting certain SQL constructs (e.g., IN, BETWEEN) in terms
 * of simpler constructs that are directly supported by the
 * predicate compiler.
 * - Pre-calculating expressions that contain only constants
 * (avoids recalculating them for each tuple at run time).
 *
 * @author immanueltrummer
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
            for (int i = 0; i < nrParams; ++i) {
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
            opStack.push(newFunction);
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
     * @param oldBinaryOp original binary operation expression
     * @param newBinaryOp new binary operation expression -
     *                    used to infer type of arithmetic
     *                    operation and as default result.
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
            long longVal1 = ((LongValue) op1).getValue();
            long longVal2 = ((LongValue) op2).getValue();
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
            double doubleVal1 = ((DoubleValue) op1).getValue();
            double doubleVal2 = ((DoubleValue) op2).getValue();
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
            long val1 = ((LongValue) op1).getValue();
            long val2 = ((LongValue) op2).getValue();
            opStack.push(new LongValue(val1 * val2));
        } else if (op1 instanceof NullValue && op2 instanceof NullValue) {
            opStack.push(new NullValue());
        } else if (op1 instanceof LongValue) {
            long val = ((LongValue) op1).getValue();
            if (val == 0) {
                opStack.push(op1);
            } else {
                opStack.push(op2);
            }
        } else if (op2 instanceof LongValue) {
            long val = ((LongValue) op2).getValue();
            if (val == 0) {
                opStack.push(op2);
            } else {
                opStack.push(op1);
            }
        } else {
            opStack.push(new AndExpression(op1, op2));
        }
    }

    /**
     * Separates a list of input expressions into expressions
     * that match one element in another given list of expressions
     * (textual matching) and the remaining elements.
     *
     * @param inputs      triage those expressions
     * @param comparisons compare input against those expressions
     * @param matches     will contain input matching comparison list
     */
    void triageByComparison(List<Expression> inputs,
                            List<Expression> comparisons,
                            List<Expression> matches,
                            List<Expression> nonMatches) {
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
            long val1 = ((LongValue) op1).getValue();
            long val2 = ((LongValue) op2).getValue();
            opStack.push(new LongValue(Math.max(val1, val2)));
        } else if (op1 instanceof NullValue && op2 instanceof NullValue) {
            opStack.push(new NullValue());
        } else if (op1 instanceof LongValue) {
            long val1 = ((LongValue) op1).getValue();
            if (val1 == 0) {
                opStack.push(op2);
            } else {
                opStack.push(new LongValue(1));
            }
        } else if (op2 instanceof LongValue) {
            long val2 = ((LongValue) op2).getValue();
            if (val2 == 0) {
                opStack.push(op1);
            } else {
                opStack.push(new LongValue(1));
            }
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
                Expression newAnd = newOr == null ? andCommon :
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
     * @param oldBinaryCmp old (=non-rewritten) comparison
     * @param newBinaryCmp empty shell for producing new comparison
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
            long val1 = ((LongValue) op1).getValue();
            long val2 = ((LongValue) op2).getValue();
            if (newBinaryCmp instanceof EqualsTo) {
                opStack.push(new LongValue(val1 == val2 ? 1 : 0));
            } else if (newBinaryCmp instanceof GreaterThan) {
                opStack.push(new LongValue(val1 > val2 ? 1 : 0));
            } else if (newBinaryCmp instanceof GreaterThanEquals) {
                opStack.push(new LongValue(val1 >= val2 ? 1 : 0));
            } else if (newBinaryCmp instanceof MinorThan) {
                opStack.push(new LongValue(val1 < val2 ? 1 : 0));
            } else if (newBinaryCmp instanceof MinorThanEquals) {
                opStack.push(new LongValue(val1 <= val2 ? 1 : 0));
            } else if (newBinaryCmp instanceof NotEqualsTo) {
                opStack.push(new LongValue(val1 != val2 ? 1 : 0));
            } else {
                newBinaryCmp.setLeftExpression(op1);
                newBinaryCmp.setRightExpression(op2);
                opStack.push(newBinaryCmp);
            }
        } else if (op1 instanceof DoubleValue && op2 instanceof DoubleValue) {
            double val1 = ((DoubleValue) op1).getValue();
            double val2 = ((DoubleValue) op2).getValue();
            if (newBinaryCmp instanceof EqualsTo) {
                opStack.push(new LongValue(val1 == val2 ? 1 : 0));
            } else if (newBinaryCmp instanceof GreaterThan) {
                opStack.push(new LongValue(val1 > val2 ? 1 : 0));
            } else if (newBinaryCmp instanceof GreaterThanEquals) {
                opStack.push(new LongValue(val1 >= val2 ? 1 : 0));
            } else if (newBinaryCmp instanceof MinorThan) {
                opStack.push(new LongValue(val1 < val2 ? 1 : 0));
            } else if (newBinaryCmp instanceof MinorThanEquals) {
                opStack.push(new LongValue(val1 <= val2 ? 1 : 0));
            } else if (newBinaryCmp instanceof NotEqualsTo) {
                opStack.push(new LongValue(val1 != val2 ? 1 : 0));
            } else {
                newBinaryCmp.setLeftExpression(op1);
                newBinaryCmp.setRightExpression(op2);
                opStack.push(newBinaryCmp);
            }
        } else if (oldBinaryCmp instanceof EqualsTo &&
                ((op1 instanceof StringValue && op2 instanceof Column) ||
                        (op1 instanceof Column && op2 instanceof StringValue))) {
            StringValue stringVal =
                    (StringValue) (op1 instanceof StringValue ? op1 : op2);
            // Is string value not in dictionary (if available)?
            Dictionary curDic = BufferManager.dictionary;
            if (curDic != null && curDic.getCode(stringVal.getValue()) < 0) {
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

    private boolean isExtractExpression(Expression expr) {
        if (!(expr instanceof CastExpression)) {
            return false;
        }

        CastExpression cast = (CastExpression) expr;
        return cast.getLeftExpression() instanceof ExtractExpression;
    }

    private boolean isConstantExpression(Expression expr) {
        if (expr instanceof LongValue) {
            return true;
        }

        if (!(expr instanceof CastExpression)) {
            return false;
        }

        Expression casted = ((CastExpression) expr).getLeftExpression();
        return casted instanceof LongValue;
    }

    private boolean isTimestampExpression(Expression expr) {
        return expr instanceof DateTimeLiteralExpression;
    }

    private boolean isDateColumn(Expression expr) {
        if (!(expr instanceof CastExpression)) {
            return false;
        }

        CastExpression cast = (CastExpression) expr;
        return cast.getType().toString().equals("DATE") &&
                cast.getLeftExpression() instanceof Column;
    }

    public static int getNextDate(String curDate) {
        final Date date = Date.valueOf(curDate);
        final Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.DAY_OF_YEAR, 1);
        return (int) (calendar.getTimeInMillis() / (long) 1000);
    }

    @SuppressWarnings("deprecation")
    void treatDateComparison() {
        BinaryExpression cmp;
        if (!(opStack.peek() instanceof BinaryExpression)) {
            return;
        }
        cmp = (BinaryExpression) opStack.pop();

        Expression right = cmp.getRightExpression();
        Expression left = cmp.getLeftExpression();


        boolean swapped = false;
        if ((isExtractExpression(right) && isConstantExpression(left)) ||
                (isDateColumn(right) && isTimestampExpression(left))) {
            swapped = true;
            Expression temp = left;
            left = right;
            right = temp;
        }


        if (isExtractExpression(left) && isConstantExpression(right)) {
            ExtractExpression extract = (ExtractExpression)
                    ((CastExpression) left).getLeftExpression();
            Expression column = extract.getExpression();
            String name = extract.getName();
            LongValue constant = right instanceof CastExpression ?
                    (LongValue) ((CastExpression) right).getLeftExpression() :
                    (LongValue) right;

            switch (name) {
                case "YEAR": {
                    int year = (int) constant.getValue();
                    Date yearDate = new Date(year - 1900, 0, 1);
                    int yearSeconds =
                            (int) (yearDate.getTime() / ((long) 1000));
                    Date nextYearDate = new Date(year + 1 - 1900, 0, 1);
                    int nextYearSeconds =
                            (int) (nextYearDate.getTime() / ((long) 1000));

                    if (cmp instanceof EqualsTo) {
                        GreaterThanEquals l = new GreaterThanEquals();
                        l.setLeftExpression(column);
                        l.setRightExpression(new LongValue(yearSeconds));
                        MinorThan r = new MinorThan();
                        r.setLeftExpression(column);
                        r.setRightExpression(new LongValue(nextYearSeconds));
                        AndExpression conjunction = new AndExpression(l, r);
                        opStack.push(conjunction);
                        return;
                    } else if (cmp instanceof NotEqualsTo) {
                        GreaterThanEquals l = new GreaterThanEquals();
                        l.setLeftExpression(column);
                        l.setRightExpression(new LongValue(nextYearSeconds));
                        MinorThan r = new MinorThan();
                        r.setLeftExpression(column);
                        r.setRightExpression(new LongValue(yearSeconds));
                        OrExpression disjunction = new OrExpression(l, r);
                        opStack.push(new Parenthesis(disjunction));
                        return;
                    } else if (cmp instanceof GreaterThanEquals) {
                        BinaryExpression gte;
                        if (swapped) {
                            gte = new MinorThanEquals();
                        } else {
                            gte = new GreaterThanEquals();
                        }

                        gte.setLeftExpression(column);
                        gte.setRightExpression(new LongValue(yearSeconds));
                        opStack.push(gte);
                        return;
                    } else if (cmp instanceof GreaterThan) {
                        BinaryExpression gt;
                        if (swapped) {
                            gt = new MinorThan();
                        } else {
                            gt = new GreaterThan();
                        }

                        gt.setLeftExpression(column);
                        gt.setRightExpression(new LongValue(yearSeconds));
                        opStack.push(gt);
                        return;
                    } else if (cmp instanceof MinorThanEquals) {
                        BinaryExpression mte;
                        if (swapped) {
                            mte = new GreaterThanEquals();
                        } else {
                            mte = new MinorThanEquals();
                        }

                        mte.setLeftExpression(column);
                        mte.setRightExpression(new LongValue(yearSeconds));
                        opStack.push(mte);
                        return;
                    } else if (cmp instanceof MinorThan) {
                        BinaryExpression mt;
                        if (swapped) {
                            mt = new GreaterThan();
                        } else {
                            mt = new MinorThan();
                        }

                        mt.setLeftExpression(column);
                        mt.setRightExpression(new LongValue(yearSeconds));
                        opStack.push(mt);
                        return;
                    }

                    break;
                }
            }

        } else if (isDateColumn(left) && isTimestampExpression(right)) {
            DateTimeLiteralExpression literal =
                    (DateTimeLiteralExpression) right;
            switch (literal.getType()) {
                case DATE: {
                    String dateLiteral = literal.getValue().replaceAll("'", "");
                    Date date = Date.valueOf(dateLiteral);
                    int dateSeconds = (int) (date.getTime() / (long) 1000);
                    int nextDateSeconds = getNextDate(dateLiteral);
                    Expression column =
                            ((CastExpression) left).getLeftExpression();
                    if (cmp instanceof EqualsTo) {
                        GreaterThanEquals l = new GreaterThanEquals();
                        l.setLeftExpression(column);
                        l.setRightExpression(new LongValue(dateSeconds));
                        MinorThan r = new MinorThan();
                        r.setLeftExpression(column);
                        r.setRightExpression(new LongValue(nextDateSeconds));
                        AndExpression conjunction = new AndExpression(l, r);
                        opStack.push(conjunction);
                        return;
                    } else if (cmp instanceof NotEqualsTo) {
                        GreaterThanEquals l = new GreaterThanEquals();
                        l.setLeftExpression(column);
                        l.setRightExpression(new LongValue(nextDateSeconds));
                        MinorThan r = new MinorThan();
                        r.setLeftExpression(column);
                        r.setRightExpression(new LongValue(dateSeconds));
                        OrExpression disjunction = new OrExpression(l, r);
                        opStack.push(new Parenthesis(disjunction));
                        return;
                    } else if (cmp instanceof GreaterThanEquals) {
                        BinaryExpression gte;
                        if (swapped) {
                            gte = new MinorThanEquals();
                        } else {
                            gte = new GreaterThanEquals();
                        }
                        gte.setLeftExpression(column);
                        gte.setRightExpression(new LongValue(dateSeconds));
                        opStack.push(gte);
                        return;
                    } else if (cmp instanceof GreaterThan) {
                        BinaryExpression gt;
                        if (swapped) {
                            gt = new MinorThan();
                        } else {
                            gt = new GreaterThan();
                        }

                        gt.setLeftExpression(column);
                        gt.setRightExpression(new LongValue(dateSeconds));
                        opStack.push(gt);
                        return;
                    } else if (cmp instanceof MinorThanEquals) {
                        BinaryExpression mte;
                        if (swapped) {
                            mte = new GreaterThanEquals();
                        } else {
                            mte = new MinorThanEquals();
                        }

                        mte.setLeftExpression(column);
                        mte.setRightExpression(new LongValue(dateSeconds));
                        opStack.push(mte);
                        return;
                    } else if (cmp instanceof MinorThan) {
                        BinaryExpression mt;
                        if (swapped) {
                            mt = new GreaterThan();
                        } else {
                            mt = new MinorThan();
                        }

                        mt.setLeftExpression(column);
                        mt.setRightExpression(new LongValue(dateSeconds));
                        opStack.push(mt);
                        return;
                    }

                    break;
                }
                case TIME:
                case TIMESTAMP:
                    throw new RuntimeException("not implemented");
            }


        }

        opStack.push(cmp);
    }

    @Override
    public void visit(EqualsTo arg0) {
        EqualsTo newEquals = new EqualsTo();
        treatBinaryComparison(arg0, newEquals);
        treatDateComparison();
    }

    @Override
    public void visit(GreaterThan arg0) {
        GreaterThan newGt = new GreaterThan();
        treatBinaryComparison(arg0, newGt);
        treatDateComparison();
    }

    @Override
    public void visit(GreaterThanEquals arg0) {
        GreaterThanEquals newGte = new GreaterThanEquals();
        treatBinaryComparison(arg0, newGte);
        treatDateComparison();
    }

    /**
     * We transform an in expression into nested OR expressions.
     */
    @Override
    public void visit(InExpression arg0) {
        ItemsList rightItems = arg0.getRightItemsList();
        if (rightItems instanceof ExpressionList) {
            List<Expression> exps =
                    ((ExpressionList) rightItems).getExpressions();
            if (exps.isEmpty()) {
                // Empty list -> Always false
                opStack.push(new LongValue(0));
            } else {
                Long previous = null;
                boolean range = true;
                List<Long> rangeValues = new ArrayList<>();
                for (int i = 0; i < exps.size(); i++) {
                    Expression curr = exps.get(i);
                    if (!(curr instanceof LongValue)) {
                        range = false;
                        break;
                    }

                    rangeValues.add(((LongValue) curr).getValue());
                }
                Collections.sort(rangeValues);
                for (int i = 1; i < rangeValues.size(); i++) {
                    if (rangeValues.get(i) != rangeValues.get(i - 1) + 1) {
                        range = false;
                        break;
                    }
                }

                if (exps.size() == 1) {
                    range = false;
                }

                if (range) {
                    GreaterThanEquals lowerBound = new GreaterThanEquals();
                    lowerBound.setLeftExpression(arg0.getLeftExpression());
                    lowerBound.setRightExpression(new LongValue(
                            rangeValues.get(0)));
                    MinorThanEquals upperBound = new MinorThanEquals();
                    upperBound.setLeftExpression(arg0.getLeftExpression());
                    upperBound.setRightExpression(new LongValue(
                            rangeValues.get(rangeValues.size() - 1)));
                    AndExpression conjunction = new AndExpression(lowerBound,
                            upperBound);
                    Parenthesis parenthesis = new Parenthesis(conjunction);
                    parenthesis.accept(this);
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
        newLike.setCaseInsensitive(arg0.isCaseInsensitive());
        newLike.setEscape(arg0.getEscape());
        opStack.push(newLike);
    }

    @Override
    public void visit(MinorThan arg0) {
        MinorThan newMt = new MinorThan();
        treatBinaryComparison(arg0, newMt);
        treatDateComparison();
    }

    @Override
    public void visit(MinorThanEquals arg0) {
        MinorThanEquals newMte = new MinorThanEquals();
        treatBinaryComparison(arg0, newMte);
        treatDateComparison();
    }

    @Override
    public void visit(NotEqualsTo arg0) {
        NotEqualsTo newNe = new NotEqualsTo();
        treatBinaryComparison(arg0, newNe);
        treatDateComparison();
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
            String s1 = ((StringValue) op1).getValue();
            String s2 = ((StringValue) op2).getValue();
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
            long val = ((LongValue) newExp).getValue();
            opStack.push(new LongValue(1 - val));
        } else {
            opStack.push(new NotExpression(newExp));
        }
    }

}
