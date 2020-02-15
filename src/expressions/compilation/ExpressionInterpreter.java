package expressions.compilation;

import buffer.BufferManager;
import catalog.CatalogManager;
import catalog.info.ColumnInfo;
import data.*;
import dk.brics.automaton.RegExp;
import dk.brics.automaton.RunAutomaton;
import expressions.ExpressionInfo;
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
import types.JavaType;
import types.SQLtype;
import types.TypeUtil;

import java.util.*;

public class ExpressionInterpreter extends SkinnerVisitor implements UnaryBoolEval {
    private ExpressionInfo expressionInfo;
    private Map<String, ColumnRef> aggMapping;

    private final Map<String, Integer> calendarPartToID;
    private final HashMap<ColumnRef, Integer> columnToID;
    private final Map<ColumnRef, ColumnRef> columnMapping;
    private final List<ColumnData> dataList;
    private final Map<Expression, RunAutomaton> likeToRunAutomaton;
    private final EvaluatorType evaluatorType;

    private class Result {
        private int RESULT_INT;
        private double RESULT_DOUBLE;
        private String RESULT_STRING;
        private long RESULT_LONG;
        private boolean RESULT_NULL;

        public Result() {
            RESULT_INT = 0;
            RESULT_DOUBLE = 0;
            RESULT_STRING = "";
            RESULT_LONG = 0;
            RESULT_NULL = false;
        }

        public void returnInt(int value) {
            RESULT_INT = value;
        }

        public void returnLong(long value) {
            RESULT_LONG = value;
        }

        public void returnDouble(double value) {
            RESULT_DOUBLE = value;
        }

        public void returnNull() {
            RESULT_NULL = true;
        }

        public void returnString(String value) {
            RESULT_STRING = value;
        }

        public int getInt() {
            return RESULT_INT;
        }

        public long getLong() {
            return RESULT_LONG;
        }

        public double getDouble() {
            return RESULT_DOUBLE;
        }

        public boolean isNull() {
            return RESULT_NULL;
        }

        public String getString() {
            return RESULT_STRING;
        }
    }

    Result interpretResult;
    int tupleIdx;
    Expression expression;

    public ExpressionInterpreter(ExpressionInfo expressionInfo,
                                 Map<ColumnRef, ColumnRef> columnMapping,
                                 Map<String, ColumnRef> aggMapping,
                                 EvaluatorType evaluatorType,
                                 Expression expression) {
        this.expressionInfo = expressionInfo;
        this.columnMapping = columnMapping;
        this.aggMapping = aggMapping;
        this.evaluatorType = evaluatorType;

        this.columnToID = new HashMap<>();
        this.likeToRunAutomaton = new HashMap<>();
        this.dataList = new ArrayList<>();
        int columnID = 0;
        for (ColumnRef columnRef : expressionInfo.columnsMentioned) {
            ColumnRef dbRef = columnMapping.get(columnRef);
            ColumnData columnData = BufferManager.colToData.get(dbRef);
            dataList.add(columnData);
            columnToID.put(columnRef, columnID);
            ++columnID;
        }
        if (aggMapping != null) {
            for (ColumnRef columnRef : aggMapping.values()) {
                ColumnRef dbRef = columnMapping.get(columnRef);
                ColumnData columnData = BufferManager.colToData.get(dbRef);
                dataList.add(columnData);
                columnToID.put(columnRef, columnID);
                ++columnID;
            }
        }
        // Assign regular expressions to IDs
        for (Expression regExp : expressionInfo.likeExpressions) {
            String regex = ((StringValue) regExp).getValue();
            // Replace special symbols
            for (char c : new char[]{'.', '(', ')', '[', ']', '{', '}'}) {
                regex = regex.replace(c + "", "\\" + c);
            }
            regex = regex.replace('?', '.');
            regex = regex.replace("%", ".*");

            RunAutomaton auto =
                    new RunAutomaton(new RegExp(regex).toAutomaton(), true);
            likeToRunAutomaton.put(regExp, auto);
        }


        this.calendarPartToID = new HashMap<>();
        calendarPartToID.put("second", Calendar.SECOND);
        calendarPartToID.put("minute", Calendar.MINUTE);
        calendarPartToID.put("hour", Calendar.HOUR);
        calendarPartToID.put("day", Calendar.DAY_OF_MONTH);
        calendarPartToID.put("month", Calendar.MONTH);
        calendarPartToID.put("year", Calendar.YEAR);

        interpretResult = new Result();
    }

    private JavaType getType(Expression expression) {
        SQLtype type = expressionInfo.expressionToType.get(expression);
        return TypeUtil.toJavaType(type);
    }

    @Override
    public void visit(NullValue nullValue) {
        interpretResult.returnNull();
    }

    @Override
    public void visit(Function function) {
        String fct = function.getName().toLowerCase();
        switch (fct) {
            case "min":
            case "max":
            case "avg":
            case "sum":
            case "count":
                // We have an aggregation function -
                // we expect that such expressions have been
                // evaluated before compilation is invoked.
                String SQL = function.toString();
                ColumnRef aggRef = aggMapping.get(SQL);
                String alias = aggRef.aliasName;
                Table table = new Table(alias);
                String column = aggRef.columnName;
                Column aggCol = new Column(table, column);
                visit(aggCol);
                break;
            default:
                // Assume a user-defined predicate by default
        }
    }

    @Override
    public void visit(DoubleValue doubleValue) {
        interpretResult.returnDouble(doubleValue.getValue());
    }

    @Override
    public void visit(LongValue longValue) {
        SQLtype type = expressionInfo.expressionToType.get(longValue);
        long value = longValue.getValue();
        switch (type) {
            case INT:
                interpretResult.returnInt((int) value);
                break;
            case LONG:
                interpretResult.returnLong(value);
                break;
            default:
                System.out.println("Warning: unsupported type for long");
        }
    }


    @Override
    public void visit(DateValue val) {
        interpretResult.returnInt((int) (val.getValue().getTime() / 1000));
    }

    @Override
    public void visit(TimeValue val) {
        interpretResult.returnInt((int) (val.getValue().getTime() / 1000));
    }

    @Override
    public void visit(TimestampValue val) {
        interpretResult.returnInt((int) (val.getValue().getTime() / 1000));
    }

    @Override
    public void visit(Parenthesis parenthesis) {
        parenthesis.getExpression().accept(this);
    }

    @Override
    public void visit(StringValue stringValue) {
        int code = -1;
        if (CatalogManager.currentDB.compressed) {
            code = BufferManager.dictionary.getCode(stringValue.getValue());
        }

        if (code >= 0) {
            interpretResult.returnInt(code);
        } else {
            interpretResult.returnString(stringValue.getValue());
        }
    }

    @Override
    public void visit(IntervalExpression interval) {
        String param = interval.getParameter();
        String strVal = param.substring(1, param.length() - 1);
        int intVal = Integer.valueOf(strVal);

        String intervalType = interval.getIntervalType().toLowerCase();
        switch (intervalType) {
            case "year":
                interpretResult.returnInt(intVal * 12);
                break;
            case "month":
                interpretResult.returnInt(intVal);
                break;
            case "day":
                interpretResult.returnInt(24 * 60 * 60 * intVal);
                break;
            case "hour":
                interpretResult.returnInt(60 * 60 * intVal);
                break;
            case "minute":
                interpretResult.returnInt(60 * intVal);
                break;
            case "second":
                interpretResult.returnInt(intVal);
                break;
            default:
                throwException("Error - unknown interval type");
        }
    }

    @Override
    public void visit(DateTimeLiteralExpression date) {
        switch (date.getType()) {
            case DATE:
                DateValue dateVal = new DateValue(date.getValue());
                visit(dateVal);
                break;
            case TIME:
                TimeValue timeVal = new TimeValue(date.getValue());
                visit(timeVal);
                break;
            case TIMESTAMP:
                TimestampValue tsVal = new TimestampValue(date.getValue());
                visit(tsVal);
                break;
        }
    }

    @Override
    public void visit(Column column) {
        String alias = column.getTable().getName();
        String columnName = column.getColumnName();
        ColumnRef queryRef = new ColumnRef(alias, columnName);
        int columnID = columnToID.get(queryRef);
        ColumnData data = dataList.get(columnID);
        ColumnRef dbRef = columnMapping.get(queryRef);
        ColumnInfo colInfo = CatalogManager.getColumn(dbRef);

        if (data.isNull.get(tupleIdx)) {
            interpretResult.returnNull();
            return;
        }

        JavaType javaType = TypeUtil.toJavaType(colInfo.type);
        switch (javaType) {
            case INT:
                interpretResult.returnInt(((IntData) data).data[tupleIdx]);
                break;
            case LONG:
                interpretResult.returnLong(((LongData) data).data[tupleIdx]);
                break;
            case DOUBLE:
                interpretResult.returnDouble(((DoubleData) data).data[tupleIdx]);
                break;
            case STRING:
                interpretResult.returnString(((StringData) data).data[tupleIdx]);
                break;
        }
    }

    /**
     * Returns true iff the left operand is either a date or
     * a timestamp and the right operand is a year-month
     * time interval.
     *
     * @param left  left operand of binary expression
     * @param right right operand of binary expression
     * @return true iff the operands require special treatment
     */
    private boolean dateYMintervalOp(Expression left, Expression right) {
        SQLtype leftType = expressionInfo.expressionToType.get(left);
        SQLtype rightType = expressionInfo.expressionToType.get(right);
        return (leftType.equals(SQLtype.DATE) ||
                leftType.equals(SQLtype.TIMESTAMP)) &&
                (rightType.equals(SQLtype.YM_INTERVAL));
    }

    /**
     * Adds given number of months to a date, represented
     * according to Unix time format.
     *
     * @param dateSecs seconds since January 1st 1970
     * @param months   number of months to add (can be negative)
     * @return seconds since January 1st 1970 after addition
     */
    private int addMonths(int dateSecs, int months) {
        Calendar calendar = Calendar.getInstance();
        try {
            calendar.setTimeInMillis((long) dateSecs * (long) 1000);
            calendar.add(Calendar.MONTH, months);
        } catch (Exception e) {
            System.out.println("dateSecs: " + dateSecs);
            System.out.println("months: " + months);
            System.out.println(calendar);
            e.printStackTrace();
        }
        return (int) (calendar.getTimeInMillis() / (long) 1000);
    }

    /**
     * Adds code to treat the addition or subtraction of a
     * number of months from a date or timestamp value.
     * Returns true iff the given expression is indeed
     * of that type.
     *
     * @param arg0 expression potentially involving months arithmetic
     * @return true if code for expression was added
     */
    private boolean treatAsMonthArithmetic(BinaryExpression arg0) {
        Expression left = arg0.getLeftExpression();
        Expression right = arg0.getRightExpression();
        if (dateYMintervalOp(left, right) || dateYMintervalOp(right, left)) {
            if (dateYMintervalOp(right, left)) {
                Expression temp = left;
                left = right;
                right = temp;
            }

            left.accept(this);
            int dateSecs = interpretResult.getInt();

            right.accept(this);
            int months = interpretResult.getInt();

            // Multiply number of months by -1 for subtraction
            if (arg0 instanceof Subtraction) {
                months *= -1;
            }

            interpretResult.returnInt(addMonths(dateSecs, months));
            return true;
        }

        return false;
    }

    private enum Binop {
        ADD, DIV, MUL, SUB, MOD,
        AND, OR, XOR,
        EQ, NEQ, GT, GEQ, LT, LEQ
    }

    private void evaluateBinop(BinaryExpression expression, Binop op) {
        if (treatAsMonthArithmetic(expression)) {
            return;
        }

        JavaType jType = getType(expression.getLeftExpression());
        switch (jType) {
            case INT: {
                expression.getLeftExpression().accept(this);
                if (interpretResult.isNull()) {
                    interpretResult.returnNull();
                    return;
                }
                int l = interpretResult.getInt();

                expression.getRightExpression().accept(this);
                if (interpretResult.isNull()) {
                    interpretResult.returnNull();
                    return;
                }
                int r = interpretResult.getInt();

                switch (op) {
                    case ADD:
                        interpretResult.returnInt(l + r);
                        break;
                    case DIV:
                        interpretResult.returnInt(l / r);
                        break;
                    case MUL:
                        interpretResult.returnInt(l % r);
                        break;
                    case SUB:
                        interpretResult.returnInt(l - r);
                        break;
                    case MOD:
                        interpretResult.returnInt(l % r);
                        break;
                    case AND:
                        interpretResult.returnInt(l & r);
                        break;
                    case OR:
                        interpretResult.returnInt(l | r);
                        break;
                    case XOR:
                        interpretResult.returnInt(l ^ r);
                        break;
                    case EQ:
                        interpretResult.returnInt(l == r ? 1 : 0);
                        break;
                    case NEQ:
                        interpretResult.returnInt(l != r ? 1 : 0);
                        break;
                    case LT:
                        interpretResult.returnInt(l < r ? 1 : 0);
                        break;
                    case LEQ:
                        interpretResult.returnInt(l <= r ? 1 : 0);
                        break;
                    case GT:
                        interpretResult.returnInt(l > r ? 1 : 0);
                        break;
                    case GEQ:
                        interpretResult.returnInt(l >= r ? 1 : 0);
                        break;
                }
                return;
            }

            case LONG: {
                expression.getLeftExpression().accept(this);
                if (interpretResult.isNull()) {
                    interpretResult.returnNull();
                    return;
                }
                long l = interpretResult.getLong();

                expression.getRightExpression().accept(this);
                if (interpretResult.isNull()) {
                    interpretResult.returnNull();
                    return;
                }
                long r = interpretResult.getLong();

                switch (op) {
                    case ADD:
                        interpretResult.returnLong(l + r);
                        break;
                    case DIV:
                        interpretResult.returnLong(l / r);
                        break;
                    case MUL:
                        interpretResult.returnLong(l % r);
                        break;
                    case SUB:
                        interpretResult.returnLong(l - r);
                        break;
                    case MOD:
                        interpretResult.returnLong(l % r);
                        break;
                    case AND:
                        interpretResult.returnLong(l & r);
                        break;
                    case OR:
                        interpretResult.returnLong(l | r);
                        break;
                    case XOR:
                        interpretResult.returnLong(l ^ r);
                        break;
                    case EQ:
                        interpretResult.returnInt(l == r ? 1 : 0);
                        break;
                    case NEQ:
                        interpretResult.returnInt(l != r ? 1 : 0);
                        break;
                    case LT:
                        interpretResult.returnInt(l < r ? 1 : 0);
                        break;
                    case LEQ:
                        interpretResult.returnInt(l <= r ? 1 : 0);
                        break;
                    case GT:
                        interpretResult.returnInt(l > r ? 1 : 0);
                        break;
                    case GEQ:
                        interpretResult.returnInt(l >= r ? 1 : 0);
                        break;
                }
                return;
            }

            case DOUBLE: {
                expression.getLeftExpression().accept(this);
                if (interpretResult.isNull()) {
                    interpretResult.returnNull();
                    return;
                }
                double l = interpretResult.getDouble();

                expression.getRightExpression().accept(this);
                if (interpretResult.isNull()) {
                    interpretResult.returnNull();
                    return;
                }
                double r = interpretResult.getDouble();

                switch (op) {
                    case ADD:
                        interpretResult.returnDouble(l + r);
                        break;
                    case DIV:
                        interpretResult.returnDouble(l / r);
                        break;
                    case MUL:
                        interpretResult.returnDouble(l % r);
                        break;
                    case SUB:
                        interpretResult.returnDouble(l - r);
                        break;
                    case MOD:
                        interpretResult.returnDouble(l % r);
                        break;
                    case EQ:
                        interpretResult.returnInt(l == r ? 1 : 0);
                        break;
                    case NEQ:
                        interpretResult.returnInt(l != r ? 1 : 0);
                        break;
                    case LT:
                        interpretResult.returnInt(l < r ? 1 : 0);
                        break;
                    case LEQ:
                        interpretResult.returnInt(l <= r ? 1 : 0);
                        break;
                    case GT:
                        interpretResult.returnInt(l > r ? 1 : 0);
                        break;
                    case GEQ:
                        interpretResult.returnInt(l >= r ? 1 : 0);
                        break;
                }
                return;
            }

            case STRING: {
                expression.getLeftExpression().accept(this);
                if (interpretResult.isNull()) {
                    interpretResult.returnNull();
                    return;
                }
                String l = interpretResult.getString();

                expression.getRightExpression().accept(this);
                if (interpretResult.isNull()) {
                    interpretResult.returnNull();
                    return;
                }
                String r = interpretResult.getString();

                int comp = l.compareTo(r);
                switch (op) {
                    case EQ:
                        interpretResult.returnInt(comp == 0 ? 1 : 0);
                        break;
                    case NEQ:
                        interpretResult.returnInt(comp != 0 ? 1 : 0);
                        break;
                    case LT:
                        interpretResult.returnInt(comp < 0 ? 1 : 0);
                        break;
                    case LEQ:
                        interpretResult.returnInt(comp <= 0 ? 1 : 0);
                        break;
                    case GT:
                        interpretResult.returnInt(comp > 0 ? 1 : 0);
                        break;
                    case GEQ:
                        interpretResult.returnInt(comp >= 1 ? 1 : 0);
                        break;
                    case ADD:
                        interpretResult.returnString(l.concat(r));
                        break;
                }
                return;
            }
        }
        System.err.println("Warning: unsupported types in " +
                expression.toString());
    }

    @Override
    public void visit(Addition addition) {
        evaluateBinop(addition, Binop.ADD);
    }

    @Override
    public void visit(Subtraction subtraction) {
        evaluateBinop(subtraction, Binop.SUB);
    }

    @Override
    public void visit(Division division) {
        evaluateBinop(division, Binop.DIV);
    }

    @Override
    public void visit(Multiplication multiplication) {
        evaluateBinop(multiplication, Binop.MUL);
    }

    @Override
    public void visit(Modulo modulo) {
        evaluateBinop(modulo, Binop.MOD);
    }

    @Override
    public void visit(BitwiseAnd bitwiseAnd) {
        evaluateBinop(bitwiseAnd, Binop.AND);
    }

    @Override
    public void visit(BitwiseOr bitwiseOr) {
        evaluateBinop(bitwiseOr, Binop.OR);
    }

    @Override
    public void visit(BitwiseXor bitwiseXor) {
        evaluateBinop(bitwiseXor, Binop.XOR);
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        evaluateBinop(equalsTo, Binop.EQ);
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        evaluateBinop(notEqualsTo, Binop.NEQ);
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        evaluateBinop(greaterThan, Binop.GT);
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        evaluateBinop(greaterThanEquals, Binop.GEQ);
    }

    @Override
    public void visit(MinorThan minorThan) {
        evaluateBinop(minorThan, Binop.LT);
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        evaluateBinop(minorThanEquals, Binop.LEQ);
    }

    @Override
    public void visit(Concat concat) {
        evaluateBinop(concat, Binop.ADD);
    }

    @Override
    public void visit(AndExpression andExpression) {
        andExpression.getLeftExpression().accept(this);
        if (interpretResult.isNull()) {
            interpretResult.returnNull();
            return;
        }

        if (interpretResult.getInt() == 0) {
            interpretResult.returnInt(0);
            return;
        }

        andExpression.getLeftExpression().accept(this);
        if (interpretResult.isNull()) {
            interpretResult.returnNull();
            return;
        }

        if (interpretResult.getInt() == 0) {
            interpretResult.returnInt(0);
            return;
        }

        interpretResult.returnInt(1);
    }

    @Override
    public void visit(OrExpression orExpression) {
        orExpression.getLeftExpression().accept(this);
        if (interpretResult.isNull()) {
            interpretResult.returnNull();
            return;
        }

        if (interpretResult.getInt() == 1) {
            interpretResult.returnInt(1);
            return;
        }

        orExpression.getLeftExpression().accept(this);
        if (interpretResult.isNull()) {
            interpretResult.returnNull();
            return;
        }

        if (interpretResult.getInt() == 1) {
            interpretResult.returnInt(1);
            return;
        }

        interpretResult.returnInt(0);
    }

    @Override
    public void visit(IsNullExpression isNullExpression) {
        isNullExpression.getLeftExpression().accept(this);
        interpretResult.returnInt(interpretResult.isNull() ? 1 : 0);
    }

    @Override
    public void visit(NotExpression notExpression) {
        notExpression.getExpression().accept(this);
        if (interpretResult.isNull()) {
            interpretResult.returnNull();
            return;
        }

        interpretResult.returnInt(interpretResult.getInt() == 0 ? 1 : 0);
    }

    @Override
    public void visit(SignedExpression signedExpression) {
        signedExpression.getExpression().accept(this);
        if (interpretResult.isNull()) {
            interpretResult.returnNull();
        }
        if (signedExpression.getSign() == '-') {
            JavaType jType = getType(signedExpression);
            switch (jType) {
                case INT: {
                    int res = interpretResult.getInt();
                    interpretResult.returnInt(-res);
                    break;
                }
                case LONG: {
                    long res = interpretResult.getLong();
                    interpretResult.returnLong(-res);
                    break;
                }
                case DOUBLE: {
                    double res = interpretResult.getDouble();
                    interpretResult.returnDouble(-res);
                    break;
                }
                default:
                    System.out.println("Signed expression unsupported");
                    break;
            }
        }
    }

    @Override
    public void visit(LikeExpression likeExpression) {
        likeExpression.getLeftExpression().accept(this);
        if (interpretResult.isNull()) {
            interpretResult.returnNull();
            return;
        }

        RunAutomaton automaton =
                likeToRunAutomaton.get(likeExpression.getRightExpression());
        if (automaton.run(interpretResult.getString())) {
            if (likeExpression.isNot()) interpretResult.returnInt(0);
            else interpretResult.returnInt(1);
        } else {
            if (likeExpression.isNot()) interpretResult.returnInt(1);
            else interpretResult.returnInt(0);
        }
    }

    @Override
    public void visit(CaseExpression cases) {
        for (Expression expr : cases.getWhenClauses()) {
            WhenClause whenClause = (WhenClause) expr;
            whenClause.getWhenExpression().accept(this);
            if (interpretResult.isNull()) {
                interpretResult.returnInt(0);
            }

            int cond = interpretResult.getInt();
            if (cond == 1) {
                whenClause.getThenExpression().accept(this);
                return;
            }
        }

        Expression elseExpr = cases.getElseExpression();
        if (elseExpr != null) {
            elseExpr.accept(this);
        } else {
            interpretResult.returnNull();
        }
    }

    @Override
    public void visit(CastExpression castExpression) {
        SQLtype sourceType = expressionInfo.expressionToType.get(
                castExpression.getLeftExpression());
        SQLtype targetType =
                expressionInfo.expressionToType.get(castExpression);
        castExpression.getLeftExpression().accept(this);
        if (interpretResult.isNull()) {
            interpretResult.returnNull();
        }

        switch (sourceType) {
            case INT: {
                int src = interpretResult.getInt();
                switch (targetType) {
                    case LONG:
                        interpretResult.returnLong((long) src);
                        break;
                    case DOUBLE:
                        interpretResult.returnDouble((double) src);
                        break;
                }
                return;
            }
            case LONG: {
                long src = interpretResult.getLong();
                switch (targetType) {
                    case INT:
                        interpretResult.returnInt((int) src);
                        break;
                    case DOUBLE:
                        interpretResult.returnDouble((double) src);
                        break;
                }
                return;
            }
            case DOUBLE: {
                double src = interpretResult.getDouble();
                switch (targetType) {
                    case INT:
                        interpretResult.returnInt((int) src);
                        break;
                    case LONG:
                        interpretResult.returnLong((long) src);
                        break;
                }
                return;
            }
            case STRING_CODE: {
                int src = interpretResult.getInt();
                switch (targetType) {
                    case STRING:
                        interpretResult.returnString(
                                BufferManager.dictionary.getString(src));
                        break;
                }
                return;
            }
        }
    }


    @Override
    public void visit(ExtractExpression extract) {
        String part = extract.getName().toLowerCase();
        if (!calendarPartToID.containsKey(part)) {
            throwException("Error - "
                    + "unsupported extraction part: "
                    + part);
        }
        int partID = calendarPartToID.get(part);

        extract.getExpression().accept(this);
        if (interpretResult.isNull()) {
            interpretResult.returnNull();
            return;
        }

        int dateSecs = interpretResult.getInt();

        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis((long) dateSecs * (long) 1000);
        interpretResult.returnInt(calendar.get(partID));
    }

    // Eval Interface Implementations


    @Override
    public byte evaluate(int tupleIdx) {
        this.tupleIdx = tupleIdx;
        this.expression.accept(this);
        if (interpretResult.isNull()) {
            return 0;
        }
        if (interpretResult.getInt() == 1) {
            return 1;
        } else {
            return -1;
        }
    }

    // Unsupported/unneeded visitor methods.
    @Override
    public void visit(JdbcParameter jdbcParameter) {}

    @Override
    public void visit(JdbcNamedParameter jdbcNamedParameter) {}

    @Override
    public void visit(AnalyticExpression analyticExpression) {}

    @Override
    public void visit(WithinGroupExpression withinGroupExpression) {}

    @Override
    public void visit(OracleHint oracleHint) {}

    @Override
    public void visit(TimeKeyExpression timeKeyExpression) {}

    @Override
    public void visit(OracleHierarchicalExpression oracleHierarchicalExpression) {}

    @Override
    public void visit(RegExpMatchOperator regExpMatchOperator) {}

    @Override
    public void visit(JsonExpression jsonExpression) {}

    @Override
    public void visit(JsonOperator jsonOperator) {}

    @Override
    public void visit(RegExpMySQLOperator regExpMySQLOperator) {}

    @Override
    public void visit(UserVariable userVariable) {}

    @Override
    public void visit(NumericBind numericBind) {}

    @Override
    public void visit(KeepExpression keepExpression) {}

    @Override
    public void visit(MySQLGroupConcat mySQLGroupConcat) {}

    @Override
    public void visit(RowConstructor rowConstructor) {}

    @Override
    public void visit(HexValue hexValue) {}

    @Override
    public void visit(Between between) {}

    @Override
    public void visit(InExpression inExpression) {}

    @Override
    public void visit(SubSelect subSelect) {}

    @Override
    public void visit(ExistsExpression existsExpression) {}

    @Override
    public void visit(AllComparisonExpression allComparisonExpression) {}

    @Override
    public void visit(AnyComparisonExpression anyComparisonExpression) {}

    @Override
    public void visit(Matches matches) {}

    @Override
    public void visit(WhenClause whenClause) {}
}
