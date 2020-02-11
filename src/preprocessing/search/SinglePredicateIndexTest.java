package preprocessing.search;

import buffer.BufferManager;
import data.Dictionary;
import indexing.HashIndex;
import indexing.HashIntIndex;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;
import query.ColumnRef;
import query.QueryInfo;

/**
 * Verifies whether a unary predicate can be
 * evaluated using indices alone. This class
 * must be kept in sync with IndexFilter.
 *
 * @author immanueltrummer
 */
public class SinglePredicateIndexTest implements ExpressionVisitor {
    /**
     * Meta-data about the query containing test predicate.
     */
    final QueryInfo query;
    /**
     * Whether we can use an index to evaluate input predicate.
     */
    public boolean canUseIndex = true;

    public HashIndex index = null;

    public Number constant = null;

    /**
     * Initialize index test for given query.
     *
     * @param query meta-data about query
     */
    public SinglePredicateIndexTest(QueryInfo query) {
        this.query = query;
    }

    @Override
    public void visit(NullValue nullValue) {
        canUseIndex = false;
    }

    @Override
    public void visit(Function function) {
        canUseIndex = false;
    }

    @Override
    public void visit(SignedExpression signedExpression) {
        canUseIndex = false;
    }

    @Override
    public void visit(JdbcParameter jdbcParameter) {
        canUseIndex = false;
    }

    @Override
    public void visit(JdbcNamedParameter jdbcNamedParameter) {
        canUseIndex = false;
    }

    @Override
    public void visit(DoubleValue doubleValue) {
        constant = doubleValue.getValue();
    }

    @Override
    public void visit(LongValue longValue) {
        constant = (int) longValue.getValue();
    }

    @Override
    public void visit(HexValue hexValue) {
        canUseIndex = false;
    }

    @Override
    public void visit(DateValue dateValue) {
        canUseIndex = false;
    }

    @Override
    public void visit(TimeValue timeValue) {
        canUseIndex = false;
    }

    @Override
    public void visit(TimestampValue timestampValue) {
        canUseIndex = false;
    }

    @Override
    public void visit(Parenthesis parenthesis) {
        parenthesis.getExpression().accept(this);
    }

    @Override
    public void visit(StringValue stringValue) {
        // Can use index if value in dictionary
        String val = stringValue.getValue();
        Dictionary curDic = BufferManager.dictionary;
        if (curDic != null && curDic.getCode(val) < 0) {
            canUseIndex = false;
        }
    }

    @Override
    public void visit(Addition addition) {
        canUseIndex = false;
    }

    @Override
    public void visit(Division division) {
        canUseIndex = false;
    }

    @Override
    public void visit(Multiplication multiplication) {
        canUseIndex = false;
    }

    @Override
    public void visit(Subtraction subtraction) {
        canUseIndex = false;
    }

    @Override
    public void visit(AndExpression andExpression) {
        canUseIndex = false;
    }

    @Override
    public void visit(OrExpression orExpression) {
        canUseIndex = false;
    }

    @Override
    public void visit(Between between) {
        canUseIndex = false;
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        Expression left = equalsTo.getLeftExpression();
        Expression right = equalsTo.getRightExpression();
        left.accept(this);
        right.accept(this);
        boolean haveConstant = left instanceof LongValue ||
                left instanceof StringValue ||
                right instanceof LongValue ||
                right instanceof StringValue;
        boolean haveColumn = left instanceof Column ||
                right instanceof Column;
        if (!haveConstant || !haveColumn) {
            canUseIndex = false;
        }
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        canUseIndex = false;
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        canUseIndex = false;
    }

    @Override
    public void visit(InExpression inExpression) {
        // Should have been replaced by ORs before
        canUseIndex = false;
    }

    @Override
    public void visit(IsNullExpression isNullExpression) {
        canUseIndex = false;
    }

    @Override
    public void visit(LikeExpression likeExpression) {
        canUseIndex = false;
    }

    @Override
    public void visit(MinorThan minorThan) {
        canUseIndex = false;
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        canUseIndex = false;
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        canUseIndex = false;
    }

    @Override
    public void visit(Column tableColumn) {
        // Resolve column reference
        String aliasName = tableColumn.getTable().getName();
        String tableName = query.aliasToTable.get(aliasName);
        String columnName = tableColumn.getColumnName();
        ColumnRef colRef = new ColumnRef(tableName, columnName);
        // Check that index of right type is available
        this.index = BufferManager.colToIndex.get(colRef);
        if (index != null) {
            if (!(index instanceof HashIntIndex)) {
                // Wrong index type
                canUseIndex = false;
            }
        } else {
            // No index available
            canUseIndex = false;
        }
    }

    @Override
    public void visit(SubSelect subSelect) {
        canUseIndex = false;
    }

    @Override
    public void visit(CaseExpression caseExpression) {
        canUseIndex = false;
    }

    @Override
    public void visit(WhenClause whenClause) {
        canUseIndex = false;
    }

    @Override
    public void visit(ExistsExpression existsExpression) {
        canUseIndex = false;
    }

    @Override
    public void visit(AllComparisonExpression allComparisonExpression) {
        canUseIndex = false;
    }

    @Override
    public void visit(AnyComparisonExpression anyComparisonExpression) {
        canUseIndex = false;
    }

    @Override
    public void visit(Concat concat) {
        canUseIndex = false;
    }

    @Override
    public void visit(Matches matches) {
        canUseIndex = false;
    }

    @Override
    public void visit(BitwiseAnd bitwiseAnd) {
        canUseIndex = false;
    }

    @Override
    public void visit(BitwiseOr bitwiseOr) {
        canUseIndex = false;
    }

    @Override
    public void visit(BitwiseXor bitwiseXor) {
        canUseIndex = false;
    }

    @Override
    public void visit(CastExpression cast) {
        canUseIndex = false;
    }

    @Override
    public void visit(Modulo modulo) {
        canUseIndex = false;
    }

    @Override
    public void visit(AnalyticExpression aexpr) {
        canUseIndex = false;
    }

    @Override
    public void visit(WithinGroupExpression wgexpr) {
        canUseIndex = false;
    }

    @Override
    public void visit(ExtractExpression eexpr) {
        canUseIndex = false;
    }

    @Override
    public void visit(IntervalExpression iexpr) {
        canUseIndex = false;
    }

    @Override
    public void visit(OracleHierarchicalExpression oexpr) {
        canUseIndex = false;
    }

    @Override
    public void visit(RegExpMatchOperator rexpr) {
        canUseIndex = false;
    }

    @Override
    public void visit(JsonExpression jsonExpr) {
        canUseIndex = false;
    }

    @Override
    public void visit(JsonOperator jsonExpr) {
        canUseIndex = false;
    }

    @Override
    public void visit(RegExpMySQLOperator regExpMySQLOperator) {
        canUseIndex = false;
    }

    @Override
    public void visit(UserVariable var) {
        canUseIndex = false;
    }

    @Override
    public void visit(NumericBind bind) {
        canUseIndex = false;
    }

    @Override
    public void visit(KeepExpression aexpr) {
        canUseIndex = false;
    }

    @Override
    public void visit(MySQLGroupConcat groupConcat) {
        canUseIndex = false;
    }

    @Override
    public void visit(RowConstructor rowConstructor) {
        canUseIndex = false;
    }

    @Override
    public void visit(OracleHint hint) {
        canUseIndex = false;
    }

    @Override
    public void visit(TimeKeyExpression timeKeyExpression) {
        canUseIndex = false;
    }

    @Override
    public void visit(DateTimeLiteralExpression literal) {
        canUseIndex = false;
    }

    @Override
    public void visit(NotExpression aThis) {
        canUseIndex = false;
    }

}
