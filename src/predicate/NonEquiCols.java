package predicate;

import buffer.BufferManager;
import expressions.normalization.PlainVisitor;
import indexing.Index;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import query.ColumnRef;
import query.QueryInfo;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Set;

public class NonEquiCols extends PlainVisitor {
    /**
     * Query to which index filter is applied.
     */
    final QueryInfo query;
    /**
     * Contains extracted columns.
     */
    public final Set<ColumnRef> extractedCols =
            new HashSet<>();
    /**
     * Initialize index filter for given query.
     *
     * @param query	meta-data on evaluated query
     */
    public NonEquiCols(QueryInfo query) {
        this.query = query;
    }

    @Override
    public void visit(AndExpression and) {
        // construct nodes
        and.getLeftExpression().accept(this);
        and.getRightExpression().accept(this);
    }

    @Override
    public void visit(OrExpression or) {
        or.getLeftExpression().accept(this);
        or.getRightExpression().accept(this);
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        equalsTo.getLeftExpression().accept(this);
        equalsTo.getRightExpression().accept(this);
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        greaterThan.getLeftExpression().accept(this);
        greaterThan.getRightExpression().accept(this);
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        greaterThanEquals.getLeftExpression().accept(this);
        greaterThanEquals.getRightExpression().accept(this);
    }

    @Override
    public void visit(MinorThan minorThan) {
        minorThan.getLeftExpression().accept(this);
        minorThan.getRightExpression().accept(this);
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        minorThanEquals.getLeftExpression().accept(this);
        minorThanEquals.getRightExpression().accept(this);
    }

    @Override
    public void visit(CastExpression cast) {
        cast.getLeftExpression().accept(this);
    }

    @Override
    public void visit(Parenthesis parenthesis) {
        parenthesis.getExpression().accept(this);
    }

    @Override
    public void visit(LongValue longValue) {

    }

    @Override
    public void visit(NotExpression notExpression) {
        notExpression.getExpression().accept(this);
    }

    @Override
    public void visit(StringValue stringValue) {

    }

    @Override
    public void visit(Column column) {
        // Resolve column reference
        String aliasName = column.getTable().getName();
        String columnName = column.getColumnName();
        ColumnRef colRef = new ColumnRef(aliasName, columnName);
        extractedCols.add(colRef);
    }
}
