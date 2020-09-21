package joining.parallel.parallelization.task;

import buffer.BufferManager;
import expressions.normalization.PlainVisitor;
import indexing.Index;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import predicate.*;
import query.ColumnRef;
import query.QueryInfo;
import query.where.WhereUtil;
import types.SQLtype;

import java.util.*;

public class SubQueryTest extends PlainVisitor {
    /**
     * Query to which index filter is applied.
     */
    final QueryInfo query;
    /**
     * Contains indexes of all rows satisfying
     * the predicate.
     */
    public final Deque<String> predicates =
            new ArrayDeque<>();
    /**
     * Contains last extracted integer constants.
     */
    final Deque<String> extractedConstants =
            new ArrayDeque<>();
    /**
     * Contains alias and columns applicable
     * for sub-expressions.
     */
    final Deque<String> applicableColumns =
            new ArrayDeque<>();
    /**
     * Maps source columns, as in query, to DB columns.
     */
    final Map<String, String> aliasMappings;
    /**
     * Sub query type:
     * 0-default
     * 1-exist,
     * 2-not exist
     */
    int type = 0;

    public SubQueryTest(QueryInfo query, Map<String, String> aliasMappings) {
        this.query = query;
        this.aliasMappings = aliasMappings;
    }

    @Override
    public void visit(AndExpression and) {
        if (and.isNot()) {
            and.getLeftExpression().accept(this);
            and.getRightExpression().accept(this);
            type = 1;
        }
        else {
            // construct nodes
            and.getLeftExpression().accept(this);
            and.getRightExpression().accept(this);
        }
    }

    @Override
    public void visit(OrExpression or) {

    }

    @Override
    public void visit(EqualsTo equalsTo) {
        equalsTo.getLeftExpression().accept(this);
        equalsTo.getRightExpression().accept(this);
        if (extractedConstants.isEmpty()) {
            String rightColumn = applicableColumns.pop();
            String leftColumn = applicableColumns.pop();
            predicates.add(rightColumn + " = " + leftColumn);
        }
        else {
            String constant = extractedConstants.pop();
            String leftColumn = applicableColumns.pop();
            predicates.add(leftColumn + " = " + constant);
        }
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        notEqualsTo.getLeftExpression().accept(this);
        notEqualsTo.getRightExpression().accept(this);
        // We assume predicate passed the index test so
        // there must be one index and one constant.
        if (extractedConstants.isEmpty()) {
            String rightColumn = applicableColumns.pop();
            String leftColumn = applicableColumns.pop();
            predicates.add(rightColumn + " <> " + leftColumn);
        }
        else {
            String constant = extractedConstants.pop();
            String leftColumn = applicableColumns.pop();
            predicates.add(leftColumn + " <> " + constant);
        }
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        greaterThan.getLeftExpression().accept(this);
        greaterThan.getRightExpression().accept(this);
        // We assume predicate passed the index test so
        // there must be one index and one constant.
        if (extractedConstants.isEmpty()) {
            String rightColumn = applicableColumns.pop();
            String leftColumn = applicableColumns.pop();
            predicates.add(leftColumn + " > " + rightColumn);
        }
        else {
            String constant = extractedConstants.pop();
            String leftColumn = applicableColumns.pop();
            predicates.add(leftColumn + " > " + constant);
        }
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        greaterThanEquals.getLeftExpression().accept(this);
        greaterThanEquals.getRightExpression().accept(this);
        // We assume predicate passed the index test so
        // there must be one index and one constant.
        if (extractedConstants.isEmpty()) {
            String rightColumn = applicableColumns.pop();
            String leftColumn = applicableColumns.pop();
            predicates.add(leftColumn + " >= " + rightColumn);
        }
        else {
            String constant = extractedConstants.pop();
            String leftColumn = applicableColumns.pop();
            predicates.add(leftColumn + " >= " + constant);
        }
    }

    @Override
    public void visit(MinorThan minorThan) {
        minorThan.getLeftExpression().accept(this);
        minorThan.getRightExpression().accept(this);
        // We assume predicate passed the index test so
        // there must be one index and one constant.
        if (extractedConstants.isEmpty()) {
            String rightColumn = applicableColumns.pop();
            String leftColumn = applicableColumns.pop();
            predicates.add(leftColumn + " < " + rightColumn);
        }
        else {
            String constant = extractedConstants.pop();
            String leftColumn = applicableColumns.pop();
            predicates.add(leftColumn + " < " + constant);
        }
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        minorThanEquals.getLeftExpression().accept(this);
        minorThanEquals.getRightExpression().accept(this);
        // We assume predicate passed the index test so
        // there must be one index and one constant.
        if (extractedConstants.isEmpty()) {
            String rightColumn = applicableColumns.pop();
            String leftColumn = applicableColumns.pop();
            predicates.add(leftColumn + " <= " + rightColumn);
        }
        else {
            String constant = extractedConstants.pop();
            String leftColumn = applicableColumns.pop();
            predicates.add(leftColumn + " <= " + constant);
        }
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
    public void visit(NotExpression not) {
        not.getExpression().accept(this);
        type = 2;
    }

    @Override
    public void visit(LongValue longValue) {
        extractedConstants.push(String.valueOf(longValue.getValue()));
    }

    @Override
    public void visit(StringValue stringValue) {
        // String must be in dictionary due to index test
        String strVal = stringValue.getValue();
        extractedConstants.push(strVal);
    }

    @Override
    public void visit(Column column) {
        // Resolve column reference
        String aliasName = column.getTable().getName();
        String columnName = column.getColumnName();
        String mappedAlias = aliasMappings.get(aliasName);
        if (mappedAlias.charAt(0) == 'T' && mappedAlias.charAt(1) == '_') {
            applicableColumns.push(mappedAlias + "." + aliasName + "_" + columnName);
        }
        else {
            applicableColumns.push(aliasName + "." + columnName);
        }
    }
}
