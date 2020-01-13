package predicate;

import buffer.BufferManager;
import expressions.normalization.PlainVisitor;
import indexing.Index;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import query.ColumnRef;
import query.QueryInfo;
import types.SQLtype;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

public class EqualsExtraction extends PlainVisitor {
    /**
     * Contains equal join predicates.
     */
    final List<EqualsTo> equiJoins =
            new ArrayList<>();
    /**
     * Contains remaining join predicates.
     */
    final List<Expression> nonEquiJoins =
            new ArrayList<>();
    /**
     * Query to process
     */
    final QueryInfo queryInfo;
    /**
     * The type of equiJoin predicates.
     */
    SQLtype equiType;

    public EqualsExtraction(QueryInfo queryInfo) {
        this.queryInfo = queryInfo;
    }

    @Override
    public void visit(AndExpression and) {
        and.getLeftExpression().accept(this);
        and.getRightExpression().accept(this);
    }
    @Override
    public void visit(OrExpression or) {
        or.getLeftExpression().accept(this);
        or.getRightExpression().accept(this);
    }

    @Override
    public void visit(NotExpression not) {
        not.getExpression().accept(this);
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        Expression left = equalsTo.getLeftExpression();
        Expression right = equalsTo.getRightExpression();
        if (left instanceof Column && right instanceof Column) {
            Column leftColumn = (Column) left;
            Table leftTable = leftColumn.getTable();
            String leftName = leftTable==null||leftTable.getName()==null?
                    "":leftTable.getName();
            ColumnRef columnRef = new ColumnRef(leftName, leftColumn.getColumnName());
            equiType = queryInfo.colRefToInfo.get(columnRef).type;
            if (queryInfo.temporaryAlias.contains(leftName)) {
                equalsTo.setRightExpression(left);
                equalsTo.setLeftExpression(right);
            }
            equiJoins.add(equalsTo);
        }
        else {
            nonEquiJoins.add(equalsTo);
        }
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        nonEquiJoins.add(greaterThan);
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        nonEquiJoins.add(greaterThanEquals);
    }

    @Override
    public void visit(MinorThan minorThan) {
        nonEquiJoins.add(minorThan);
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        nonEquiJoins.add(minorThanEquals);
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        nonEquiJoins.add(notEqualsTo);
    }
}
