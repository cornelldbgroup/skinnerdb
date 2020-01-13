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
import query.where.WhereUtil;
import types.SQLtype;

import java.util.*;

public class NonEquiNodesTest extends PlainVisitor {
    /**
     * Query to which index filter is applied.
     */
    final QueryInfo query;
    /**
     * Contains indexes of all rows satisfying
     * the predicate.
     */
    public final Deque<NonEquiNode> nonEquiNodes =
            new ArrayDeque<>();
    /**
     * Contains last extracted integer constants.
     */
    final Deque<Number> extractedConstants =
            new ArrayDeque<>();
    /**
     * Contains alias indexes applicable
     * for sub-expressions.
     */
    final Deque<Index> applicableIndices =
            new ArrayDeque<>();
    /**
     * Contains table indexes applicable
     * for sub-expressions.
     */
    final Deque<Integer> applicableIdx =
            new ArrayDeque<>();
    /**
     * Maps source columns, as in query, to DB columns.
     */
    final Map<ColumnRef, ColumnRef> columnMappings;
    /**
     * Initialize index filter for given query.
     *
     * @param query	            meta-data on evaluated query
     * @param columnMappings	maps source columns, as in query, to DB columns
     */
    public NonEquiNodesTest(QueryInfo query, Map<ColumnRef, ColumnRef> columnMappings) {
        this.query = query;
        this.columnMappings = columnMappings;
    }

    @Override
    public void visit(AndExpression and) {
        if (and.isNot()) {
            EqualsExtraction extraction = new EqualsExtraction(query);
            and.accept(extraction);
            Expression nonEquals = WhereUtil.conjunction(extraction.nonEquiJoins);
            NonEquiNode equalNode = null;
            NonEquiNode nonEqualNode = null;
            if (extraction.equiJoins.size() > 0) {
                List<Expression> expressions = new ArrayList<>(extraction.equiJoins);
                Expression equals = WhereUtil.conjunction(expressions);

                if (extraction.equiType == SQLtype.INT) {
                    equalNode = new IntIndexNode(equals, extraction.equiJoins, columnMappings, query);
                }
                else if (extraction.equiType == SQLtype.DOUBLE) {
                    equalNode = new DoubleIndexNode(equals, extraction.equiJoins, columnMappings, query);
                }
                else {
                    throw new RuntimeException("Index is not supported!");
                }
            }
            if (nonEquals != null) {
                nonEquals.accept(this);
                nonEqualNode = nonEquiNodes.pop();
            }
            NonEquiNode node = new NonEquiNode(equalNode, nonEqualNode, and,
                    null, null, Operator.Exist, -1);
            nonEquiNodes.push(node);
        }
        else {
            // construct nodes
            and.getLeftExpression().accept(this);
            and.getRightExpression().accept(this);
            // Intersect bool results for left and right expression.
            NonEquiNode leftNode = nonEquiNodes.pop();
            NonEquiNode rightNode = nonEquiNodes.pop();
            NonEquiNode node = new NonEquiNode(leftNode, rightNode, and, null, null, Operator.AND, -1);
            nonEquiNodes.push(node);
        }
    }

    @Override
    public void visit(OrExpression or) {
        or.getLeftExpression().accept(this);
        or.getRightExpression().accept(this);
        // Intersect bool results for left and right expression.
        NonEquiNode leftNode = nonEquiNodes.pop();
        NonEquiNode rightNode = nonEquiNodes.pop();
        NonEquiNode node = new NonEquiNode(leftNode, rightNode, or, null, null, Operator.OR, -1);
        nonEquiNodes.push(node);
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        equalsTo.getLeftExpression().accept(this);
        equalsTo.getRightExpression().accept(this);
        // We assume predicate passed the index test so
        // there must be one index and one constant.
        Operator operator = equalsTo.isNot() ? Operator.EqualsExist : Operator.EqualsTo;
        if (extractedConstants.isEmpty()) {
            Index rightIndex = applicableIndices.pop();
            Index leftIndex = applicableIndices.pop();
            int rightTable = applicableIdx.pop();
            int leftTable = applicableIdx.pop();
            NonEquiNode node = new NonEquiNode(null, null, equalsTo,
                    leftIndex, rightIndex, operator, leftTable, rightTable);
            nonEquiNodes.push(node);
        }
        else {
            Number constant = extractedConstants.pop();
            Index index = applicableIndices.pop();
            int table = applicableIdx.pop();
            NonEquiNode node = new NonEquiNode(null, null, equalsTo,
                    index, constant, operator, table);
            nonEquiNodes.push(node);
        }
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        notEqualsTo.getLeftExpression().accept(this);
        notEqualsTo.getRightExpression().accept(this);
        // We assume predicate passed the index test so
        // there must be one index and one constant.
        Operator operator = notEqualsTo.isNot() ? Operator.NotEqualsAll : Operator.NotEqualsTo;
        if (extractedConstants.isEmpty()) {
            Index rightIndex = applicableIndices.pop();
            Index leftIndex = applicableIndices.pop();
            int rightTable = applicableIdx.pop();
            int leftTable = applicableIdx.pop();
            NonEquiNode node = new NonEquiNode(null, null, notEqualsTo,
                    leftIndex, rightIndex, operator, leftTable, rightTable);
            nonEquiNodes.push(node);
        }
        else {
            Number constant = extractedConstants.pop();
            Index index = applicableIndices.pop();
            int table = applicableIdx.pop();
            NonEquiNode node = new NonEquiNode(null, null, notEqualsTo,
                    index, constant, operator, table);
            nonEquiNodes.push(node);
        }
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        greaterThan.getLeftExpression().accept(this);
        greaterThan.getRightExpression().accept(this);
        // We assume predicate passed the index test so
        // there must be one index and one constant.
        if (extractedConstants.isEmpty()) {
            Index rightIndex = applicableIndices.pop();
            Index leftIndex = applicableIndices.pop();
            int rightTable = applicableIdx.pop();
            int leftTable = applicableIdx.pop();
            NonEquiNode node = new NonEquiNode(null, null, greaterThan,
                    leftIndex, rightIndex, Operator.GreaterThan, leftTable, rightTable);
            nonEquiNodes.push(node);
        }
        else {
            Number constant = extractedConstants.pop();
            Index index = applicableIndices.pop();
            int table = applicableIdx.pop();
            NonEquiNode node = new NonEquiNode(null, null, greaterThan,
                    index, constant, Operator.GreaterThan, table);
            nonEquiNodes.push(node);
        }
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        greaterThanEquals.getLeftExpression().accept(this);
        greaterThanEquals.getRightExpression().accept(this);
        // We assume predicate passed the index test so
        // there must be one index and one constant.
        if (extractedConstants.isEmpty()) {
            Index rightIndex = applicableIndices.pop();
            Index leftIndex = applicableIndices.pop();
            int rightTable = applicableIdx.pop();
            int leftTable = applicableIdx.pop();
            NonEquiNode node = new NonEquiNode(null, null, greaterThanEquals,
                    leftIndex, rightIndex, Operator.GreaterThanEquals, leftTable, rightTable);
            nonEquiNodes.push(node);
        }
        else {
            Number constant = extractedConstants.pop();
            Index index = applicableIndices.pop();
            int table = applicableIdx.pop();
            NonEquiNode node = new NonEquiNode(null, null, greaterThanEquals,
                    index, constant, Operator.GreaterThanEquals, table);
            nonEquiNodes.push(node);
        }
    }

    @Override
    public void visit(MinorThan minorThan) {
        minorThan.getLeftExpression().accept(this);
        minorThan.getRightExpression().accept(this);
        // We assume predicate passed the index test so
        // there must be one index and one constant.
        if (extractedConstants.isEmpty()) {
            Index rightIndex = applicableIndices.pop();
            Index leftIndex = applicableIndices.pop();
            int rightTable = applicableIdx.pop();
            int leftTable = applicableIdx.pop();
            NonEquiNode node = new NonEquiNode(null, null, minorThan,
                    leftIndex, rightIndex, Operator.MinorThan, leftTable, rightTable);
            nonEquiNodes.push(node);
        }
        else {
            Number constant = extractedConstants.pop();
            Index index = applicableIndices.pop();
            int table = applicableIdx.pop();
            NonEquiNode node = new NonEquiNode(null, null, minorThan,
                    index, constant, Operator.MinorThan, table);
            nonEquiNodes.push(node);
        }
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        minorThanEquals.getLeftExpression().accept(this);
        minorThanEquals.getRightExpression().accept(this);
        // We assume predicate passed the index test so
        // there must be one index and one constant.
        if (extractedConstants.isEmpty()) {
            Index rightIndex = applicableIndices.pop();
            Index leftIndex = applicableIndices.pop();
            int rightTable = applicableIdx.pop();
            int leftTable = applicableIdx.pop();
            NonEquiNode node = new NonEquiNode(null, null, minorThanEquals,
                    leftIndex, rightIndex, Operator.MinorThanEquals, leftTable, rightTable);
            nonEquiNodes.push(node);
        }
        else {
            Number constant = extractedConstants.pop();
            Index index = applicableIndices.pop();
            int table = applicableIdx.pop();
            NonEquiNode node = new NonEquiNode(null, null, minorThanEquals,
                    index, constant, Operator.MinorThanEquals, table);
            nonEquiNodes.push(node);
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
//        notExpression.getExpression().accept(this);
//        NonEquiNode leftNode = nonEquiNodes.pop();
//        NonEquiNode node = new NonEquiNode(leftNode, null,
//                notExpression, null, null, Operator.NotExist, -1);
//        nonEquiNodes.push(node);
        EqualsExtraction extraction = new EqualsExtraction(query);
        not.accept(extraction);
        Expression nonEquals = WhereUtil.conjunction(extraction.nonEquiJoins);
        NonEquiNode equalNode = null;
        NonEquiNode nonEqualNode = null;
        if (extraction.equiJoins.size() > 0) {
            List<Expression> expressions = new ArrayList<>(extraction.equiJoins);
            Expression equals = WhereUtil.conjunction(expressions);

            if (extraction.equiType == SQLtype.INT) {
                equalNode = new IntIndexNode(equals, extraction.equiJoins, columnMappings, query);
            }
            else if (extraction.equiType == SQLtype.DOUBLE) {
                equalNode = new DoubleIndexNode(equals, extraction.equiJoins, columnMappings, query);
            }
            else {
                throw new RuntimeException("Index is not supported!");
            }
        }
        if (nonEquals != null) {
            nonEquals.accept(this);
            nonEqualNode = nonEquiNodes.pop();
        }
        NonEquiNode node = new NonEquiNode(equalNode, nonEqualNode, not,
                null, null, Operator.NotExist, -1);
        nonEquiNodes.push(node);
    }

    @Override
    public void visit(LongValue longValue) {
        extractedConstants.push((int)longValue.getValue());
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
        String columnName = column.getColumnName();
        ColumnRef colRef = new ColumnRef(aliasName, columnName);
        ColumnRef dbRef = columnMappings.get(colRef);

        // Check for available index
        Index index = BufferManager.colToIndex.get(dbRef);
        if (index != null) {
            applicableIndices.push(index);
            applicableIdx.push(query.aliasToIndex.get(aliasName));
        }
    }
}
