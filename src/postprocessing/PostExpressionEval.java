package postprocessing;

import buffer.BufferManager;
import data.ColumnData;
import expressions.ExpressionInfo;
import expressions.normalization.PlainVisitor;
import indexing.Index;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.schema.Column;
import org.objectweb.asm.Opcodes;
import query.ColumnRef;
import query.QueryInfo;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;

/**
 *  For each select expression,
 *  compile evaluator for each column involved in the expression.
 *  Then return the final result of evaluation.
 *
 * @author Ziyun Wei
 */
public class PostExpressionEval extends PlainVisitor {
    public ExpressionNode expressionNode;
    /**
     * Query to which index filter is applied.
     */
    final QueryInfo query;
    /**
     * Contains last extracted integer constants.
     */
    final Deque<Number> extractedConstants =
            new ArrayDeque<>();
    /**
     * Maps source columns, as in query, to DB columns.
     */
    final Map<ColumnRef, ColumnRef> columnMappings;
    /**
     * Contains alias column data applicable
     * for sub-expressions.
     */
    final Deque<ColumnData> applicableData =
            new ArrayDeque<>();
    /**
     * Contains table indexes applicable
     * for sub-expressions.
     */
    final Deque<Integer> applicableIdx =
            new ArrayDeque<>();

    public PostExpressionEval(ExpressionInfo expression, Map<ColumnRef, ColumnRef> columnMapping, QueryInfo query) {
        this.columnMappings = columnMapping;
        this.query = query;
    }

    @Override
    public void visit(Addition arg0) {
        treatBinaryArithmetic(arg0, Opcodes.IADD, Opcodes.LADD,
                Opcodes.FADD, Opcodes.DADD, "addition");
    }

    @Override
    public void visit(Division arg0) {
        treatBinaryArithmetic(arg0, Opcodes.IDIV, Opcodes.LDIV,
                Opcodes.FDIV, Opcodes.DDIV, "division");
    }

    @Override
    public void visit(Multiplication arg0) {
        treatBinaryArithmetic(arg0, Opcodes.IMUL, Opcodes.LMUL,
                Opcodes.FMUL, Opcodes.DMUL, "multiplication");
    }

    @Override
    public void visit(Subtraction arg0) {
        treatBinaryArithmetic(arg0, Opcodes.ISUB, Opcodes.LSUB,
                Opcodes.FSUB, Opcodes.DSUB, "subtraction");
    }

    @Override
    public void visit(BitwiseAnd arg0) {
        treatBinaryArithmetic(arg0, Opcodes.IAND,
                Opcodes.LAND, -1, -1, "BitwiseAnd");
    }

    @Override
    public void visit(BitwiseOr arg0) {
        treatBinaryArithmetic(arg0, Opcodes.IOR,
                Opcodes.LOR, -1, -1, "BitwiseOr");
    }

    @Override
    public void visit(BitwiseXor arg0) {
        treatBinaryArithmetic(arg0, Opcodes.IXOR,
                Opcodes.LXOR, -1, -1, "BitwiseXor");
    }

    @Override
    public void visit(Modulo arg0) {
        treatBinaryArithmetic(arg0, Opcodes.IREM, Opcodes.LREM,
                Opcodes.FREM, Opcodes.DREM, "modulo");
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
        extractedConstants.push((int)longValue.getValue());
    }

    @Override
    public void visit(Column column) {
        // Resolve column reference
        String aliasName = column.getTable().getName();
        String columnName = column.getColumnName();
        ColumnRef colRef = new ColumnRef(aliasName, columnName);
        ColumnRef dbRef = columnMappings.get(colRef);

        // Check for available index
        ColumnData columnData = BufferManager.colToData.get(dbRef);
        if (columnData != null) {
            applicableData.push(columnData);
            applicableIdx.push(query.aliasToIndex.get(aliasName));
        }
    }

    void treatBinaryArithmetic(BinaryExpression binaryExpression,
                               int intOp, int longOp, int floatOp, int doubleOp,
                               String readableName) {

    }


}
