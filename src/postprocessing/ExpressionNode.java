package postprocessing;

import data.ColumnData;
import net.sf.jsqlparser.expression.Expression;
import predicate.NonEquiNode;
import predicate.Operator;

public class ExpressionNode {
    NonEquiNode left;
    NonEquiNode right;
    Expression expression;
    Operator operator;
    ColumnData columnData;

//    public Number evaluate(int[] tupleIndices) {
//        if (operator == Operator.OR) {
//            return left.evaluate(tupleIndices) || right.evaluate(tupleIndices);
//        }
//        else if (operator == Operator.AND) {
//            return left.evaluate(tupleIndices) && right.evaluate(tupleIndices);
//        }
//        else {
//            if (constant != null) {
//                int curTuple = tupleIndices[table];
//                return nonEquiIndex.evaluate(curTuple, constant, operator);
//            }
//            else {
//                int leftTuple = tupleIndices[leftTable];
//                int rightTuple = tupleIndices[rightTable];
//                Number constant = rightIndex.getNumber(rightTuple);
//                return leftIndex.evaluate(leftTuple, constant, operator);
//            }
//        }
//    }
}
