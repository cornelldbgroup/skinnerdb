package operators.parallel;

import data.ColumnData;
import data.IntData;
import expressions.compilation.UnaryIntEval;
import java.util.concurrent.Callable;

public class MapTask implements Callable<Object>{

    private final int firstTuple;
    private final int lastTuple;
    private final boolean groupBy;
    private final IntData groupData;
    private UnaryIntEval unaryIntEval;
    private IntData intResult;


    public MapTask(int firstTuple, int lastTuple,
                   boolean groupBy, IntData groupData,
                   UnaryIntEval unaryIntEval, IntData intResult) {
        this.firstTuple = firstTuple;
        this.lastTuple = lastTuple;
        this.groupBy = groupBy;
        this.groupData = groupData;
        this.unaryIntEval = unaryIntEval;
        this.intResult = intResult;
    }


    @Override
    public Object call() throws Exception {
        int[] rowResult = new int[1];
        for (int resultRow = firstTuple; resultRow <= lastTuple; ++resultRow) {
            // Either map row to row or row to group
            int targetRow = !groupBy ? resultRow : groupData.data[resultRow];
            boolean notNull = unaryIntEval.evaluate(targetRow, rowResult);
            if (!groupBy || notNull) {
//                intResult.isNull.set(targetRow, !notNull);
                intResult.data[targetRow] = rowResult[0];
            }
        }
        return null;
    }
}
