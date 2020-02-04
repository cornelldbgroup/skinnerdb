package preprocessing.search;

import expressions.compilation.UnaryBoolEval;
import operators.RowRange;

import java.util.ArrayList;
import java.util.List;

public class BudgetedFilter {
    private int lastCompletedRow;
    private List<Integer> result;
    private List<UnaryBoolEval> compiled;
    private RowRange range;

    public BudgetedFilter(List<UnaryBoolEval> compiled,
                          RowRange range) {
        this.result = new ArrayList<>();
        this.compiled = compiled;
        this.lastCompletedRow = range.firstTuple - 1;
        this.range = range;
    }

    public double executeWithBudget(int remainingRows, int[] order) {
        int currentCompletedRow = lastCompletedRow;

        long startTime = System.nanoTime();
        ROW_LOOP:
        while (remainingRows > 0 && currentCompletedRow + 1 <= range.lastTuple) {
            currentCompletedRow++;
            remainingRows--;

            for (int predIndex : order) {
                UnaryBoolEval expr = compiled.get(predIndex);
                if (expr.evaluate(currentCompletedRow) <= 0) {
                    continue ROW_LOOP;
                }
            }

            result.add(currentCompletedRow);
        }
        long endTime = System.nanoTime();
        long duration = endTime - startTime;
        double reward = Math.exp(-duration * 0.0001);
        lastCompletedRow = currentCompletedRow;
        return reward;
    }

    public boolean isFinished() {
        return lastCompletedRow == range.lastTuple;
    }

    public List<Integer> getResult() {
        return result;
    }
}
