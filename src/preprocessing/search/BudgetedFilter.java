package preprocessing.search;

import catalog.CatalogManager;
import expressions.compilation.UnaryBoolEval;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;

public class BudgetedFilter {
    private final int cardinality;
    private int lastCompletedRow;
    private List<Integer> result;
    private List<Pair<UnaryBoolEval, Double>> compiled;

    public BudgetedFilter(String tableName,
                          List<Pair<UnaryBoolEval, Double>> compiled) {
        this.result = new ArrayList<>();
        this.compiled = compiled;
        this.cardinality = CatalogManager.getCardinality(tableName);
        this.lastCompletedRow = -1;
    }

    public double executeWithBudget(int remainingRows, int[] order) {
        int currentCompletedRow = lastCompletedRow;

        long startTime = System.nanoTime();
        ROW_LOOP:
        while (remainingRows > 0 && currentCompletedRow + 1 < cardinality) {
            currentCompletedRow++;
            remainingRows--;

            for (int predIndex : order) {
                Pair<UnaryBoolEval, Double> expr = compiled.get(predIndex);

                if (expr.getLeft().evaluate(currentCompletedRow) <= 0) {
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
        return lastCompletedRow == cardinality - 1;
    }

    public List<Integer> getResult() {
        return result;
    }
}
