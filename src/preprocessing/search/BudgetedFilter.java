package preprocessing.search;

import catalog.CatalogManager;
import expressions.compilation.UnaryBoolEval;

import java.util.ArrayList;
import java.util.List;

public class BudgetedFilter {
    private final int cardinality;
    private int lastCompletedRow;
    private List<Integer> result;
    private List<UnaryBoolEval> compiled;

    public BudgetedFilter(String tableName, List<UnaryBoolEval> compiled) {
        this.result = new ArrayList<>();
        this.compiled = compiled;
        this.cardinality = CatalogManager.getCardinality(tableName);
        this.lastCompletedRow = -1;
    }

    public int executeWithBudget(int budget, int[] order) {
        int remainingBudget = budget;
        int currentCompletedRow = lastCompletedRow;

        ROW_LOOP:
        while (remainingBudget > 0 && currentCompletedRow + 1 < cardinality) {
            currentCompletedRow++;
            for (int predIndex : order) {
                --remainingBudget;
                if (compiled.get(predIndex).evaluate(currentCompletedRow) <= 0) {
                    continue ROW_LOOP;
                }

                if (remainingBudget == 0) {
                    // Since we're out of budget undo marking current row as
                    // complete and exit
                    currentCompletedRow--;
                    break ROW_LOOP;
                }
            }
            result.add(currentCompletedRow);
        }

        int reward = currentCompletedRow - lastCompletedRow;
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
