package preprocessing.search;

import catalog.CatalogManager;
import expressions.compilation.UnaryBoolEval;
import indexing.HashIndex;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.eclipse.collections.impl.factory.primitive.IntLists;

import java.util.List;

public class BudgetedFilter {
    private final int cardinality;
    private int lastCompletedRow;
    private MutableIntList result;
    private List<UnaryBoolEval> compiled;

    public BudgetedFilter(String tableName,
                          List<UnaryBoolEval> compiled) {
        this.result = IntLists.mutable.empty();
        this.compiled = compiled;
        this.cardinality = CatalogManager.getCardinality(tableName);
        this.lastCompletedRow = -1;
    }


    private Pair<Long, Integer> indexScanWithOnePredicate(int remainingRows,
                                                          FilterState state) {

        int currentCompletedRow = lastCompletedRow;

        long startTime = System.nanoTime();
        int constant = 0; //TODO: replace this
        HashIndex index = null; //TODO: replace this

        int dataLocation = index.getDataLocation(constant);
        if (dataLocation < 0) {
            return Pair.of(0l, this.cardinality - 1);
        }

        int startPos = index.nextHighestRowInBucket(dataLocation,
                lastCompletedRow);
        if (startPos < -1) {
            return Pair.of(0l, this.cardinality - 1);
        }

        int endPos = index.data[dataLocation] + dataLocation + 1;

        ROW_LOOP:
        while (remainingRows > 0 && startPos < endPos) {
            currentCompletedRow = index.data[startPos];
            remainingRows -= currentCompletedRow - lastCompletedRow;
            startPos++;

            for (int i = 1; i < state.order.length; i++) {
                UnaryBoolEval expr = compiled.get(state.order[i]);
                if (expr.evaluate(currentCompletedRow) <= 0) {
                    continue ROW_LOOP;
                }
            }

            result.add(currentCompletedRow);
        }

        long endTime = System.nanoTime();
        long duration = endTime - startTime;
        return Pair.of(duration, currentCompletedRow);
    }

    private Pair<Long, Integer> tableScan(int remainingRows,
                                          FilterState state) {
        int currentCompletedRow = lastCompletedRow;
        long startTime = System.nanoTime();
        ROW_LOOP:
        while (remainingRows > 0 && currentCompletedRow + 1 < cardinality) {
            currentCompletedRow++;
            remainingRows--;

            for (int predIndex : state.order) {
                UnaryBoolEval expr = compiled.get(predIndex);
                if (expr.evaluate(currentCompletedRow) <= 0) {
                    continue ROW_LOOP;
                }
            }

            result.add(currentCompletedRow);
        }
        long endTime = System.nanoTime();
        long duration = endTime - startTime;
        return Pair.of(duration, currentCompletedRow);
    }

    public double executeWithBudget(int remainingRows, FilterState state) {
        Pair<Long, Integer> result;

        if (state.useIndexScan()) { // Use index to filter rows.
            result = indexScanWithOnePredicate(remainingRows, state);
        } else if (state.avoidBranching) { // TODO Use Bitmaps to avoid
            // branching
            return 0;
        } else {
            result = tableScan(remainingRows, state);
        }


        double reward = Math.exp(-result.getLeft() * 0.0001);
        lastCompletedRow = result.getRight();
        return reward;
    }

    public boolean isFinished() {
        return lastCompletedRow == cardinality - 1;
    }

    public MutableIntList getResult() {
        return result;
    }
}
