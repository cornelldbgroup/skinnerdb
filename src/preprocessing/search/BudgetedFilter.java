package preprocessing.search;

import catalog.CatalogManager;
import expressions.compilation.UnaryBoolEval;
import indexing.HashIndex;
import net.sf.jsqlparser.expression.Expression;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.eclipse.collections.impl.factory.primitive.IntLists;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class BudgetedFilter {
    private final int cardinality;
    private int lastCompletedRow;
    private MutableIntList result;
    private ImmutableList<Expression> predicates;
    private ImmutableList<UnaryBoolEval> compiled;
    private List<HashIndex> indices;
    private List<Number> values;

    public BudgetedFilter(String tableName,
                          ImmutableList<Expression> predicates,
                          ImmutableList<UnaryBoolEval> compiled,
                          List<HashIndex> indices,
                          List<Number> values) {
        this.result = IntLists.mutable.empty();
        this.compiled = compiled;
        this.cardinality = CatalogManager.getCardinality(tableName);
        this.lastCompletedRow = -1;
        this.indices = indices;
        this.predicates = predicates;
        this.values = values;
    }


    private Pair<Long, Integer> indexScanWithOnePredicate(int remainingRows,
                                                          FilterState state) {
        int currentCompletedRow = lastCompletedRow;

        long startTime = System.nanoTime();
        HashIndex index = indices.get(state.order[0]);
        int dataLocation =
                index.getDataLocation(values.get(state.order[0]));
        if (dataLocation < 0) {
            return Pair.of(0l, this.cardinality - 1);
        }

        int startPos = index.nextHighestRowInBucket(dataLocation,
                lastCompletedRow);
        if (startPos < 0) {
            return Pair.of(0l, this.cardinality - 1);
        }

        int endPos = index.data[dataLocation] + dataLocation + 1;

        if (state.cachedEval != null) {
            ROW_LOOP:
            while (remainingRows > 0 && startPos < endPos) {
                currentCompletedRow = index.data[startPos];
                remainingRows -= currentCompletedRow - lastCompletedRow;
                startPos++;

                if (state.cachedEval.evaluate(currentCompletedRow) <= 0) {
                    continue ROW_LOOP;
                }

                for (int i = state.cachedTil + 1; i < state.order.length; i++) {
                    UnaryBoolEval expr = compiled.get(state.order[i]);
                    if (expr.evaluate(currentCompletedRow) <= 0) {
                        continue ROW_LOOP;
                    }
                }

                result.add(currentCompletedRow);
            }
        } else {
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
        }

        long endTime = System.nanoTime();
        long duration = endTime - startTime;
        return Pair.of(duration, currentCompletedRow);
    }

    private Pair<Long, Integer> tableScanBranching(int remainingRows,
                                                   FilterState state) {
        int currentCompletedRow = lastCompletedRow;
        long startTime = System.nanoTime();

        if (state.cachedEval != null) {
            ROW_LOOP:
            while (remainingRows > 0 && currentCompletedRow + 1 < cardinality) {
                currentCompletedRow++;
                remainingRows--;

                if (state.cachedEval.evaluate(currentCompletedRow) <= 0) {
                    continue ROW_LOOP;
                }

                for (int i = state.cachedTil + 1; i < state.order.length; i++) {
                    UnaryBoolEval expr = compiled.get(state.order[i]);
                    if (expr.evaluate(currentCompletedRow) <= 0) {
                        continue ROW_LOOP;
                    }
                }

                result.add(currentCompletedRow);
            }
        } else {
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
        }

        long endTime = System.nanoTime();
        long duration = endTime - startTime;
        return Pair.of(duration, currentCompletedRow);
    }

    private Pair<Long, Integer> tableScanBitset(int nrRows,
                                                FilterState state) {
        long startTime = System.nanoTime();

        int begin = lastCompletedRow + 1;
        int end = Math.min(lastCompletedRow + nrRows, cardinality - 1);
        nrRows = end - begin + 1;

        List<BitSet> predicateEval = new ArrayList<>(compiled.size());
        for (int i = 0; i < compiled.size(); i++) {
            predicateEval.add(new BitSet(nrRows));
        }


        for (int row = begin, i = 0; row <= end; row++, i++) {
            for (int j = 0; j < compiled.size(); j++) {
                predicateEval.get(j).set(i, compiled.get(j).evaluate(row) > 0);
            }
        }

        BitSet filter = new BitSet(nrRows);
        filter.set(0, nrRows);
        for (BitSet pred : predicateEval) {
            filter.and(pred);
        }

        for (int row = begin, i = 0; row <= end; row++, i++) {
            if (filter.get(i)) {
                result.add(row);
            }
        }

        long endTime = System.nanoTime();
        long duration = endTime - startTime;
        return Pair.of(duration, end);
    }

    public double executeWithBudget(int remainingRows, FilterState state) {
        Pair<Long, Integer> result;

        if (state.useIndexScan) { // Use index to filter rows.
            result = indexScanWithOnePredicate(remainingRows, state);
        } else if (state.avoidBranching) {
            result = tableScanBitset(remainingRows, state);
        } else {
            result = tableScanBranching(remainingRows, state);
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
