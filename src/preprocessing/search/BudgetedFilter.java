package preprocessing.search;

import catalog.CatalogManager;
import expressions.compilation.UnaryBoolEval;
import indexing.HashIndex;
import net.sf.jsqlparser.expression.Expression;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.eclipse.collections.impl.factory.primitive.IntLists;
import parallel.ParallelService;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.Future;

public class BudgetedFilter {
    private final int LAST_TABLE_ROW;
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
        this.LAST_TABLE_ROW = CatalogManager.getCardinality(tableName) - 1;
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
            return Pair.of(0l, LAST_TABLE_ROW);
        }

        int startPos = index.nextHighestRowInBucket(dataLocation,
                lastCompletedRow);
        if (startPos < 0) {
            return Pair.of(0l, LAST_TABLE_ROW);
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

    private Pair<Long, Integer> tableScanBranchingParallel(int budget,
                                                           FilterState state) {
        final int perThreadBudget = budget / state.batches;
        final int lastRow = lastCompletedRow + budget;
        long startTime = System.nanoTime();

        List<Future<MutableIntList>> futures = new ArrayList<>();

        for (int j = 0; j < state.batches; j++) {
            final int start = lastCompletedRow + perThreadBudget * j;
            final int end = j == state.batches - 1 ? lastRow :
                    start + perThreadBudget;

            if (state.cachedEval != null) {
                futures.add(ParallelService.HIGH_POOL.submit(() -> {
                    MutableIntList tempResult = IntLists.mutable.empty();
                    int currentCompletedRow = start;
                    ROW_LOOP:
                    while (currentCompletedRow < end &&
                            currentCompletedRow < LAST_TABLE_ROW) {
                        currentCompletedRow++;

                        if (state.cachedEval.evaluate(currentCompletedRow)
                                <= 0) {
                            continue ROW_LOOP;
                        }

                        for (int i = state.cachedTil + 1;
                             i < state.order.length; i++) {
                            UnaryBoolEval expr = compiled.get(state.order[i]);
                            if (expr.evaluate(currentCompletedRow) <= 0) {
                                continue ROW_LOOP;
                            }
                        }

                        tempResult.add(currentCompletedRow);
                    }
                    return tempResult;
                }));
            } else {
                futures.add(ParallelService.HIGH_POOL.submit(() -> {
                    MutableIntList tempResult = IntLists.mutable.empty();
                    int currentCompletedRow = start;
                    ROW_LOOP:
                    while (currentCompletedRow < end &&
                            currentCompletedRow < LAST_TABLE_ROW) {
                        currentCompletedRow++;

                        for (int predIndex : state.order) {
                            UnaryBoolEval expr = compiled.get(predIndex);
                            if (expr.evaluate(currentCompletedRow) <= 0) {
                                continue ROW_LOOP;
                            }
                        }

                        tempResult.add(currentCompletedRow);
                    }
                    return tempResult;
                }));
            }
        }

        for (Future<MutableIntList> f : futures) {
            try {
                result.addAll(f.get());
            } catch (Exception e) {}
        }

        long endTime = System.nanoTime();
        long duration = endTime - startTime;
        return Pair.of(duration, lastCompletedRow + budget);
    }

    private Pair<Long, Integer> tableScanBranching(int remainingRows,
                                                   FilterState state) {
        int currentCompletedRow = lastCompletedRow;
        long startTime = System.nanoTime();

        if (state.cachedEval != null) {
            ROW_LOOP:
            while (remainingRows > 0 && currentCompletedRow < LAST_TABLE_ROW) {
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
            while (remainingRows > 0 && currentCompletedRow < LAST_TABLE_ROW) {
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
        int end = Math.min(lastCompletedRow + nrRows, LAST_TABLE_ROW);
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

    public double executeWithBudget(int budget, FilterState state) {
        Pair<Long, Integer> result;

        if (state.useIndexScan) { // Use index to filter rows.
            result = indexScanWithOnePredicate(budget, state);
        } else if (state.avoidBranching) {
            result = tableScanBitset(budget, state);
        } else if (state.batches > 0) {
            result = tableScanBranchingParallel(budget, state);
        } else {
            result = tableScanBranching(budget, state);
        }


        double reward = Math.exp(-result.getLeft() / (double) budget);
        lastCompletedRow = result.getRight();
        return reward;
    }

    public boolean isFinished() {
        return lastCompletedRow == LAST_TABLE_ROW;
    }

    public MutableIntList getResult() {
        return result;
    }
}
